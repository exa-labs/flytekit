import base64
import json

import pytest
import ray
import yaml

from flytekit.core.resources import pod_spec_from_resources
from flytekitplugins.ray import HeadNodeConfig
from flytekitplugins.ray.models import (
    HeadGroupSpec,
    RayCluster,
    RayJob,
    WorkerGroupSpec,
)
from flytekitplugins.ray.task import RayJobConfig, WorkerNodeConfig
from google.protobuf.json_format import MessageToDict

from flytekit import PythonFunctionTask, task, PodTemplate, Resources
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.models.task import K8sPod


pod_template=PodTemplate(
        primary_container_name="primary",
        labels={"lKeyA": "lValA"},
        annotations={"aKeyA": "aValA"},
    )

config = RayJobConfig(
    worker_node_config=[
        WorkerNodeConfig(
            group_name="test_group",
            replicas=3,
            min_replicas=0,
            max_replicas=10,
            pod_template=pod_template,
        )
    ],
    head_node_config=HeadNodeConfig(requests=Resources(cpu="1", mem="1Gi"), limits=Resources(cpu="2", mem="2Gi")),
    runtime_env={"pip": ["numpy"]},
    enable_autoscaling=True,
    shutdown_after_job_finishes=True,
    ttl_seconds_after_finished=20,
)


def test_ray_task():
    @task(task_config=config)
    def t1(a: int) -> str:
        assert ray.is_initialized()
        inc = a + 2
        return str(inc)

    assert t1.task_config is not None
    assert t1.task_config == config
    assert t1.task_type == "ray"
    assert isinstance(t1, PythonFunctionTask)

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        env={},
    )
    head_pod_template = PodTemplate(
                pod_spec=pod_spec_from_resources(
                    primary_container_name="ray-head",
                    requests=Resources(cpu="1", mem="1Gi"),
                    limits=Resources(cpu="2", mem="2Gi"),
                )
            )

    ray_job_pb = RayJob(
        ray_cluster=RayCluster(
            worker_group_spec=[
                WorkerGroupSpec(
                    group_name="test_group",
                    replicas=3,
                    min_replicas=0,
                    max_replicas=10,
                    k8s_pod=K8sPod.from_pod_template(pod_template),
                )
            ],
            head_group_spec=HeadGroupSpec(k8s_pod=K8sPod.from_pod_template(head_pod_template)),
            enable_autoscaling=True,
        ),
        runtime_env=base64.b64encode(json.dumps({"pip": ["numpy"]}).encode()).decode(),
        runtime_env_yaml=yaml.dump({"pip": ["numpy"]}),
        shutdown_after_job_finishes=True,
        ttl_seconds_after_finished=20,
    ).to_flyte_idl()

    assert t1.get_custom(settings) == MessageToDict(ray_job_pb)

    assert t1.get_command(settings) == [
        "pyflyte-execute",
        "--inputs",
        "{{.input}}",
        "--output-prefix",
        "{{.outputPrefix}}",
        "--raw-output-data-prefix",
        "{{.rawOutputDataPrefix}}",
        "--checkpoint-path",
        "{{.checkpointOutputPrefix}}",
        "--prev-checkpoint",
        "{{.prevCheckpointPrefix}}",
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "tests.test_ray",
        "task-name",
        "t1",
    ]

    assert t1(a=3) == "5"
    assert ray.is_initialized()


def test_ray_job_config_cluster_selector():
    """Test RayJobConfig with cluster_selector for existing cluster mode."""
    cluster_selector_config = RayJobConfig(
        cluster_selector={"ray.io/cluster": "my-dev-cluster"},
        runtime_env={"pip": ["numpy"]},
    )

    assert cluster_selector_config.cluster_selector == {"ray.io/cluster": "my-dev-cluster"}
    assert cluster_selector_config.worker_node_config is None
    assert cluster_selector_config.head_node_config is None


def test_ray_job_config_mutual_exclusivity():
    """Test that cluster_selector and worker_node_config are mutually exclusive."""
    with pytest.raises(ValueError, match="cluster_selector and worker_node_config are mutually exclusive"):
        RayJobConfig(
            cluster_selector={"ray.io/cluster": "my-dev-cluster"},
            worker_node_config=[
                WorkerNodeConfig(group_name="test_group", replicas=3)
            ],
        )


def test_ray_job_config_requires_one_mode():
    """Test that either cluster_selector or worker_node_config must be provided."""
    with pytest.raises(ValueError, match="Either cluster_selector or worker_node_config must be provided"):
        RayJobConfig()


def test_ray_job_model_cluster_selector():
    """Test RayJob model with cluster_selector."""
    ray_job = RayJob(
        cluster_selector={"ray.io/cluster": "my-dev-cluster"},
        runtime_env_yaml="pip:\n- numpy\n",
        shutdown_after_job_finishes=False,
    )

    assert ray_job.cluster_selector == {"ray.io/cluster": "my-dev-cluster"}
    assert ray_job.ray_cluster is None

    pb = ray_job.to_flyte_idl()
    assert pb.cluster_selector == {"ray.io/cluster": "my-dev-cluster"}


def test_ray_job_model_mutual_exclusivity():
    """Test that RayJob model enforces mutual exclusivity."""
    with pytest.raises(ValueError, match="ray_cluster and cluster_selector are mutually exclusive"):
        RayJob(
            ray_cluster=RayCluster(worker_group_spec=[]),
            cluster_selector={"ray.io/cluster": "my-dev-cluster"},
        )


def test_ray_job_model_requires_one_mode():
    """Test that RayJob model requires either ray_cluster or cluster_selector."""
    with pytest.raises(ValueError, match="Either ray_cluster or cluster_selector must be provided"):
        RayJob()


def test_ray_task_with_cluster_selector():
    """Test Ray task with cluster_selector configuration."""
    cluster_selector_config = RayJobConfig(
        cluster_selector={"ray.io/cluster": "my-dev-cluster"},
        runtime_env={"pip": ["numpy"]},
    )

    @task(task_config=cluster_selector_config)
    def t2(a: int) -> str:
        return str(a + 1)

    assert t2.task_config is not None
    assert t2.task_config.cluster_selector == {"ray.io/cluster": "my-dev-cluster"}
    assert t2.task_type == "ray"

    default_img = Image(name="default", fqn="test", tag="tag")
    settings = SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        env={},
    )

    custom = t2.get_custom(settings)
    assert "clusterSelector" in custom
    assert custom["clusterSelector"] == {"ray.io/cluster": "my-dev-cluster"}
    assert "rayCluster" not in custom or custom.get("rayCluster") is None
