from dataclasses import dataclass, fields
from typing import Dict, List, Optional, Union

from kubernetes.client import V1Container, V1PodSpec, V1ResourceRequirements, V1Toleration
from mashumaro.mixins.json import DataClassJSONMixin

from flytekit.models import task as task_models


@dataclass
class Resources(DataClassJSONMixin):
    """
    This class is used to specify both resource requests and resource limits.

    .. code-block:: python

        Resources(cpu="1", mem="2048")  # This is 1 CPU and 2 KB of memory
        Resources(cpu="100m", mem="2Gi")  # This is 1/10th of a CPU and 2 gigabytes of memory
        Resources(cpu=0.5, mem=1024) # This is 500m CPU and 1 KB of memory

        # For Kubernetes-based tasks, pods use ephemeral local storage for scratch space, caching, and for logs.
        # This allocates 1Gi of such local storage.
        Resources(ephemeral_storage="1Gi")

    .. note::

        Persistent storage is not currently supported on the Flyte backend.

    Please see the :std:ref:`User Guide <cookbook:customizing task resources>` for detailed examples.
    Also refer to the `K8s conventions. <https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-units-in-kubernetes>`__
    """

    cpu: Optional[Union[str, int, float]] = None
    mem: Optional[Union[str, int]] = None
    gpu: Optional[Union[str, int]] = None
    ephemeral_storage: Optional[Union[str, int]] = None

    def __post_init__(self):
        def _check_cpu(value):
            if value is None:
                return
            if not isinstance(value, (str, int, float)):
                raise AssertionError(f"{value} should be of type str or int or float")

        def _check_others(value):
            if value is None:
                return
            if not isinstance(value, (str, int)):
                raise AssertionError(f"{value} should be of type str or int")

        _check_cpu(self.cpu)
        _check_others(self.mem)
        _check_others(self.gpu)
        _check_others(self.ephemeral_storage)


@dataclass
class ResourceSpec(DataClassJSONMixin):
    requests: Resources
    limits: Resources


_ResourceName = task_models.Resources.ResourceName
_ResourceEntry = task_models.Resources.ResourceEntry


def _convert_resources_to_resource_entries(resources: Resources) -> List[_ResourceEntry]:  # type: ignore
    resource_entries = []
    if resources.cpu is not None:
        resource_entries.append(_ResourceEntry(name=_ResourceName.CPU, value=str(resources.cpu)))
    if resources.mem is not None:
        resource_entries.append(_ResourceEntry(name=_ResourceName.MEMORY, value=str(resources.mem)))
    if resources.gpu is not None:
        resource_entries.append(_ResourceEntry(name=_ResourceName.GPU, value=str(resources.gpu)))
    if resources.ephemeral_storage is not None:
        resource_entries.append(
            _ResourceEntry(
                name=_ResourceName.EPHEMERAL_STORAGE,
                value=str(resources.ephemeral_storage),
            )
        )
    return resource_entries


def convert_resources_to_resource_model(
    requests: Optional[Resources] = None,
    limits: Optional[Resources] = None,
) -> task_models.Resources:
    """
    Convert flytekit ``Resources`` objects to a Resources model

    :param requests: Resource requests. Optional, defaults to ``None``
    :param limits: Resource limits. Optional, defaults to ``None``
    :return: The given resources as requests and limits
    """
    request_entries = []
    limit_entries = []
    if requests is not None:
        request_entries = _convert_resources_to_resource_entries(requests)
    if limits is not None:
        limit_entries = _convert_resources_to_resource_entries(limits)
    return task_models.Resources(requests=request_entries, limits=limit_entries)


def pod_spec_from_resources(
    primary_container_name: str,
    requests: Optional[Resources] = None,
    limits: Optional[Resources] = None,
    k8s_gpu_resource_key: str = "nvidia.com/gpu",
    node_selector: Optional[Dict[str, str]] = None,
    tolerations: Optional[List[V1Toleration]] = None,
    host_network: bool = False,
    dns_policy: Optional[str] = "ClusterFirst",
) -> V1PodSpec:
    def _construct_k8s_pods_resources(resources: Optional[Resources], k8s_gpu_resource_key: str):
        if resources is None:
            return None

        resources_map = {
            "cpu": "cpu",
            "mem": "memory",
            "gpu": k8s_gpu_resource_key,
            "ephemeral_storage": "ephemeral-storage",
        }

        k8s_pod_resources = {}

        for resource in fields(resources):
            resource_value = getattr(resources, resource.name)
            if resource_value is not None:
                k8s_pod_resources[resources_map[resource.name]] = resource_value

        return k8s_pod_resources

    # Check if GPU is requested in either requests or limits before converting to k8s resources
    gpu_requested = (requests and requests.gpu is not None and int(requests.gpu) > 0) or (
        limits and limits.gpu is not None and int(limits.gpu) > 0
    )

    requests = _construct_k8s_pods_resources(resources=requests, k8s_gpu_resource_key=k8s_gpu_resource_key)
    limits = _construct_k8s_pods_resources(resources=limits, k8s_gpu_resource_key=k8s_gpu_resource_key)

    # Initialize tolerations to empty list if None
    if tolerations is None:
        tolerations = []

    # Add GPU toleration if GPU resources are requested
    if gpu_requested:
        # Use the same key as the GPU resource key for consistency
        # This allows pods requesting GPUs to be scheduled on nodes with GPU taints
        tolerations.append(V1Toleration(key="gpu", operator="Equal", value="true", effect="NoSchedule"))

    requests = requests or limits
    limits = limits or requests

    if node_selector:
        cluster_name = node_selector.get("cluster", None)
        # Support both old and new (Karpenter-compatible) node selector keys
        instance_type = node_selector.get("instance_type") or node_selector.get("node.kubernetes.io/instance-type")
        capacity_type = node_selector.get("capacity_type") or node_selector.get("karpenter.sh/capacity-type")

        if (cluster_name and cluster_name == "aws") or instance_type or capacity_type:
            # Note: We intentionally do NOT override dns_policy to "Default" here.
            # Using "Default" breaks DNS resolution for short hostnames in non-default namespaces,
            # which causes PyTorchJob multi-node elastic training to fail (C10d rendezvous can't resolve
            # worker hostnames). The default "ClusterFirst" policy ensures proper DNS resolution.
            tolerations.append(
                V1Toleration(key="cloud", operator="Equal", value="aws", effect="NoSchedule"),
            )

    pod_spec = V1PodSpec(
        host_network=host_network,
        dns_policy=dns_policy,
        containers=[
            V1Container(
                name=primary_container_name,
                resources=V1ResourceRequirements(
                    requests=requests,
                    limits=limits,
                ),
            )
        ],
        node_selector=node_selector,
        tolerations=tolerations,
    )

    return pod_spec
