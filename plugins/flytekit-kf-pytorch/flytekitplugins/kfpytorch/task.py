"""
This Plugin adds the capability of running distributed pytorch training to Flyte using backend plugins, natively on
Kubernetes. It leverages `Pytorch Job <https://github.com/kubeflow/pytorch-operator>`_ Plugin from kubeflow.
"""

import logging
import os
import sys

# Force unbuffered output for immediate visibility
sys.stdout.flush()
os.environ['PYTHONUNBUFFERED'] = '1'

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Union

import cloudpickle
import flytekit
from flyteidl.plugins.kubeflow import common_pb2 as kubeflow_common
from flyteidl.plugins.kubeflow import pytorch_pb2 as pytorch_task
from google.protobuf.json_format import MessageToDict

from flytekit import FlyteContextManager, PythonFunctionTask, Resources, lazy_module, task
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import FlyteContext, OutputMetadata, OutputMetadataTracker
from flytekit.core.pod_template import PodTemplate
from flytekit.core.resources import convert_resources_to_resource_model
from flytekit.exceptions.base import FlyteRecoverableException
from flytekit.exceptions.user import FlyteUserRuntimeException
from flytekit.extend import IgnoreOutputs, TaskPlugins
from flytekit.models import task as _task_models

from .error_handling import is_recoverable_worker_error
from .pod_template import add_shared_mem_volume_to_pod_template

pd = lazy_module("pandas")

TORCH_IMPORT_ERROR_MESSAGE = "PyTorch is not installed. Please install `flytekitplugins-kfpytorch['elastic']`."

# Configure logger to show INFO level messages
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add console handler if not already present
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s - flytekitplugins.kfpytorch - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# Force immediate output
logger.info(f"PyTorch Elastic plugin logger initialized")

# Version marker for debugging
PYTORCH_ELASTIC_FIX_VERSION = "1.0-nnodes-override-fix"
print(f"[PYTORCH_ELASTIC] Plugin loaded with fix version: {PYTORCH_ELASTIC_FIX_VERSION}")


@dataclass
class RestartPolicy(Enum):
    """
    RestartPolicy describes how the replicas should be restarted
    """

    ALWAYS = kubeflow_common.RESTART_POLICY_ALWAYS
    FAILURE = kubeflow_common.RESTART_POLICY_ON_FAILURE
    NEVER = kubeflow_common.RESTART_POLICY_NEVER


@dataclass
class CleanPodPolicy(Enum):
    """
    CleanPodPolicy describes how to deal with pods when the job is finished.
    """

    NONE = kubeflow_common.CLEANPOD_POLICY_NONE
    ALL = kubeflow_common.CLEANPOD_POLICY_ALL
    RUNNING = kubeflow_common.CLEANPOD_POLICY_RUNNING


@dataclass
class RunPolicy:
    """
    RunPolicy describes some policy to apply to the execution of a kubeflow job.
    Args:
        clean_pod_policy (int): Defines the policy for cleaning up pods after the PyTorchJob completes. Default to None.
        ttl_seconds_after_finished (int): Defines the TTL for cleaning up finished PyTorchJobs.
        active_deadline_seconds (int): Specifies the duration (in seconds) since startTime during which the job.
        can remain active before it is terminated. Must be a positive integer. This setting applies only to pods.
        where restartPolicy is OnFailure or Always.
        backoff_limit (int): Number of retries before marking this job as failed.
    """

    clean_pod_policy: CleanPodPolicy = None
    ttl_seconds_after_finished: Optional[int] = None
    active_deadline_seconds: Optional[int] = None
    backoff_limit: Optional[int] = None


@dataclass
class Worker:
    image: Optional[str] = None
    requests: Optional[Resources] = None
    limits: Optional[Resources] = None
    replicas: Optional[int] = None
    restart_policy: Optional[RestartPolicy] = None


@dataclass
class Master:
    """
    Configuration for master replica group. Master should always have 1 replica, so we don't need a `replicas` field
    """

    image: Optional[str] = None
    requests: Optional[Resources] = None
    limits: Optional[Resources] = None
    restart_policy: Optional[RestartPolicy] = None


@dataclass
class PyTorch(object):
    """
    Configuration for an executable `PyTorch Job <https://github.com/kubeflow/pytorch-operator>`_. Use this
    to run distributed PyTorch training on Kubernetes. Please notice, in most cases, you should not worry
    about the configuration of the master and worker groups. The default configuration should work. The only
    field you should change is the number of workers. Both replicas will use the same image, and the same
    resources inherited from task function decoration.

    Args:
        master: Configuration for the master replica group.
        worker: Configuration for the worker replica group.
        run_policy: Configuration for the run policy.
        num_workers: [DEPRECATED] This argument is deprecated. Use `worker.replicas` instead.
        increase_shared_mem (bool): PyTorch uses shared memory to share data between processes. If torch multiprocessing is used
            (e.g. for multi-processed data loaders) the default shared memory segment size that the container runs with might not be enough
            and and one might have to increase the shared memory size. This option configures the task's pod template to mount
            an `emptyDir` volume with medium `Memory` to to `/dev/shm`.
            The shared memory size upper limit is the sum of the memory limits of the containers in the pod.
    """

    master: Master = field(default_factory=lambda: Master())
    worker: Worker = field(default_factory=lambda: Worker())
    run_policy: Optional[RunPolicy] = None
    # Support v0 config for backwards compatibility
    num_workers: Optional[int] = None
    increase_shared_mem: bool = True


@dataclass
class Elastic(object):
    """
    Configuration for `torch elastic training <https://pytorch.org/docs/stable/elastic/run.html>`_.

    Use this to run single- or multi-node distributed pytorch elastic training on k8s.

    Single-node elastic training is executed in a k8s pod when `nnodes` is set to 1.
    Multi-node training is executed otherwise using a `Pytorch Job <https://github.com/kubeflow/training-operator>`_.

    Like `torchrun`, this plugin sets the environment variable `OMP_NUM_THREADS` to 1 if it is not set.
    Please see https://pytorch.org/tutorials/recipes/recipes/tuning_guide.html for potential performance improvements.
    To change `OMP_NUM_THREADS`, specify it in the environment dict of the flytekit task decorator or via `pyflyte run --env`.

    .. note::

        The task type (and execution backend) is dynamically determined based on the `nnodes` value:
        
        - When `nnodes=1`: Task runs as a standalone pod (task_type="python-task")
        - When `nnodes>1`: Task runs as a PyTorchJob via Kubeflow operator (task_type="pytorch")
        
        This behavior is preserved even when using `with_overrides()` to change the task configuration.
        For example, a task created with `nnodes=2` can be overridden to `nnodes=1` and will correctly
        execute as a standalone pod instead of a PyTorchJob.

    Args:
        nnodes (Union[int, str]): Number of nodes, or the range of nodes in form <minimum_nodes>:<maximum_nodes>.
        nproc_per_node (str): Number of workers per node.
        start_method (str): Multiprocessing start method to use when creating workers.
        monitor_interval (int): Interval, in seconds, to monitor the state of workers.
        max_restarts (int): Maximum number of worker group restarts before failing.
        rdzv_configs (Dict[str, Any]): Additional rendezvous configs to pass to torch elastic, e.g. `{"timeout": 1200, "join_timeout": 900}`.
            See `torch.distributed.launcher.api.LaunchConfig` and `torch.distributed.elastic.rendezvous.dynamic_rendezvous.create_handler`.
            Default timeouts are set to 15 minutes to account for the fact that some workers might start faster than others: Some pods might
            be assigned to a running node which might have the image in its cache while other workers might require a node scale up and image pull.

        increase_shared_mem (bool): PyTorch uses shared memory to share data between processes. If torch multiprocessing is used
            (e.g. for multi-processed data loaders) the default shared memory segment size that the container runs with might not be enough
            and and one might have to increase the shared memory size. This option configures the task's pod template to mount
            an `emptyDir` volume with medium `Memory` to to `/dev/shm`.
            The shared memory size upper limit is the sum of the memory limits of the containers in the pod.
        run_policy: Configuration for the run policy.
    """

    nnodes: Union[int, str] = 1
    nproc_per_node: int = 1
    start_method: str = "spawn"
    monitor_interval: int = 5
    max_restarts: int = 0
    rdzv_configs: Dict[str, Any] = field(default_factory=lambda: {"timeout": 900, "join_timeout": 900})
    increase_shared_mem: bool = True
    run_policy: Optional[RunPolicy] = None

    def __repr__(self) -> str:
        """String representation for better logging."""
        return (f"Elastic(nnodes={self.nnodes}, nproc_per_node={self.nproc_per_node}, "
                f"start_method={self.start_method}, max_restarts={self.max_restarts})")


class PyTorchFunctionTask(PythonFunctionTask[PyTorch]):
    """
    Plugin that submits a PyTorchJob (see https://github.com/kubeflow/pytorch-operator)
        defined by the code within the _task_function to k8s cluster.
    """

    _PYTORCH_TASK_TYPE = "pytorch"

    def __init__(self, task_config: PyTorch, task_function: Callable, **kwargs):
        if task_config.num_workers and task_config.worker.replicas:
            raise ValueError(
                "Cannot specify both `num_workers` and `worker.replicas`. Please use `worker.replicas` as `num_workers` is depreacated."
            )
        if task_config.num_workers is None and task_config.worker.replicas is None:
            raise ValueError(
                "Must specify either `num_workers` or `worker.replicas`. Please use `worker.replicas` as `num_workers` is depreacated."
            )
        super().__init__(
            task_config,
            task_function,
            task_type=self._PYTORCH_TASK_TYPE,
            # task_type_version controls the version of the task template, do not change
            task_type_version=1,
            **kwargs,
        )
        if self.task_config.increase_shared_mem:
            if self.pod_template is None:
                self.pod_template = PodTemplate()
            add_shared_mem_volume_to_pod_template(self.pod_template)

        self._task_config = task_config

    def _convert_replica_spec(
        self, replica_config: Union[Master, Worker]
    ) -> pytorch_task.DistributedPyTorchTrainingReplicaSpec:
        resources = convert_resources_to_resource_model(requests=replica_config.requests, limits=replica_config.limits)
        replicas = 1
        # Master should always have 1 replica
        if not isinstance(replica_config, Master):
            replicas = replica_config.replicas
        return pytorch_task.DistributedPyTorchTrainingReplicaSpec(
            replicas=replicas,
            image=replica_config.image,
            resources=resources.to_flyte_idl() if resources else None,
            restart_policy=(replica_config.restart_policy.value if replica_config.restart_policy else None),
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        worker = self._convert_replica_spec(self._task_config.worker)
        # support v0 config for backwards compatibility
        if self._task_config.num_workers:
            worker.replicas = self._task_config.num_workers

        run_policy = (
            _convert_run_policy_to_flyte_idl(self._task_config.run_policy) if self._task_config.run_policy else None
        )
        pytorch_job = pytorch_task.DistributedPyTorchTrainingTask(
            worker_replicas=worker,
            master_replicas=self._convert_replica_spec(self._task_config.master),
            run_policy=run_policy,
        )
        return MessageToDict(pytorch_job)


# Register the Pytorch Plugin into the flytekit core plugin system
TaskPlugins.register_pythontask_plugin(PyTorch, PyTorchFunctionTask)


class ElasticWorkerResult(NamedTuple):
    """
    A named tuple representing the result of a torch elastic worker process.

    Attributes:
        return_value (Any): The value returned by the task function in the worker process.
        decks (list[flytekit.Deck]): A list of flytekit Deck objects created in the worker process.
    """

    return_value: Any
    decks: List[flytekit.Deck]
    om: Optional[OutputMetadata] = None


def spawn_helper(
    fn: bytes, raw_output_prefix: str, checkpoint_dest: str, checkpoint_src: str, kwargs
) -> ElasticWorkerResult:
    """Help to spawn worker processes.

    The purpose of this function is to 1) be pickleable so that it can be used with
    the multiprocessing start method `spawn` and 2) to call a cloudpickle-serialized
    function passed to it. This function itself doesn't have to be pickleable. Without
    such a helper task functions, which are not pickleable, couldn't be used with the
    start method `spawn`.

    Args:
        fn (bytes): Cloudpickle-serialized target function to be executed in the worker process.
        raw_output_prefix (str): Where to write offloaded data (files, directories, dataframes).
        checkpoint_dest (str): If a previous checkpoint exists, this path should is set to the folder
            that contains the checkpoint information.
        checkpoint_src (str): Location where the new checkpoint should be copied to.

    Returns:
        ElasticWorkerResult: A named tuple containing the return value of the task function and a list of
            flytekit Deck objects created in the worker process.
    """
    from flytekit.bin.entrypoint import setup_execution

    with setup_execution(
        raw_output_data_prefix=raw_output_prefix,
        checkpoint_path=checkpoint_dest,
        prev_checkpoint=checkpoint_src,
    ) as ctx:
        fn = cloudpickle.loads(fn)
        try:
            return_val = fn(**kwargs)
            omt = ctx.output_metadata_tracker
            om = None
            if omt:
                om = omt.get(return_val)
        except Exception as e:
            # See explanation in `create_recoverable_error_file` why we check
            # for recoverable errors here in the worker processes.
            if isinstance(e, FlyteRecoverableException):
                create_recoverable_error_file()
            raise
        return ElasticWorkerResult(return_value=return_val, decks=flytekit.current_context().decks, om=om)


def _convert_run_policy_to_flyte_idl(
    run_policy: RunPolicy,
) -> kubeflow_common.RunPolicy:
    return kubeflow_common.RunPolicy(
        clean_pod_policy=(run_policy.clean_pod_policy.value if run_policy.clean_pod_policy else None),
        ttl_seconds_after_finished=run_policy.ttl_seconds_after_finished,
        active_deadline_seconds=run_policy.active_deadline_seconds,
        backoff_limit=run_policy.backoff_limit,
    )


class PytorchElasticFunctionTask(PythonFunctionTask[Elastic]):
    """
    Plugin for distributed training with torch elastic/torchrun (see
    https://pytorch.org/docs/stable/elastic/run.html).
    
    This task type dynamically adjusts its execution behavior based on the `nnodes` configuration:
    
    - When `nnodes=1`: Executes as a regular Python task without elastic launch, avoiding 
      unnecessary overhead and rendezvous timeouts.
    - When `nnodes>1`: Uses torch elastic launch for distributed execution across multiple nodes.
    
    This behavior is preserved even when using `with_overrides()` to change the configuration,
    allowing seamless switching between single-node and multi-node execution modes.
    """

    _ELASTIC_TASK_TYPE = "pytorch"
    _ELASTIC_TASK_TYPE_STANDALONE = "python-task"

    def __init__(self, task_config: Elastic, task_function: Callable, **kwargs):
        # Store initial task type based on initial config
        # Handle both int and string nnodes values
        nnodes = task_config.nnodes
        print(f"[PYTORCH_ELASTIC] __init__: nnodes={nnodes}, type={type(nnodes)}")
        
        if isinstance(nnodes, int):
            initial_task_type = self._ELASTIC_TASK_TYPE_STANDALONE if nnodes == 1 else self._ELASTIC_TASK_TYPE
        else:
            # For string values like "1:4", check if it's "1" or "1:1"
            nnodes_str = str(nnodes)
            initial_task_type = self._ELASTIC_TASK_TYPE_STANDALONE if nnodes_str in ["1", "1:1"] else self._ELASTIC_TASK_TYPE
        
        print(f"[PYTORCH_ELASTIC] __init__: initial_task_type={initial_task_type}")

        super(PytorchElasticFunctionTask, self).__init__(
            task_config=task_config,
            task_type=initial_task_type,
            task_function=task_function,
            # task_type_version controls the version of the task template, do not change
            task_type_version=1,
            **kwargs,
        )
        """
        c10d is the backend recommended by torch elastic.
        https://pytorch.org/docs/stable/elastic/run.html#note-on-rendezvous-backend

        For c10d, no backend server has to be deployed.
        https://pytorch.org/docs/stable/elastic/run.html#deployment
        Instead, the workers will use the master's address as the rendezvous point.
        """
        self.rdzv_backend = "c10d"

        if self.task_config.increase_shared_mem:
            if self.pod_template is None:
                self.pod_template = PodTemplate()
            add_shared_mem_volume_to_pod_template(self.pod_template)

    @property
    def task_type(self) -> str:
        """
        Dynamically determine task type based on current nnodes configuration.
        This ensures that task type updates when task_config is overridden.
        """
        print(f"[PYTORCH_ELASTIC] task_type property accessed")
        if self._task_config:
            # Handle both int and string nnodes values
            nnodes = self._task_config.nnodes
            print(f"[PYTORCH_ELASTIC] task_type property: checking nnodes={nnodes}, type={type(nnodes)}")
            
            if isinstance(nnodes, int):
                if nnodes == 1:
                    print(f"[PYTORCH_ELASTIC] task_type property: returning STANDALONE (nnodes=1)")
                    return self._ELASTIC_TASK_TYPE_STANDALONE
            else:
                # For string values like "1:4", check if it's "1" or "1:1"
                nnodes_str = str(nnodes)
                if nnodes_str == "1" or nnodes_str == "1:1":
                    print(f"[PYTORCH_ELASTIC] task_type property: returning STANDALONE (nnodes_str={nnodes_str})")
                    return self._ELASTIC_TASK_TYPE_STANDALONE
        
        print(f"[PYTORCH_ELASTIC] task_type property: returning ELASTIC (multi-node)")
        return self._ELASTIC_TASK_TYPE

    def _execute(self, **kwargs) -> Any:
        """
        Execute the task function using torch distributed's `elastic_launch`.

        Returns:
            The result of (global) rank zero.

        Raises:
            FlyteRecoverableException: If the first exception raised in the local worker group is or
                inherits from `FlyteRecoverableException`.
            RuntimeError: The first exception raised in the local worker group is not and does not
                inherit from `FlyteRecoverableException`.
            IgnoreOutputs: Raised when the task is successful in any worker group with index > 0.
        """
        print(f"[PYTORCH_ELASTIC] _execute: ENTERED ELASTIC LAUNCH METHOD")
        print(f"[PYTORCH_ELASTIC] _execute: task_config.nnodes={self._task_config.nnodes}")
        
        try:
            from torch.distributed import run
            from torch.distributed.launcher.api import LaunchConfig, elastic_launch
        except ImportError:
            raise ImportError(TORCH_IMPORT_ERROR_MESSAGE)

        nnodes_str = os.environ.get("PET_NNODES", str(self._task_config.nnodes))
        print(f"[PYTORCH_ELASTIC] _execute: PET_NNODES env var={os.environ.get('PET_NNODES')}, using nnodes_str={nnodes_str}")
        min_nodes, max_nodes = run.parse_min_max_nnodes(nnodes_str)
        print(f"[PYTORCH_ELASTIC] _execute: parsed min_nodes={min_nodes}, max_nodes={max_nodes}")

        nproc_per_node = int(os.environ.get("PET_NPROC_PER_NODE", self._task_config.nproc_per_node))
        max_restarts = int(os.environ.get("PET_MAX_RESTARTS", self._task_config.max_restarts))
        monitor_interval = int(os.environ.get("PET_MONITOR_INTERVAL", self._task_config.monitor_interval))
        rdzv_endpoint = os.environ.get("PET_RDZV_ENDPOINT", "localhost:0")
        
        print(f"[PYTORCH_ELASTIC] _execute: nproc_per_node={nproc_per_node}, max_restarts={max_restarts}")
        print(f"[PYTORCH_ELASTIC] _execute: monitor_interval={monitor_interval}, rdzv_endpoint={rdzv_endpoint}")

        # If OMP_NUM_THREADS is not set, set it to 1 to avoid overloading the system.
        # Doing so to copy the default behavior of torchrun.
        # See https://github.com/pytorch/pytorch/blob/eea4ece256d74c6f25c1f4eab37b3f2f4aeefd4d/torch/distributed/run.py#L791
        if "OMP_NUM_THREADS" not in os.environ and nproc_per_node > 1:
            omp_num_threads = 1
            print(
                "\n*****************************************\n"
                "Setting OMP_NUM_THREADS environment variable for each process to be "
                "%s in default, to avoid your system being overloaded, "
                "please further tune the variable for optimal performance in "
                "your application as needed. \n"
                "*****************************************",
                omp_num_threads,
            )
            os.environ["OMP_NUM_THREADS"] = str(omp_num_threads)

        config = LaunchConfig(
            run_id=flytekit.current_context().execution_id.name,
            min_nodes=min_nodes,
            max_nodes=max_nodes,
            nproc_per_node=nproc_per_node,
            rdzv_backend=self.rdzv_backend,  # rdzv settings
            rdzv_configs=self._task_config.rdzv_configs,
            rdzv_endpoint=rdzv_endpoint,
            max_restarts=max_restarts,
            monitor_interval=monitor_interval,
            start_method=self._task_config.start_method,
        )
        
        print(f"[PYTORCH_ELASTIC] _execute: LaunchConfig created with:")
        print(f"  - min_nodes={min_nodes}, max_nodes={max_nodes}")
        print(f"  - nproc_per_node={nproc_per_node}")
        print(f"  - rdzv_backend={self.rdzv_backend}")
        print(f"  - rdzv_endpoint={rdzv_endpoint}")
        print(f"  - start_method={self._task_config.start_method}")

        if self._task_config.start_method == "spawn":
            """
            We use cloudpickle to serialize the non-pickleable task function.
            The torch elastic launcher then launches the spawn_helper function (which is pickleable)
            instead of the task function. This helper function, in the child-process, then deserializes
            the task function, again with cloudpickle, and executes it.
            """
            launcher_target_func = spawn_helper

            dumped_target_function = cloudpickle.dumps(self._task_function)

            ctx = flytekit.current_context()
            try:
                checkpoint_dest = ctx.checkpoint._checkpoint_dest
                checkpoint_src = ctx.checkpoint._checkpoint_src
            except NotImplementedError:
                # Not using checkpointing in parent process
                checkpoint_dest = None
                checkpoint_src = None

            launcher_args = (
                dumped_target_function,
                ctx.raw_output_prefix,
                checkpoint_dest,
                checkpoint_src,
                kwargs,
            )
        elif self._task_config.start_method == "fork":
            """
            The torch elastic launcher doesn't support passing kwargs to the target function,
            only args. Flyte only works with kwargs. Thus, we create a closure which already has
            the task kwargs bound. We tell the torch elastic launcher to start this function in
            the child processes.
            """

            def fn_partial():
                """Closure of the task function with kwargs already bound."""
                try:
                    return_val = self._task_function(**kwargs)
                    core_context = FlyteContextManager.current_context()
                    omt = core_context.output_metadata_tracker
                    om = None
                    if omt:
                        om = omt.get(return_val)
                except Exception as e:
                    # See explanation in `create_recoverable_error_file` why we check
                    # for recoverable errors here in the worker processes.
                    if isinstance(e, FlyteRecoverableException):
                        create_recoverable_error_file()
                    raise
                return ElasticWorkerResult(
                    return_value=return_val,
                    decks=flytekit.current_context().decks,
                    om=om,
                )

            launcher_target_func = fn_partial
            launcher_args = ()

        else:
            raise ValueError("Bad start method")

        from torch.distributed.elastic.multiprocessing.api import SignalException
        from torch.distributed.elastic.multiprocessing.errors import ChildFailedError

        try:
            out = elastic_launch(
                config=config,
                entrypoint=launcher_target_func,
            )(*launcher_args)
        except ChildFailedError as e:
            _, first_failure = e.get_first_failure()
            if is_recoverable_worker_error(first_failure):
                # keep the timestamp of the original exception, rather than
                # the automatically assigned timestamp based on exception creation time
                raise FlyteRecoverableException(e.format_msg(), timestamp=first_failure.timestamp)
            else:
                raise FlyteUserRuntimeException(e, timestamp=first_failure.timestamp)
        except SignalException as e:
            logger.exception(f"Elastic launch agent process terminating: {e}")
            raise IgnoreOutputs()

        # `out` is a dictionary of rank (not local rank) -> result
        # Rank 0 returns the result of the task function
        if 0 in out:
            # For rank 0, we transfer the decks created in the worker process to the parent process
            ctx = flytekit.current_context()
            for deck in out[0].decks:
                if not isinstance(deck, flytekit.deck.deck.TimeLineDeck):
                    ctx.decks.append(deck)
            if out[0].om:
                core_context = FlyteContextManager.current_context()
                core_context.output_metadata_tracker.add(out[0].return_value, out[0].om)

            return out[0].return_value
        else:
            raise IgnoreOutputs()

    def execute(self, **kwargs) -> Any:
        """
        This method will be invoked to execute the task.

        Handles the exception scope for the `_execute` method.
        """
        print(f"[PYTORCH_ELASTIC] ========== EXECUTE METHOD CALLED ==========")
        print(f"PytorchElasticFunctionTask.execute called")
        print(f"Current task_config: {self._task_config}")
        print(f"Current task_type: {self.task_type}")
        
        print(f"[PYTORCH_ELASTIC] execute: task_config={self._task_config}")
        print(f"[PYTORCH_ELASTIC] execute: task_type={self.task_type}")
        
        # Log relevant environment variables
        print(f"[PYTORCH_ELASTIC] Environment: PET_NNODES={os.environ.get('PET_NNODES', 'NOT SET')}")
        
        # Check if this is a single-node configuration
        nnodes = self._task_config.nnodes
        is_single_node = False
        if isinstance(nnodes, int):
            is_single_node = (nnodes == 1)
            print(f"[PYTORCH_ELASTIC] execute: nnodes is int={nnodes}, is_single_node={is_single_node}")
        else:
            # For string values like "1:4", check if it's "1" or "1:1"
            nnodes_str = str(nnodes)
            is_single_node = nnodes_str in ["1", "1:1"]
            print(f"[PYTORCH_ELASTIC] execute: nnodes is str={nnodes_str}, is_single_node={is_single_node}")
        
        # For single-node execution, bypass elastic launch and run directly
        if is_single_node:
            # Run as a regular Python task without elastic launch
            print(f"[PYTORCH_ELASTIC] *** SINGLE-NODE DETECTED - BYPASSING ELASTIC LAUNCH ***")
            try:
                # Get parent class info
                parent_class = super().__class__
                print(f"[PYTORCH_ELASTIC] execute: Parent class is {parent_class}")
                print(f"[PYTORCH_ELASTIC] execute: Calling {parent_class.__name__}.execute()")
                result = super().execute(**kwargs)
                print(f"[PYTORCH_ELASTIC] execute: Parent execute returned successfully")
                return result
            except Exception as e:
                print(f"[PYTORCH_ELASTIC] execute: ERROR in parent execute: {type(e).__name__}: {e}")
                raise
        
        # For multi-node execution, use elastic launch
        print(f"[PYTORCH_ELASTIC] *** MULTI-NODE DETECTED - USING ELASTIC LAUNCH ***")
        return self._execute(**kwargs)

    def get_custom(self, settings: SerializationSettings) -> Optional[Dict[str, Any]]:
        print(f"[PYTORCH_ELASTIC] get_custom: Called for serialization")
        print(f"[PYTORCH_ELASTIC] get_custom: Current task_type property returns: {self.task_type}")
        print(f"[PYTORCH_ELASTIC] get_custom: settings={settings}")
        
        # Check if this is a single-node configuration
        nnodes = self._task_config.nnodes
        is_single_node = False
        if isinstance(nnodes, int):
            is_single_node = (nnodes == 1)
        else:
            # For string values like "1:4", check if it's "1" or "1:1"
            nnodes_str = str(nnodes)
            is_single_node = nnodes_str in ["1", "1:1"]
        
        print(f"[PYTORCH_ELASTIC] get_custom: nnodes={nnodes}, is_single_node={is_single_node}")
            
        if is_single_node:
            """
            Torch elastic distributed training is executed in a normal k8s pod so that this
            works without the kubeflow train operator.
            """
            print(f"[PYTORCH_ELASTIC] get_custom: Single-node configuration, returning super().get_custom()")
            return super().get_custom(settings)
        else:
            print(f"[PYTORCH_ELASTIC] get_custom: Multi-node configuration, creating ElasticConfig")
            from flyteidl.plugins.kubeflow.pytorch_pb2 import ElasticConfig

            try:
                from torch.distributed import run
            except ImportError:
                raise ImportError(TORCH_IMPORT_ERROR_MESSAGE)

            min_nodes, max_nodes = run.parse_min_max_nnodes(str(self._task_config.nnodes))

            elastic_config = ElasticConfig(
                rdzv_backend=self.rdzv_backend,
                min_replicas=min_nodes,
                max_replicas=max_nodes,
                nproc_per_node=self._task_config.nproc_per_node,
                max_restarts=self._task_config.max_restarts,
            )
            run_policy = (
                _convert_run_policy_to_flyte_idl(self._task_config.run_policy) if self._task_config.run_policy else None
            )
            job = pytorch_task.DistributedPyTorchTrainingTask(
                worker_replicas=pytorch_task.DistributedPyTorchTrainingReplicaSpec(
                    replicas=max_nodes,
                ),
                elastic_config=elastic_config,
                run_policy=run_policy,
            )
            return MessageToDict(job)


# Register the PytorchElastic Plugin into the flytekit core plugin system
TaskPlugins.register_pythontask_plugin(Elastic, PytorchElasticFunctionTask)
