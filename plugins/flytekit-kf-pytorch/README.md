# Flytekit Kubeflow PyTorch Plugin

This plugin uses the Kubeflow PyTorch Operator and provides an extremely simplified interface for executing distributed training using various PyTorch backends.

This plugin can execute torch elastic training, which is equivalent to run `torchrun`. Elastic training can be executed
in a single Pod (without requiring the PyTorch operator, see below) as well as in a distributed multi-node manner.

To install the plugin, run the following command:

```bash
pip install flytekitplugins-kfpytorch
```

To set up PyTorch operator in the Flyte deployment's backend, follow the [PyTorch Operator Setup](https://docs.flyte.org/en/latest/flytesnacks/examples/kfpytorch_plugin/index.html) guide.

An [example](https://docs.flyte.org/en/latest/flytesnacks/examples/mnist_classifier/index.html#mnist-classifier-training) showcasing PyTorch operator can be found in the documentation.

## Code Example
```python
from flytekitplugins.kfpytorch import PyTorch, Worker

@task(
    task_config = PyTorch(
        worker=Worker(replicas=5)
    )
    image="test_image",
    resources=Resources(cpu="1", mem="1Gi"),
)
def pytorch_job():
    ...
```

You can specify run policy and restart policy of a pytorch job. The default restart policy for both master and worker group is the never restart,
you can set it to other policy.
```python
from flytekitplugins.kfpytorch import PyTorch, Worker, RestartPolicy, RunPolicy

@task(
    task_config = PyTorch(
        worker=Worker(replicas=5, restart_policy=RestartPolicy.FAILURE),
        run_policy=RunPolicy(
            clean_pod_policy=CleanPodPolicy.ALL,
        )
    )
    image="test_image",
    resources=Resources(cpu="1", mem="1Gi"),
)
def pytorch_job():
    ...
```

## Upgrade Pytorch Plugin from V0 to V1
Pytorch plugin is now updated from v0 to v1 to enable more configuration options.
To migrate from v0 to v1, change the following:
1. Update flytepropeller to v1.6.0
2. Update flytekit version to v1.6.2
3. Update your code from:
    ```
    task_config=Pytorch(num_workers=10),
    ```
    to:
    ```
    task_config=PyTorch(worker=Worker(replicas=10)),
    ```

## Dynamic Execution Modes with Overrides

The PyTorch Elastic plugin now supports dynamic switching between single-node and multi-node execution modes using `with_overrides()`. This allows you to adapt your training based on runtime conditions without creating separate task definitions.

### Example: Dynamic Node Configuration

```python
from flytekit import task, workflow
from flytekitplugins.kfpytorch import Elastic

# Define a task with default multi-node configuration
@task(task_config=Elastic(nnodes=2, nproc_per_node=2))
def train_model(epochs: int, batch_size: int) -> float:
    # Your training code here
    return accuracy

@workflow
def adaptive_training(use_single_node: bool) -> float:
    if use_single_node:
        # Override to single-node execution
        # This will run as a regular pod without PyTorchJob
        result = train_model(epochs=10, batch_size=32).with_overrides(
            task_config=Elastic(nnodes=1, nproc_per_node=1)
        )
    else:
        # Use the original multi-node configuration
        result = train_model(epochs=10, batch_size=32)
    
    return result
```

### Key Benefits

1. **No Rendezvous Timeouts**: Single-node tasks bypass elastic launch entirely, avoiding unnecessary rendezvous attempts
2. **Resource Efficiency**: Single-node tasks run as regular pods, reducing overhead
3. **Flexibility**: Switch between execution modes based on runtime conditions
4. **Backward Compatible**: Existing tasks continue to work as before

### Execution Behavior

- `nnodes=1`: Task type becomes `"python-task"`, executes directly without elastic launch
- `nnodes>1`: Task type is `"pytorch"`, uses PyTorchJob with elastic launch
- String values like `"1"` or `"1:1"` are treated as single-node
- Elastic ranges like `"1:4"` are treated as multi-node

## Debug Output

The plugin now automatically prints debug messages to help diagnose issues. Look for messages with the `[PYTORCH_ELASTIC]` prefix:

```
[PYTORCH_ELASTIC] Plugin loaded with fix version: 1.0-nnodes-override-fix
[PYTORCH_ELASTIC] __init__: nnodes=1, type=<class 'int'>
[PYTORCH_ELASTIC] execute: task_config=Elastic(nnodes=1, nproc_per_node=1, ...)
[PYTORCH_ELASTIC] *** SINGLE-NODE DETECTED - BYPASSING ELASTIC LAUNCH ***
```

If you see these messages in your logs, the fix is working correctly.
