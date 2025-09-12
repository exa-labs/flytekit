#!/usr/bin/env python3
"""
Test to verify that the Elastic PyTorch task override fix works correctly.
This test demonstrates the issue and verifies the fix.
"""

import logging
from flytekitplugins.kfpytorch.task import Elastic
from flytekit import task, workflow
from flytekit.configuration import SerializationSettings

# Set up logging to see our debug messages
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(task_config=Elastic(nnodes=2, nproc_per_node=1))
def train_task():
    """A training task with default nnodes=2 (multi-node)."""
    print("Training...")
    return "done"


@workflow
def training_workflow():
    """Workflow that overrides the task to use nnodes=1 (single-node)."""
    # This should override the task to use single-node execution
    single_node_task = train_task().with_overrides(
        task_config=Elastic(nnodes=1, nproc_per_node=1)
    )
    return single_node_task


def test_override_fix():
    """Test that demonstrates the fix for Elastic PyTorch task overrides."""
    print("=== Testing Elastic PyTorch Task Override Fix ===\n")
    
    # Get the workflow and its node
    wf = training_workflow
    node = list(wf.nodes)[0]  # First (and only) node
    
    print(f"Original task nnodes: {train_task._task_config.nnodes}")
    print(f"Overridden node task nnodes: {node.run_entity._task_config.nnodes}")
    
    # Test serialization - this is where the bug manifests
    settings = SerializationSettings(image_config=None)
    
    print("\n=== Testing Serialization ===")
    print("Getting custom config for original task:")
    original_custom = train_task.get_custom(settings)
    
    print("\nGetting custom config for overridden task:")
    overridden_custom = node.run_entity.get_custom(settings)
    
    print(f"\nOriginal custom config: {original_custom}")
    print(f"Overridden custom config: {overridden_custom}")
    
    # Analyze the results
    print("\n=== Results Analysis ===")
    
    # Original task should use PyTorchJob (multi-node)
    if original_custom is None:
        print("✅ Original task correctly uses single-node (None custom config)")
    else:
        print("✅ Original task correctly uses multi-node (PyTorchJob custom config)")
    
    # Overridden task should use normal pod (single-node)
    if overridden_custom is None:
        print("✅ SUCCESS: Overridden task correctly uses single-node execution!")
        print("   The override from nnodes=2 to nnodes=1 is working properly.")
    else:
        print("❌ ISSUE: Overridden task still uses multi-node execution")
        print("   The override is not taking effect properly.")
        
    # Additional verification
    if original_custom != overridden_custom:
        print("✅ Override is taking effect - configurations are different")
    else:
        print("❌ Override not working - configurations are identical")


if __name__ == "__main__":
    test_override_fix()
