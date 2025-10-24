#!/usr/bin/env python3
"""
Test script to reproduce the Elastic PyTorch task override issue.
"""

from flytekitplugins.kfpytorch.task import Elastic
from flytekit import task, workflow
from flytekit.configuration import SerializationSettings


# Create a task with default nnodes=2
@task(task_config=Elastic(nnodes=2, nproc_per_node=1))
def train_task():
    print("Training...")
    return "done"


@workflow
def training_workflow():
    # Override the task to use nnodes=1
    single_node_task = train_task().with_overrides(task_config=Elastic(nnodes=1, nproc_per_node=1))
    return single_node_task


def test_serialization():
    """Test what happens during serialization."""
    print("=== Original Task Config ===")
    print(f"Original task nnodes: {train_task._task_config.nnodes}")
    
    # Get the workflow
    wf = training_workflow
    
    # Get the node from the workflow
    node = list(wf.nodes)[0]  # First node
    print(f"Node task config nnodes: {node.run_entity._task_config.nnodes}")
    
    # Test serialization
    settings = SerializationSettings(image_config=None)
    
    print("\n=== Serialization Test ===")
    print("Original task get_custom:")
    original_custom = train_task.get_custom(settings)
    print(f"Original custom config: {original_custom}")
    
    print("\nOverridden task get_custom:")
    overridden_custom = node.run_entity.get_custom(settings)
    print(f"Overridden custom config: {overridden_custom}")
    
    # Check if the issue exists
    if original_custom == overridden_custom:
        print("\n❌ ISSUE CONFIRMED: Override not taking effect in serialization!")
    else:
        print("\n✅ Override working correctly in serialization")


if __name__ == "__main__":
    test_serialization()
