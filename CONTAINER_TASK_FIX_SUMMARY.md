# ContainerTask with .with_overrides() Fix

## Problem
When using `ContainerTask` with `.with_overrides()` and fast serialization enabled, Flytekit would throw an `AttributeError`:

```
AttributeError: 'ContainerTask' object has no attribute 'set_command_fn'
```

This happened because the code in `flytekit/tools/translator.py` was trying to call `set_command_fn` on any `PythonTask` when fast serialization was enabled, but `ContainerTask` doesn't have this method.

## Root Cause
The issue was in the `get_serializable_node` function at line 460 in `flytekit/tools/translator.py`:

```python
if entity._pod_template is not None and settings.should_fast_serialize():
    entity.flyte_entity.set_command_fn(_fast_serialize_command_fn(settings, entity.flyte_entity))
```

The code assumed that all `PythonTask` instances have a `set_command_fn` method, but:
- `PythonAutoContainerTask` has `set_command_fn` ✅
- `ContainerTask` does NOT have `set_command_fn` ❌

## Solution
Added a `hasattr` check to only call `set_command_fn` if the entity actually has this method:

```python
if entity._pod_template is not None and settings.should_fast_serialize():
    # Only set command function if the entity is a PythonAutoContainerTask
    # ContainerTask doesn't have set_command_fn method
    if hasattr(entity.flyte_entity, 'set_command_fn'):
        entity.flyte_entity.set_command_fn(_fast_serialize_command_fn(settings, entity.flyte_entity))
```

## File Changes
- **Modified**: `flytekit/tools/translator.py` - Added `hasattr` check before calling `set_command_fn`
- **Added**: Test in `tests/flytekit/unit/test_translator.py` - Verifies the fix works correctly

## Testing
The fix includes a comprehensive test that reproduces the exact issue reported:

```python
def test_container_task_with_overrides():
    """Test that ContainerTask with .with_overrides doesn't raise AttributeError on set_command_fn"""
    dev_task = ContainerTask(
        name="dev-task",
        image="alpine:latest",
        inputs=kwtypes(timeout=str),
        command=["/bin/sh", "-c", "sleep {{.inputs.timeout}}"],
    )
    
    pod_template = PodTemplate()
    
    # This should NOT raise AttributeError
    task_with_overrides = dev_task(
        timeout="30"
    ).with_overrides(
        pod_template=pod_template,
    )
    
    # Enable fast serialization to trigger the bug
    settings = serialization_settings.new_builder().with_fast_serialization_settings(
        FastSerializationSettings(enabled=True)
    ).build()
    
    # This should work without error
    entity_mapping = OrderedDict()
    serialized = get_serializable(entity_mapping, settings, task_with_overrides)
    assert serialized is not None
```

## Verification
The fix ensures:
1. ✅ `ContainerTask` with `.with_overrides()` works correctly
2. ✅ `PythonAutoContainerTask` continues to work as before
3. ✅ All existing tests continue to pass
4. ✅ The specific user case now works without errors

## Usage
After the fix, users can use `ContainerTask` with `.with_overrides()` without issues:

```python
dev_task = ContainerTask(
    name="dev-task",
    image=container_image,
    inputs=kwtypes(timeout=str),
    command=["/bin/sh", "-c", "sleep {{.inputs.timeout}}"],
)

# This now works correctly!
task_with_overrides = dev_task(
    timeout=str(timeout) if timeout is not None else "infinity"
).with_overrides(
    pod_template=pod_template,
)
```