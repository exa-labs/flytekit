# Pyflyte JSON CLI Arguments Parsing to Pydantic Models

## Overview

This document details how pyflyte parses JSON CLI arguments back into Pydantic models, the expected format, and the complete parsing flow.

## Main Components

### 1. JSON Parameter Type Handler (`JsonParamType`)

**Location:** `flytekit/interaction/click_types.py:297-404`

The `JsonParamType` class is the core component responsible for converting JSON CLI arguments into Python objects, including Pydantic models.

#### Key Features:
- **Flexible Input**: Accepts JSON strings, file paths, or YAML files
- **Pydantic Support**: Built-in support for both Pydantic v1 and v2
- **Nested Object Support**: Handles nested dataclasses and complex structures
- **File Path Resolution**: Can load JSON/YAML from file paths

#### JSON Parsing Logic:
```python
def _parse(self, value: typing.Any, param: typing.Optional[click.Parameter]):
    if type(value) == dict or type(value) == list:
        return value
    try:
        return json.loads(value)
    except Exception:
        try:
            # Try to load as file if JSON parsing fails
            if os.path.exists(value):
                if value.endswith(".yaml") or value.endswith(".yml"):
                    with open(value, "r") as f:
                        return yaml.safe_load(f)
                with open(value, "r") as f:
                    return json.load(f)
            raise
        except json.JSONDecodeError as e:
            raise click.BadParameter(f"parameter {param} should be a valid json object, {value}, error: {e}")
```

### 2. Pydantic Model Conversion

**Location:** `flytekit/interaction/click_types.py:370-395`

The conversion to Pydantic models happens in the `convert` method:

```python
if is_pydantic_basemodel(self._python_type):
    """
    This function supports backward compatibility for the Pydantic v1 plugin.
    If the class is a Pydantic BaseModel, it attempts to parse JSON input using
    the appropriate version of Pydantic (v1 or v2).
    """
    try:
        if importlib.util.find_spec("pydantic.v1") is not None:
            from pydantic import BaseModel as BaseModelV2

            if issubclass(self._python_type, BaseModelV2):
                return self._python_type.model_validate_json(
                    json.dumps(parsed_value), strict=False, context={"deserialize": True}
                )
    except ImportError:
        pass

    # The behavior of the Pydantic v1 plugin.
    return self._python_type.parse_raw(json.dumps(parsed_value))
```

### 3. CLI Integration

**Location:** `flytekit/clis/sdk_in_container/run.py:439-509`

The `to_click_option` function creates Click options for workflow/task parameters:

```python
def to_click_option(
    ctx: click.Context,
    flyte_ctx: FlyteContext,
    input_name: str,
    literal_var: Variable,
    python_type: typing.Type,
    default_val: typing.Any,
    required: bool,
) -> click.Option:
    """
    Creates a Click option for each workflow/task parameter.
    For structured data (STRUCT type), it uses JsonParamType.
    """
```

### 4. Type Mapping

**Location:** `flytekit/interaction/click_types.py:451-487`

The `literal_type_to_click_type` function maps Flyte types to Click parameter types:

```python
def literal_type_to_click_type(lt: LiteralType, python_type: typing.Type) -> click.ParamType:
    """
    Converts a Flyte LiteralType given a python_type to a click.ParamType
    """
    if lt.simple:
        if lt.simple == SimpleType.STRUCT:
            ct = JsonParamType(python_type)
            ct.name = f"JSON object {python_type.__name__}"
            return ct
        # ... other type mappings
    
    if lt.collection_type or lt.map_value_type:
        ct = JsonParamType(python_type)
        if lt.collection_type:
            ct.name = "json list"
        else:
            ct.name = "json dictionary"
        return ct
```

## Expected JSON Format

### 1. Command Line Arguments

When passing JSON via command line, pyflyte expects:

```bash
# Simple JSON object
pyflyte run workflow.py my_task --param '{"field1": "value1", "field2": 123}'

# Complex nested structures
pyflyte run workflow.py my_task --param '{"nested": {"inner": [1, 2, 3]}, "flag": true}'

# Lists/Arrays
pyflyte run workflow.py my_task --param '[{"item": 1}, {"item": 2}]'
```

### 2. Input Files

**Location:** `flytekit/clis/sdk_in_container/run.py:944-1010`

The `YamlFileReadingCommand` class handles JSON/YAML input files:

```bash
# Using JSON file
pyflyte run workflow.py my_task --inputs-file inputs.json

# Using YAML file  
pyflyte run workflow.py my_task --inputs-file inputs.yaml

# Using stdin
echo '{"param": "value"}' | pyflyte run workflow.py my_task -
```

### 3. Expected Format Examples

Based on the test cases (`tests/flytekit/unit/cli/pyflyte/my_wf_input.json`):

```json
{
    "simple_int": 1,
    "simple_string": "Hello",
    "simple_float": 1.1,
    "complex_object": {
        "i": 1,
        "a": ["h", "e"]
    },
    "array": [1, 2, 3],
    "nested_dict": {
        "x": 1.0,
        "y": 2.0
    },
    "boolean": true,
    "date": "2020-05-01",
    "duration": "20H",
    "enum": "RED",
    "dict": {
        "hello": "world"
    },
    "list_of_objects": [{"i": 1, "a": ["h", "e"]}],
    "nested_structures": {
        "x": {"i": 1, "a": ["h", "e"]}
    },
    "deeply_nested": {
        "i": [{"i": 1, "a": ["h", "e"]}]
    }
}
```

## Pydantic Model Parsing Details

### 1. Pydantic v2 Support

For Pydantic v2 models, pyflyte uses:
- `model_validate_json()` with `strict=False` and `context={"deserialize": True}`
- This allows flexible deserialization and enables custom validation contexts

### 2. Pydantic v1 Support

For Pydantic v1 models, pyflyte uses:
- `parse_raw()` method for backward compatibility
- Automatic detection of Pydantic v1 vs v2 via `importlib.util.find_spec()`

### 3. JSON Serialization Context

The parsing process involves:
1. Parse JSON string into Python dict/list
2. Convert back to JSON string with `json.dumps(parsed_value)`
3. Use Pydantic's JSON parsing methods to create the model instance

## File Processing Flow

### 1. Input File Processing

The `YamlFileReadingCommand.parse_args()` method:

```python
def parse_args(self, ctx: Context, args: t.List[str]) -> t.List[str]:
    def load_inputs(f: str) -> t.Dict[str, str]:
        try:
            inputs = yaml.safe_load(f)
        except yaml.YAMLError as e:
            yaml_e = e
            try:
                inputs = json.loads(f)
            except json.JSONDecodeError as e:
                raise click.BadParameter(
                    message=f"Could not load the inputs file. Please make sure it is a valid JSON or YAML file."
                    f"\n json error: {e},"
                    f"\n yaml error: {yaml_e}",
                    param_hint="--inputs-file",
                )
        return inputs

    # Process inputs from file and convert to CLI arguments
    new_args = []
    for k, v in inputs.items():
        if isinstance(v, str):
            new_args.extend([f"--{k}", v])
        elif isinstance(v, bool):
            if v:
                new_args.append(f"--{k}")
        else:
            v = json.dumps(v)  # Convert complex objects to JSON strings
            new_args.extend([f"--{k}", v])
```

### 2. Stdin Processing

Input can also be read from stdin using `-` as the final argument:

```bash
echo '{"param": "value"}' | pyflyte run workflow.py my_task -
```

## Error Handling

### 1. JSON Parsing Errors

When JSON parsing fails:
```python
raise click.BadParameter(f"parameter {param} should be a valid json object, {value}, error: {e}")
```

### 2. Pydantic Validation Errors

Pydantic validation errors are caught and re-raised as Click parameter errors:
```python
except Exception as e:
    raise click.BadParameter(
        f"Failed to convert param: {param if param else 'NA'}, value: {value} to type: {self._python_type}."
        f" Reason {e}"
    ) from e
```

## Key Implementation Details

### 1. Type Detection

The `is_pydantic_basemodel()` function handles both Pydantic v1 and v2:
```python
def is_pydantic_basemodel(python_type: typing.Type) -> bool:
    try:
        import pydantic
    except ImportError:
        return False
    else:
        try:
            from pydantic import BaseModel as BaseModelV2
            from pydantic.v1 import BaseModel as BaseModelV1
            return issubclass(python_type, BaseModelV1) or issubclass(python_type, BaseModelV2)
        except ImportError:
            from pydantic import BaseModel
            return issubclass(python_type, BaseModel)
```

### 2. Nested Object Handling

The `has_nested_dataclass()` function recursively checks for nested dataclasses:
```python
def has_nested_dataclass(t: typing.Type) -> bool:
    """
    Recursively checks whether the given type or its nested types contain any dataclass.
    """
    if dataclasses.is_dataclass(t):
        return t not in FLYTE_TYPES  # Exclude special Flyte types
    
    return any(has_nested_dataclass(arg) for arg in get_args(t))
```

### 3. Fallback Mechanisms

The parser includes multiple fallback mechanisms:
1. Try JSON parsing first
2. If that fails, try loading as a file
3. Check for YAML files (.yaml/.yml extensions)
4. Fall back to raw JSON file loading

## Summary

Pyflyte's JSON parsing system is comprehensive and handles:
- **Multiple input methods**: CLI arguments, files, stdin
- **Multiple formats**: JSON, YAML
- **Complex structures**: Nested objects, lists, dataclasses
- **Pydantic compatibility**: Both v1 and v2 support
- **Error handling**: Detailed error messages for debugging
- **Type safety**: Proper conversion with validation

The system is designed to be flexible and user-friendly while maintaining type safety and proper error handling throughout the parsing pipeline.