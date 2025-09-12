# Flyte Log Links Research

## Current State Analysis

### How Log Links Work in Flyte

Based on my analysis of the codebase, log links in Flyte are implemented through the following mechanism:

1. **TaskLog Class**: Located in `flytekit/models/core/execution.py` (line 153)
   - Takes parameters: `uri`, `name`, `message_format`, `ttl`
   - Creates clickable links that appear in the Flyte console

2. **Agent Integration**: Various agents add log links via the `Resource` class:
   - **BigQuery Agent** (`plugins/flytekit-bigquery/flytekitplugins/bigquery/agent.py`): Creates "BigQuery Console" links
   - **Databricks Agent** (`plugins/flytekit-spark/flytekitplugins/spark/agent.py`): Creates "Databricks Console" links
   - **Snowflake Agent** (`plugins/flytekit-snowflake/flytekitplugins/snowflake/agent.py`): Creates log links

### Pattern for Adding Log Links

```python
# Example from BigQuery agent
log_link = TaskLog(
    uri=f"https://console.cloud.google.com/bigquery?project={project}&j=bq:{location}:{job_id}&page=queryresults",
    name="BigQuery Console",
)
return Resource(phase=cur_phase, message=str(job.state), log_links=[log_link], outputs=res)
```

## Current Gap: Grafana Log Links

**Finding**: There is currently NO existing Grafana log link implementation in the codebase.

The user mentioned "somewhere we add grafana logs link to the grafana dash" but my analysis shows this functionality doesn't exist yet in the flytekit codebase.

## Recommendations for Implementation

### 1. Where to Add Grafana Log Links

Based on the existing pattern, Grafana log links should be added in one of these locations:

**Option A: Generic Log Link Generator**
- Create a new utility in `flytekit/remote/` that generates log links for executions
- This would be called when displaying execution details in the console

**Option B: Execution-level Log Links**
- Add log links directly to the `FlyteWorkflowExecution` or `FlyteTaskExecution` classes
- Modify the `generate_console_url` method in `flytekit/remote/remote.py` to include log links

**Option C: Agent-based Approach**
- Create a generic "logging agent" that adds log links for all executions
- This would be the most consistent with the existing pattern

### 2. Implementation Structure

The functionality should be added to enhance the `FlyteWorkflowExecution` class in `flytekit/remote/executions.py`:

```python
# In flytekit/remote/executions.py
@property
def log_links(self) -> List[TaskLog]:
    """Generate log links for this execution."""
    if self._remote is None:
        return []
    
    links = []
    
    # Existing individual pod logs (if this exists)
    pod_logs_url = self._remote.generate_grafana_pod_logs_url(self.id)
    if pod_logs_url:
        links.append(TaskLog(uri=pod_logs_url, name="Pod Logs"))
    
    # NEW: All workflow logs
    all_logs_url = self._remote.generate_grafana_all_workflow_logs_url(self.id)
    if all_logs_url:
        links.append(TaskLog(uri=all_logs_url, name="All workflow logs"))
    
    return links
```

### 3. URL Generation Pattern

Based on the user's request, the "All workflow logs" should use this query pattern:
```
{app=~"{exec_id}.*"} |= 
```

This should be implemented in `flytekit/remote/remote.py`:

```python
# In flytekit/remote/remote.py
def generate_grafana_all_workflow_logs_url(self, execution_id: WorkflowExecutionIdentifier) -> str:
    """Generate Grafana URL for all workflow logs."""
    # Get Grafana base URL from config
    grafana_base_url = self.config.platform.grafana_endpoint or "http://localhost:3000"
    
    # Build the query
    query = f'{{app=~"{execution_id.name}.*"}} |= '
    encoded_query = urllib.parse.quote(query)
    
    # Build full URL
    return f"{grafana_base_url}/explore?query={encoded_query}"
```

### 4. Configuration Requirements

Add Grafana configuration to the platform config:

```python
# In flytekit/configuration/platform.py
@dataclass
class PlatformConfig:
    # ... existing fields ...
    grafana_endpoint: Optional[str] = None
```

## Action Items

1. **Locate existing Grafana integration**: Search for any existing Grafana log functionality in the broader Flyte ecosystem (not just flytekit)
2. **Implement URL generation**: Add the `generate_grafana_all_workflow_logs_url` method
3. **Add configuration**: Extend platform config to include Grafana endpoint
4. **Integrate with execution display**: Add log links to the execution display logic
5. **Test integration**: Verify links work correctly with the Flyte console

## Files to Modify

1. `flytekit/remote/remote.py` - Add URL generation methods
2. `flytekit/remote/executions.py` - Add log links property
3. `flytekit/configuration/platform.py` - Add Grafana configuration
4. Wherever the console displays execution details - Add log links rendering

## Notes

- The user mentioned this is "to the flyte dashboard" - this suggests the functionality should be visible in the Flyte console UI
- The specific query pattern `{app=~"{exec_id}.*"} |= ` suggests this is for a Loki/Grafana Loki setup
- The implementation should follow the existing pattern used by other agents for consistency