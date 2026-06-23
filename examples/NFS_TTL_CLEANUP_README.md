# NFS TTL Cleanup Workflow

This example demonstrates a Flyte workflow that cleans up old directories from NFS storage based on a Time-To-Live (TTL) period.

## Overview

The workflow scans directories on NFS mounts and deletes directories where **all files** haven't been accessed within the specified TTL period (default: 4 weeks). This helps manage storage space by removing stale data.

## Features

- **Two Cluster Support**: 
  - `exa-cluster`: Uses NFS PVC (nfs-pvc)
  - `cirrascale`: Uses NFS direct mount (172.18.72.200:/export/metaphor)
  
- **Scheduled Execution**: Runs daily at midnight UTC

- **Configurable TTL**: Default is 28 days (4 weeks), but can be customized

- **Dry Run Mode**: Test the cleanup without actually deleting files

- **Safe Deletion Logic**: Only deletes directories where ALL files are older than TTL

## Architecture

### Tasks

- `cleanup_nfs_ttl_exa`: Task configured for exa-cluster with PVC mounting
- `cleanup_nfs_ttl_cirrascale`: Task configured for cirrascale with direct NFS mounting

Each task:
- Mounts the appropriate NFS storage
- Selects the correct cluster via node selector
- Configures resource requirements (4 CPU, 8Gi memory)

### Workflows

- `nfs_ttl_cleanup_exa_workflow`: Workflow for exa-cluster
- `nfs_ttl_cleanup_cirrascale_workflow`: Workflow for cirrascale

### Launch Plans

Two launch plans are configured to run daily:

- `nfs_ttl_cleanup_exa_daily`: Runs on exa-cluster daily at midnight UTC
- `nfs_ttl_cleanup_cirrascale_daily`: Runs on cirrascale daily at midnight UTC

Both launch plans have default inputs:
- `base_path`: "/mnt/nfs"
- `ttl_days`: 28 (4 weeks)
- `dry_run`: False

## Usage

### Register the Workflow

```bash
pyflyte register --project my-project --domain development nfs_ttl_cleanup.py
```

### Manual Execution

You can manually execute the workflow with custom parameters:

```bash
# Dry run to see what would be deleted
pyflyte run nfs_ttl_cleanup.py nfs_ttl_cleanup_exa_workflow \
    --base_path /mnt/nfs \
    --ttl_days 28 \
    --dry_run True

# Actual cleanup with custom TTL (7 days)
pyflyte run nfs_ttl_cleanup.py nfs_ttl_cleanup_exa_workflow \
    --base_path /mnt/nfs \
    --ttl_days 7 \
    --dry_run False
```

### Launch Plan Execution

The launch plans will run automatically on schedule, but you can also trigger them manually:

```bash
# Using flytectl
flytectl create execution --project my-project --domain production \
    -p nfs_ttl_cleanup_exa_daily
```

## How It Works

1. **Directory Scanning**: The task scans all top-level directories in the base path

2. **Access Time Check**: For each directory, it walks through all files and checks their last access time (`st_atime`)

3. **TTL Evaluation**: If ALL files in a directory have not been accessed within the TTL period, the directory is marked for deletion

4. **Safe Deletion**: Only directories that meet the criteria are deleted using `shutil.rmtree()`

5. **Statistics**: Returns counts of deleted and skipped directories

## Configuration

### Changing the Schedule

Modify the `daily_schedule` to change when the workflow runs:

```python
# Run every 12 hours
daily_schedule = CronSchedule(schedule="0 */12 * * *")

# Run weekly on Sunday at midnight
weekly_schedule = CronSchedule(schedule="0 0 * * 0")
```

### Adjusting Resources

Modify the `V1ResourceRequirements` in the pod templates:

```python
resources=V1ResourceRequirements(
    requests={
        "cpu": "8",
        "memory": "16Gi"
    },
    limits={
        "cpu": "8",
        "memory": "16Gi"
    }
)
```

### Customizing Base Path

You can change the default `base_path` in the launch plan:

```python
default_inputs={
    "base_path": "/mnt/nfs/custom/path",
    "ttl_days": 28,
    "dry_run": False
}
```

## Safety Considerations

1. **File Access Time**: The workflow uses `st_atime` (last access time). Note that some filesystems may not update this accurately depending on mount options (e.g., `noatime`).

2. **Permissions**: The cirrascale task runs as root (UID 0) to ensure proper NFS access. The exa-cluster task uses the default pod user.

3. **Dry Run First**: Always test with `dry_run=True` before running actual cleanup.

4. **Top-Level Only**: The workflow only considers top-level directories in the base path, not nested subdirectories.

5. **Error Handling**: If a file cannot be accessed, the directory is considered active and not deleted.

## Monitoring

Check the execution logs to see:
- Number of directories scanned
- Number of directories deleted
- Number of directories skipped
- List of deleted directory names
- Any errors encountered

## Return Value

The workflow returns a dictionary with:
```python
{
    "deleted_count": int,      # Number of directories deleted
    "skipped_count": int,      # Number of directories skipped
    "deleted_dirs": List[str], # Names of deleted directories
    "error": str               # Error message if any (optional)
}
```
