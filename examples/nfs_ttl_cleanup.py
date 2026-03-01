"""
NFS TTL Cleanup Workflow

This workflow deletes directories from NFS storage where all files haven't been 
accessed in the specified TTL period. It supports two clusters:
- exa-cluster: Uses NFS PVC (nfs-pvc)
- cirrascale: Uses NFS direct mount (172.18.72.200:/export/metaphor)

The workflow runs daily and defaults to a 4-week TTL period.
"""

import os
import time
from datetime import timedelta
from typing import List

from flytekit import LaunchPlan, PodTemplate, task, workflow
from flytekit.core.schedule import CronSchedule
from kubernetes.client import (
    V1Container,
    V1NFSVolumeSource,
    V1PersistentVolumeClaimVolumeSource,
    V1PodSpec,
    V1ResourceRequirements,
    V1SecurityContext,
    V1Volume,
    V1VolumeMount,
)


def should_delete_directory(dir_path: str, ttl_seconds: float) -> bool:
    """
    Check if all files in a directory haven't been accessed within TTL period.
    
    Args:
        dir_path: Path to the directory to check
        ttl_seconds: TTL in seconds
        
    Returns:
        True if all files haven't been accessed within TTL, False otherwise
    """
    current_time = time.time()
    cutoff_time = current_time - ttl_seconds
    
    try:
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    stat_info = os.stat(file_path)
                    if stat_info.st_atime > cutoff_time:
                        return False
                except (OSError, PermissionError) as e:
                    print(f"Warning: Could not stat {file_path}: {e}")
                    return False
        
        return True
    except (OSError, PermissionError) as e:
        print(f"Warning: Could not walk directory {dir_path}: {e}")
        return False


def delete_directory(dir_path: str) -> None:
    """
    Delete a directory and all its contents.
    
    Args:
        dir_path: Path to the directory to delete
    """
    try:
        import shutil
        shutil.rmtree(dir_path)
        print(f"Deleted directory: {dir_path}")
    except Exception as e:
        print(f"Error deleting directory {dir_path}: {e}")


exa_cluster_pod_template = PodTemplate(
    pod_spec=V1PodSpec(
        node_selector={"cluster": "exa-cluster"},
        volumes=[
            V1Volume(
                name="nfs-pvc",
                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                    claim_name="nfs-pvc",
                ),
            ),
        ],
        containers=[
            V1Container(
                name="primary",
                volume_mounts=[
                    V1VolumeMount(
                        name="nfs-pvc",
                        mount_path="/mnt/nfs",
                    ),
                ],
                resources=V1ResourceRequirements(
                    requests={
                        "cpu": "4",
                        "memory": "8Gi"
                    },
                    limits={
                        "cpu": "4",
                        "memory": "8Gi"
                    }
                ),
            )
        ],
    )
)


cirrascale_pod_template = PodTemplate(
    pod_spec=V1PodSpec(
        node_selector={"cluster": "cirrascale"},
        volumes=[
            V1Volume(
                name="nfs-volume",
                nfs=V1NFSVolumeSource(
                    server="172.18.72.200",
                    path="/export/metaphor"
                ),
            ),
        ],
        containers=[
            V1Container(
                name="primary",
                volume_mounts=[
                    V1VolumeMount(
                        name="nfs-volume",
                        mount_path="/mnt/nfs",
                    ),
                ],
                resources=V1ResourceRequirements(
                    requests={
                        "cpu": "4",
                        "memory": "8Gi"
                    },
                    limits={
                        "cpu": "4",
                        "memory": "8Gi"
                    }
                ),
                security_context=V1SecurityContext(
                    run_as_user=0,
                    run_as_group=0,
                )
            )
        ],
    )
)


@task(pod_template=exa_cluster_pod_template)
def cleanup_nfs_ttl_exa(
    base_path: str = "/mnt/nfs",
    ttl_days: int = 28,
    dry_run: bool = False
) -> dict:
    """
    Clean up old directories on exa-cluster NFS.
    
    Args:
        base_path: Base path to scan for directories
        ttl_days: Time-to-live in days (default 28 days = 4 weeks)
        dry_run: If True, only report what would be deleted without deleting
        
    Returns:
        Dictionary with cleanup statistics
    """
    ttl_seconds = ttl_days * 24 * 60 * 60
    deleted_count = 0
    skipped_count = 0
    deleted_dirs: List[str] = []
    
    print(f"Starting NFS cleanup on exa-cluster")
    print(f"Base path: {base_path}")
    print(f"TTL: {ttl_days} days ({ttl_seconds} seconds)")
    print(f"Dry run: {dry_run}")
    
    if not os.path.exists(base_path):
        print(f"Base path {base_path} does not exist")
        return {
            "deleted_count": 0,
            "skipped_count": 0,
            "deleted_dirs": [],
            "error": f"Base path {base_path} does not exist"
        }
    
    try:
        entries = os.listdir(base_path)
        print(f"Found {len(entries)} entries in {base_path}")
        
        for entry in entries:
            full_path = os.path.join(base_path, entry)
            
            if not os.path.isdir(full_path):
                continue
            
            if should_delete_directory(full_path, ttl_seconds):
                print(f"Directory eligible for deletion: {full_path}")
                if not dry_run:
                    delete_directory(full_path)
                deleted_count += 1
                deleted_dirs.append(entry)
            else:
                skipped_count += 1
                
    except Exception as e:
        print(f"Error scanning base path: {e}")
        return {
            "deleted_count": deleted_count,
            "skipped_count": skipped_count,
            "deleted_dirs": deleted_dirs,
            "error": str(e)
        }
    
    print(f"Cleanup complete. Deleted: {deleted_count}, Skipped: {skipped_count}")
    return {
        "deleted_count": deleted_count,
        "skipped_count": skipped_count,
        "deleted_dirs": deleted_dirs
    }


@task(pod_template=cirrascale_pod_template)
def cleanup_nfs_ttl_cirrascale(
    base_path: str = "/mnt/nfs",
    ttl_days: int = 28,
    dry_run: bool = False
) -> dict:
    """
    Clean up old directories on cirrascale NFS.
    
    Args:
        base_path: Base path to scan for directories
        ttl_days: Time-to-live in days (default 28 days = 4 weeks)
        dry_run: If True, only report what would be deleted without deleting
        
    Returns:
        Dictionary with cleanup statistics
    """
    ttl_seconds = ttl_days * 24 * 60 * 60
    deleted_count = 0
    skipped_count = 0
    deleted_dirs: List[str] = []
    
    print(f"Starting NFS cleanup on cirrascale")
    print(f"Base path: {base_path}")
    print(f"TTL: {ttl_days} days ({ttl_seconds} seconds)")
    print(f"Dry run: {dry_run}")
    
    if not os.path.exists(base_path):
        print(f"Base path {base_path} does not exist")
        return {
            "deleted_count": 0,
            "skipped_count": 0,
            "deleted_dirs": [],
            "error": f"Base path {base_path} does not exist"
        }
    
    try:
        entries = os.listdir(base_path)
        print(f"Found {len(entries)} entries in {base_path}")
        
        for entry in entries:
            full_path = os.path.join(base_path, entry)
            
            if not os.path.isdir(full_path):
                continue
            
            if should_delete_directory(full_path, ttl_seconds):
                print(f"Directory eligible for deletion: {full_path}")
                if not dry_run:
                    delete_directory(full_path)
                deleted_count += 1
                deleted_dirs.append(entry)
            else:
                skipped_count += 1
                
    except Exception as e:
        print(f"Error scanning base path: {e}")
        return {
            "deleted_count": deleted_count,
            "skipped_count": skipped_count,
            "deleted_dirs": deleted_dirs,
            "error": str(e)
        }
    
    print(f"Cleanup complete. Deleted: {deleted_count}, Skipped: {skipped_count}")
    return {
        "deleted_count": deleted_count,
        "skipped_count": skipped_count,
        "deleted_dirs": deleted_dirs
    }


@workflow
def nfs_ttl_cleanup_workflow(
    base_path: str = "/mnt/nfs",
    ttl_days: int = 28,
    dry_run: bool = False
) -> dict:
    """
    Workflow to clean up old directories from NFS based on TTL.
    
    This is a generic workflow that will be specialized by launch plans
    for different clusters.
    
    Args:
        base_path: Base path to scan for directories
        ttl_days: Time-to-live in days (default 28 days = 4 weeks)
        dry_run: If True, only report what would be deleted without deleting
        
    Returns:
        Dictionary with cleanup statistics
    """
    return cleanup_nfs_ttl_exa(base_path=base_path, ttl_days=ttl_days, dry_run=dry_run)


@workflow
def nfs_ttl_cleanup_exa_workflow(
    base_path: str = "/mnt/nfs",
    ttl_days: int = 28,
    dry_run: bool = False
) -> dict:
    """
    Workflow specifically for exa-cluster NFS cleanup.
    
    Args:
        base_path: Base path to scan for directories
        ttl_days: Time-to-live in days (default 28 days = 4 weeks)
        dry_run: If True, only report what would be deleted without deleting
        
    Returns:
        Dictionary with cleanup statistics
    """
    return cleanup_nfs_ttl_exa(base_path=base_path, ttl_days=ttl_days, dry_run=dry_run)


@workflow
def nfs_ttl_cleanup_cirrascale_workflow(
    base_path: str = "/mnt/nfs",
    ttl_days: int = 28,
    dry_run: bool = False
) -> dict:
    """
    Workflow specifically for cirrascale NFS cleanup.
    
    Args:
        base_path: Base path to scan for directories
        ttl_days: Time-to-live in days (default 28 days = 4 weeks)
        dry_run: If True, only report what would be deleted without deleting
        
    Returns:
        Dictionary with cleanup statistics
    """
    return cleanup_nfs_ttl_cirrascale(base_path=base_path, ttl_days=ttl_days, dry_run=dry_run)


daily_schedule = CronSchedule(
    schedule="0 0 * * *",  # Run at midnight every day
)

exa_cluster_launch_plan = LaunchPlan.get_or_create(
    workflow=nfs_ttl_cleanup_exa_workflow,
    name="nfs_ttl_cleanup_exa_daily",
    schedule=daily_schedule,
    default_inputs={
        "base_path": "/mnt/nfs",
        "ttl_days": 28,  # 4 weeks default
        "dry_run": False
    }
)

cirrascale_launch_plan = LaunchPlan.get_or_create(
    workflow=nfs_ttl_cleanup_cirrascale_workflow,
    name="nfs_ttl_cleanup_cirrascale_daily",
    schedule=daily_schedule,
    default_inputs={
        "base_path": "/mnt/nfs",
        "ttl_days": 28,  # 4 weeks default
        "dry_run": False
    }
)


if __name__ == "__main__":
    print("Testing exa-cluster workflow...")
    result = nfs_ttl_cleanup_exa_workflow(base_path="/tmp/test", ttl_days=1, dry_run=True)
    print(f"Result: {result}")
