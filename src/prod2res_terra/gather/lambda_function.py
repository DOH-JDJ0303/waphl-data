"""
Terra Workspace to AWS Batch Submission Pipeline

Author: Jared Johnson, jared.johnson@doh.wa.gov

This script manages the transfer of Terra workflow outputs to AWS S3 by:
1. Discovering completed Terra workflow submissions
2. Caching submission metadata in S3
3. Submitting AWS Batch jobs for data transfer
"""

import os
import re
import io
import json
import urllib.parse
from datetime import datetime, timezone

import boto3
import pandas as pd
from firecloud import api as fapi
from dateutil import parser

from shared import io_ops, data_processing
from shared.aws_ops import log_print
from prod2res_terra.utils import set_google_credentials

# ============================================================================
# Configuration
# ============================================================================

GOOGLE_CLOUD_CREDENTIALS = os.environ.get('GOOGLE_CLOUD_CREDENTIALS')
DEST_BUCKET              = os.environ.get('DEST_BUCKET')
JOB_QUEUE                = os.environ.get('JOB_QUEUE')
JOB_DEFINITION           = os.environ.get('JOB_DEFINITION')
TERRA_WORKSPACES         = os.environ.get('TERRA_WORKSPACES', '').split(',')
TERRA_PROJECT            = os.environ.get('TERRA_PROJECT')

# Limit number of new batch jobs submitted per run
MAX_NEW_RUNS = 3

# Workflows matching these patterns will be excluded
EXCLUDE_WORKFLOW_PATTERNS = [r"basespace"]

TERRA_CACHE_PREFIX           = io_ops.TERRA_CACHE_PREFIX
TERRA_CACHE_TABLE_KEYS       = data_processing.TERRA_CACHE_TABLE_KEYS
TERRA_CACHE_TABLE_MERGE_KEYS = data_processing.TERRA_CACHE_TABLE_MERGE_KEYS
TERRA_CACHE_TABLE_PARTITIONS = data_processing.TERRA_CACHE_TABLE_PARTITIONS

# Define required columns for cache
REQUIRED_COLS = ['project', 'workspace', 'entity_type', 'workflow_name', 
                 'submission_id', 'submission_time', 'transfer_status']

# AWS clients
AWS_SESSION = boto3.session.Session()
S3 = AWS_SESSION.client("s3")
BATCH = AWS_SESSION.client("batch")

# ============================================================================
# Workflow Filtering
# ============================================================================

def should_exclude_workflow(workflow_name):
    """Check if workflow should be excluded based on name patterns."""
    for pattern in EXCLUDE_WORKFLOW_PATTERNS:
        if re.search(pattern, workflow_name, flags=re.IGNORECASE):
            return True
    return False

# ============================================================================
# Cache Management
# ============================================================================

def ensure_required_columns(df):
    """Ensure DataFrame has all required columns, adding empty ones if missing."""
    for col in REQUIRED_COLS:
        if col not in df.columns:
            df[col] = ""
    
    # Convert all to string for consistent handling
    for col in REQUIRED_COLS:
        df[col] = df[col].astype(str)
    
    return df[REQUIRED_COLS].copy()


def update_transfer_status(df):
    """
    Update transfer status by selecting highest priority status per submission.
    Priority order: complete > partial > failed > pending > queue > skipped
    """
    if df.empty:
        return df
    
    df = ensure_required_columns(df)
    group_cols = [c for c in REQUIRED_COLS if c != "transfer_status"]
    priority = ["complete", "partial", "failed", "pending", "queue", "skipped"]

    def pick_status(series):
        """Pick highest priority status from a series of statuses."""
        vals = {str(v).strip().lower() for v in series if pd.notna(v)}
        for p in priority:
            if p in vals:
                return p
        return ""

    result = (
        df.groupby(group_cols, dropna=False)["transfer_status"]
          .apply(pick_status)
          .reset_index()
    )
    
    return result[REQUIRED_COLS].astype(str)


def gather_workspace_cache(bucket, project, workspace):
    """
    Load and combine all cache CSVs for a workspace from S3.
    Returns a consolidated DataFrame with deduplicated entries.
    """
    uri = f"s3://{bucket}/{TERRA_CACHE_PREFIX}"

    filt = [
        ("project","=",project),
        ("workspace","=",workspace)]
    
    try:
        df = io_ops.read_delta_as_pandas(uri=uri, filters=filt)
    except Exception as e:
        log_print(f"Issue loading cache: {e}")
        return pd.DataFrame()
    
    return df


# ============================================================================
# Terra API Operations
# ============================================================================

def get_terra_submissions(project, workspace):
    """Fetch all submissions from a Terra workspace."""
    response = fapi.list_submissions(project, workspace)
    if response.status_code != 200:
        log_print(f"ERROR: Failed to list submissions for {workspace}: {response.text}")
        return []
    return response.json()


def get_terra_entity_types(project, workspace):
    """Fetch entity types available in a Terra workspace."""
    response = fapi.list_entity_types(project, workspace)
    if response.status_code != 200:
        log_print(f"ERROR: Failed to list entity types for {workspace}: {response.text}")
        return {}
    return response.json()


def parse_submission(submission):
    """Extract relevant metadata from a Terra submission object."""
    entity_type = re.sub('_set$', '', submission.get("submissionEntity", {}).get("entityType", ""))
    workflow_name = submission.get("methodConfigurationName", "")
    submission_id = submission["submissionId"]
    submission_date = int(parser.isoparse(submission["submissionDate"]).replace(tzinfo=timezone.utc).timestamp())
    status = submission["status"]
    workflow_statuses = submission.get("workflowStatuses", {})
    
    return {
        "entity_type": entity_type,
        "workflow_name": workflow_name,
        "submission_id": submission_id,
        "submission_date": submission_date,
        "status": status,
        "workflow_statuses": workflow_statuses,
    }


def is_eligible_submission(submission_data, entity_types):
    """Check if a submission is eligible for data transfer."""
    entity_has_data = submission_data["entity_type"] in entity_types
    is_done = submission_data["status"] == "Done"
    has_success = "Succeeded" in submission_data["workflow_statuses"]
    not_excluded = not should_exclude_workflow(submission_data["workflow_name"])
    
    return entity_has_data and is_done and has_success and not_excluded


def find_most_recent_per_entity(submissions):
    """
    Find the most recent submission for each entity type.
    Returns a dict mapping submission_id to metadata.
    """
    most_recent = {}
    
    for sub_id, (workflow, entity_type, timestamp) in submissions.items():
        
        if entity_type not in most_recent or timestamp > most_recent[entity_type]["timestamp"]:
            most_recent[entity_type] = {
                "workflow": workflow,
                "entity_type": entity_type,
                "timestamp": timestamp,
                "submission_id": sub_id,
            }
    
    # Return as dict mapping submission_id to [workflow, entity_type, date_str]
    return {
        entry["submission_id"]: [entry["workflow"], entry["entity_type"], entry["timestamp"]]
        for entry in most_recent.values()
    }


# ============================================================================
# Batch Job Submission
# ============================================================================

def submit_batch_job(project, workspace, submission_id, workflow, entity_type, date_str):
    """Submit an AWS Batch job for a Terra submission."""
    job_name = f"{project[:10]}_{workspace[:10]}_{submission_id[:8]}"
    
    try:
        BATCH.submit_job(
            jobName=job_name,
            jobQueue=JOB_QUEUE,
            jobDefinition=JOB_DEFINITION,
            containerOverrides={
                'environment': [
                    {'name': 'TERRA_PROJECT', 'value': str(project)},
                    {'name': 'TERRA_WORKSPACE', 'value': str(workspace)},
                    {'name': 'TERRA_SUBMISSIONID', 'value': str(submission_id)},
                    {'name': 'TERRA_WORKFLOW', 'value': str(workflow)},
                    {'name': 'TERRA_SUBMISSIONENTITY', 'value': str(entity_type)},
                    {'name': 'TERRA_SUBMISSIONTIME', 'value': str(date_str)},
                    {'name': 'DEST_BUCKET', 'value': str(DEST_BUCKET)},
                    {'name': 'GOOGLE_CLOUD_CREDENTIALS', 'value': str(GOOGLE_CLOUD_CREDENTIALS)},
                ]
            }
        )
        log_print(f"Submitted batch job for submission {submission_id}")
        return True
    except Exception as e:
        log_print(f"ERROR: Failed to submit job for {submission_id}: {e}")
        return False


# ============================================================================
# Main Workflow
# ============================================================================

def process_workspace(workspace):
    """
    Process a Terra workspace:
    1. Gather eligible submissions
    2. Update cache
    3. Submit batch jobs for new runs
    """
    log_print(f"Processing workspace: {workspace}")
    
    # Get Terra data
    entity_types = get_terra_entity_types(TERRA_PROJECT, workspace)
    if not entity_types:
        return
    
    submissions = get_terra_submissions(TERRA_PROJECT, workspace)
    if not submissions:
        log_print(f"No submissions found for workspace: {workspace}")
        return
    
    # Filter eligible submissions
    eligible_runs = {}
    excluded_count = 0
    
    for submission in submissions:
        try:
            parsed = parse_submission(submission)
        except Exception as e:
            log_print(f"Warning: Skipping malformed submission: {e}")
            continue
        
        if should_exclude_workflow(parsed["workflow_name"]):
            excluded_count += 1
            continue
        
        if is_eligible_submission(parsed, entity_types):
            eligible_runs[parsed["submission_id"]] = [
                parsed["workflow_name"],
                parsed["entity_type"],
                parsed["submission_date"]
            ]
    
    if excluded_count > 0:
        log_print(f"Excluded {excluded_count} submissions matching patterns: {EXCLUDE_WORKFLOW_PATTERNS}")
    
    if not eligible_runs:
        log_print(f"No eligible submissions found for workspace: {workspace}")
        return
    
    # Find most recent run per entity type
    latest_runs = find_most_recent_per_entity(eligible_runs)
    
    # Load existing cache
    cache = gather_workspace_cache(DEST_BUCKET, TERRA_PROJECT, workspace)
    if not cache.empty:
        cached_submissions = set(cache["submission_id"].tolist())
        cached_terminal = set(cache[cache["transfer_status"].isin(['complete', 'partial', 'skipped'])]["submission_id"].tolist())
        cached_incomplete = cached_submissions - cached_terminal
    else:
        cached_submissions = set()
        cached_terminal = set()
        cached_incomplete = set()
    
    # Determine which runs to process
    new_runs = {}
    new_cache_entries = []
    
    for sub_id, meta in eligible_runs.items():
        # If already complete/partial in cache, skip — no update needed
        if sub_id in cached_terminal:
            continue

        # Determine status
        if sub_id in latest_runs:
            if len(new_runs) < MAX_NEW_RUNS:
                status = 'pending'
                new_runs[sub_id] = meta
            else:
                status = 'queue'
        else:
            # Newer run exists for this entity type — mark incomplete cached entry as skipped
            if sub_id in cached_incomplete:
                status = 'skipped'
            else:
                continue  # Not latest and not in cache yet — nothing to do
        
        new_cache_entries.append({
            'project': TERRA_PROJECT,
            'workspace': workspace,
            'entity_type': meta[1],
            'workflow_name': meta[0],
            'submission_id': sub_id,
            'submission_time': meta[2],
            'transfer_status': status
        })
    
    # Update cache in S3
    if new_cache_entries:
        uri = f"s3://{DEST_BUCKET}/{TERRA_CACHE_PREFIX}"
        log_print(f"Adding new entries to cache: {uri}")
        df_new_cache = pd.DataFrame(new_cache_entries)
        io_ops.write_delta(
            df=df_new_cache, 
            uri=uri,
            key_cols=TERRA_CACHE_TABLE_MERGE_KEYS,
            partition_by=TERRA_CACHE_TABLE_PARTITIONS
        )

    # Submit batch jobs for new runs
    if not new_runs:
        log_print(f"No new submissions to process for workspace: {workspace}")
        return
    
    log_print(f"Submitting {len(new_runs)} batch job(s) for: {list(new_runs.keys())}")
    
    for sub_id, (workflow, entity_type, date_str) in new_runs.items():
        submit_batch_job(
            TERRA_PROJECT, 
            workspace, 
            sub_id, 
            workflow, 
            entity_type, 
            date_str
        )


def handler(event, context):
    """Lambda/AWS Batch entry point."""
    set_google_credentials()
    
    for workspace in TERRA_WORKSPACES:
        try:
            process_workspace(workspace)
        except Exception as e:
            log_print(f"ERROR processing workspace {workspace}: {e}")
            continue


def main():
    """Local testing entry point."""
    handler(None, None)


if __name__ == '__main__':
    main()