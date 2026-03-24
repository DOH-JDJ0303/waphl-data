#!/usr/bin/env python3
"""
Terra to S3 Data Transfer Script

Author: Jared Johnson, jared.johnson@doh.wa.gov

This script transfers workflow output files from Terra/GCS to AWS S3:
1. Fetches Terra data table for the submission
2. Identifies all output files (GCS URIs)
3. Transfers files to S3 with organized directory structure
4. Records transfer status back to cache
"""

import os
import re
import csv
import json
import sys
import tempfile
import traceback
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid

import boto3
import pandas as pd
from firecloud import api as fapi
from google.cloud import storage as gcs_storage
from botocore.exceptions import ClientError

from shared import io_ops, data_processing

# ============================================================================
# Configuration
# ============================================================================
SESSION_ID = str(uuid.uuid4().hex)

DEST_BUCKET = os.environ['DEST_BUCKET']
GCRED_URI = os.environ['GOOGLE_CLOUD_CREDENTIALS']

TERRA_PROJECT = os.environ['TERRA_PROJECT']
TERRA_WORKSPACE = os.environ['TERRA_WORKSPACE']
TERRA_SUBMISSIONID = os.environ['TERRA_SUBMISSIONID']
TERRA_WORKFLOW = os.environ['TERRA_WORKFLOW']
TERRA_SUBMISSIONENTITY = os.environ['TERRA_SUBMISSIONENTITY']  # entity_type
TERRA_SUBMISSIONTIME = os.environ['TERRA_SUBMISSIONTIME']

# AWS clients
SESSION = boto3.session.Session()
S3 = SESSION.client("s3")

# Local temp storage
TEMP_DIR = "/tmp/terra_rows"
TERRA_TABLE_BASENAME = "terra_table.csv"
os.makedirs(TEMP_DIR, exist_ok=True)

# Transfer concurrency
MAX_WORKERS = 3

RAW_PREFIX = io_ops.RAW_PREFIX

FILES_PREFIX             = io_ops.FILES_PREFIX
FILES_TABLE_KEYS         = data_processing.FILES_TABLE_KEYS
FILES_TABLE_MERGE_KEYS   = data_processing.FILES_TABLE_MERGE_KEYS
FILES_TABLE_PARTITIONS   = data_processing.FILES_TABLE_PARTITIONS

TERRA_CACHE_PREFIX           = io_ops.TERRA_CACHE_PREFIX
TERRA_CACHE_TABLE_KEYS       = data_processing.TERRA_CACHE_TABLE_KEYS
TERRA_CACHE_TABLE_MERGE_KEYS = data_processing.TERRA_CACHE_TABLE_MERGE_KEYS
TERRA_CACHE_TABLE_PARTITIONS = data_processing.TERRA_CACHE_TABLE_PARTITIONS


# ============================================================================
# Logging
# ============================================================================

def log_print(msg):
    """Print with flush for containerized environments."""
    print(str(msg), flush=True)


# ============================================================================
# URI Utilities
# ============================================================================

def parse_uri(uri):
    """Parse GCS or S3 URI into bucket and key/path components."""
    parsed = urllib.parse.urlparse(uri)
    bucket = parsed.netloc
    path = re.sub(r'/{2,}', '/', parsed.path.lstrip('/'))
    return bucket, path


def is_gs_uri(value):
    """Check if a value is a GCS URI string."""
    return isinstance(value, str) and value.startswith("gs://")


def all_gs_uris(seq):
    """Check if a sequence contains only GCS URI strings."""
    try:
        return isinstance(seq, (list, tuple)) and all(is_gs_uri(s) for s in seq)
    except Exception:
        return False


# ============================================================================
# Google Cloud Setup
# ============================================================================

def set_google_credentials():
    """Download and configure Google Cloud credentials from S3."""
    try:
        bucket, key = parse_uri(GCRED_URI)
        gcred_local = 'application_default_credentials.json'
        S3.download_file(bucket, key, gcred_local)
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gcred_local
        log_print("Google Cloud credentials configured successfully.")
    except Exception as e:
        log_print(f"ERROR: Failed to set Google credentials: {e}")
        raise


# ============================================================================
# S3 Utilities
# ============================================================================

def s3_file_exists(bucket, key):
    """Check if a file exists in S3."""
    try:
        S3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        raise

# ============================================================================
# Terra Data Table Processing
# ============================================================================

def normalize_value(value):
    """
    Normalize Terra attribute values to strings.
    - None/empty -> ""
    - GCS URIs -> kept as-is
    - Lists/tuples -> semicolon-separated
    - Dicts -> JSON
    - Everything else -> string
    """
    if value is None:
        return ""
    
    if is_gs_uri(value):
        return value
    
    if isinstance(value, (list, tuple)):
        return ";".join("" if x is None else str(x) for x in value)
    
    if isinstance(value, dict):
        try:
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            return str(value)
    
    return str(value)


def fetch_terra_entities():
    """
    Fetch all entities from Terra workspace for the submission entity type.
    Returns list of entity dictionaries with 'id' and attributes.
    """
    # Verify entity type exists
    resp = fapi.list_entity_types(TERRA_PROJECT, TERRA_WORKSPACE)
    if resp.status_code != 200:
        log_print(f"ERROR: Failed to list entity types: {resp.text}")
        sys.exit(1)
    
    # Fetch entities with pagination
    page = 1
    page_size = 10000
    entities = []
    
    while True:
        resp = fapi.get_entities_query(
            TERRA_PROJECT,
            TERRA_WORKSPACE,
            TERRA_SUBMISSIONENTITY,
            page=page,
            page_size=page_size
        )
        
        if resp.status_code != 200:
            log_print(f"ERROR: Failed to fetch entities: {resp.text}")
            sys.exit(1)
        
        payload = resp.json()
        batch = payload.get("results", [])
        
        if not batch:
            break
        
        for entity_json in batch:
            attrs = dict(entity_json.get("attributes", {}))
            attrs["id"] = entity_json.get("name", "")
            entities.append(attrs)
        
        if len(batch) < page_size:
            break
        
        page += 1
    
    log_print(f"Fetched {len(entities)} entities from Terra")
    return entities


def separate_attributes(entities):
    """
    Separate entity attributes into value attributes and file attributes.
    File attributes contain GCS URIs or arrays of GCS URIs.
    Returns (value_attrs, file_attrs) as sets.
    """
    value_attrs = set()
    file_attrs = set()
    
    for entity in entities:
        for key, value in entity.items():
            if key == "id":
                continue
            
            if value in (None, ""):
                continue
            
            if is_gs_uri(value) or all_gs_uris(value):
                file_attrs.add(key)
            else:
                value_attrs.add(key)
    
    return value_attrs, file_attrs


def create_terra_table(entities, value_attrs, file_attrs):
    """
    Create Terra data table CSV containing both value and file columns.
    Returns (table_filepath, file_rows) where file_rows contains GCS URIs.
    """
    if not entities:
        log_print("No entities found, skipping Terra table creation.")
        return None, []
    
    value_rows = []
    file_rows = []
    
    for entity in entities:
        entity_id = entity.get("id", "")
        
        # Value row: includes all attributes normalized to strings
        value_row = {"id": entity_id}
        for attr in value_attrs:
            value_row[attr] = normalize_value(entity.get(attr))
        for attr in file_attrs:
            value_row[attr] = ""  # Placeholder for file columns
        value_rows.append(value_row)
        
        # File row: only includes GCS URIs
        file_row = {"id": entity_id}
        for attr in file_attrs:
            file_row[attr] = normalize_value(entity.get(attr))
        file_rows.append(file_row)
    
    # Write value rows to CSV
    headers = ["id"] + sorted(value_attrs) + sorted(file_attrs)
    table_file = "terra_table.csv"
    
    with open(table_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(value_rows)
    
    log_print(f"Created Terra table: {table_file}")
    return table_file, file_rows


def fetch_table():
    """
    Fetch Terra data table and separate into value table and file URIs.
    Returns (table_filepath, file_rows).
    """
    entities = fetch_terra_entities()
    
    if not entities:
        return None, []
    
    value_attrs, file_attrs = separate_attributes(entities)
    return create_terra_table(entities, value_attrs, file_attrs)


# ============================================================================
# Workflow Schema
# ============================================================================

def load_workflow_schema():
    """
    Load workflow-specific schema configuration.
    Schema defines which files are reportable and their types.
    """
    log_print(f"Loading schema for workflow: {TERRA_WORKFLOW}")
    
    scheme_dir = Path(__file__).resolve().parent / "schemes"
    if not scheme_dir.is_dir():
        log_print(f"WARNING: Schema directory not found: {scheme_dir}")
        return {}
    
    # Find matching schema file
    workflow_name = (TERRA_WORKFLOW or "").lower()
    
    for schema_file in scheme_dir.glob("*.json"):
        try:
            with open(schema_file, "r", encoding="utf-8") as f:
                schema = json.load(f)
            
            schema_workflow = str(schema.get("workflow", "")).lower()
            if schema_workflow and schema_workflow in workflow_name:
                log_print(f"Using schema: {schema.get('scheme', 'Unknown')}")
                return schema
        except Exception as e:
            log_print(f"Warning: Failed to load schema {schema_file}: {e}")
    
    log_print("No matching schema found, proceeding without schema")
    return {}


# ============================================================================
# Metadata Generation
# ============================================================================

def extract_reportable_columns(schema):
    """
    Extract reportable file columns from schema.
    Returns dict mapping column_name -> file_type.
    """
    reportable = {}
    
    for item in schema.get('reportable_files', []):
        if isinstance(item, dict) and 'terra_column' in item:
            reportable[item['terra_column']] = item.get('type', 'other')
    
    return reportable


def identify_run_level_files(file_rows, terra_table_path):
    """
    Identify run-level files (files that appear in all samples).
    Returns dict of column_name -> gcs_uri for run-level files.
    """
    file_counts = {}
    
    # Count occurrences of each file
    for row in file_rows:
        for key, value in row.items():
            if key == "id" or not value:
                continue
            file_counts[value] = file_counts.get(value, 0) + 1
    
    # Files appearing more than once are run-level
    run_files = {}
    for row in file_rows:
        for key, value in row.items():
            if key == "id" or not value:
                continue
            if file_counts[value] >= 2:
                run_files[key] = value
    
    return run_files


def create_per_sample_terra_tables(terra_table_path):
    """
    Split Terra table CSV into per-sample files.
    Returns list of dicts with sample_id -> temp_file_path.
    """
    if not terra_table_path or not os.path.exists(terra_table_path):
        log_print(f"Terra table not found: {terra_table_path}")
        return []
    
    try:
        df = pd.read_csv(terra_table_path)
    except Exception as e:
        log_print(f"ERROR: Failed to read Terra table: {e}")
        return []
    
    if df.empty:
        log_print("Terra table is empty")
        return []
    
    if 'id' not in df.columns:
        log_print("WARNING: No 'id' column in Terra table")
        return []
    
    per_sample_files = []
    
    for _, row in df.iterrows():
        sample_id = str(row.get('id', '')).strip()
        if not sample_id:
            continue
        
        id_csv_path = os.path.join(TEMP_DIR, f"{sample_id}__{TERRA_TABLE_BASENAME}")
        
        try:
            row.to_frame().T.to_csv(id_csv_path, index=False)
            per_sample_files.append({
                'id': sample_id,
                'path': id_csv_path
            })
        except Exception as e:
            log_print(f"WARNING: Could not create per-sample table for {sample_id}: {e}")
    
    log_print(f"Created {len(per_sample_files)} per-sample Terra tables")
    return per_sample_files


def build_file_metadata(file_rows, terra_table_path, schema):
    """
    Build comprehensive metadata for all files to transfer.
    Returns list of file metadata dicts with origin, destination, etc.
    """
    reportable = extract_reportable_columns(schema)
    run_files = identify_run_level_files(file_rows, terra_table_path)
    
    # Add run-level files as a pseudo-row
    file_rows.append(run_files)
    
    all_files = []
    
    # Process GCS files from file_rows
    for row in file_rows:
        sample_id = row.get('id', '')
        
        for column, gcs_uri in row.items():
            if column == 'id':
                continue
            
            # Skip run-level files when processing sample rows
            if sample_id and column in run_files:
                continue
            
            if not gcs_uri:
                continue
            
            filename = os.path.basename(gcs_uri)
            dest_key = (
                f"data/id={sample_id}/workflow={TERRA_WORKFLOW}/"
                f"run={TERRA_SUBMISSIONENTITY}/file={filename}/"
                f"timestamp={TERRA_SUBMISSIONTIME}/{filename}"
            )
            
            all_files.append({
                'id': sample_id,
                'workflow': TERRA_WORKFLOW,
                'run': TERRA_SUBMISSIONENTITY,
                'file': filename,
                'timestamp': int(TERRA_SUBMISSIONTIME),
                'origin': gcs_uri,
                'current': f"s3://{DEST_BUCKET}/{dest_key}",
                'reportable': column in reportable,
                'type': reportable.get(column, None)
            })
    
    # Add per-sample Terra tables
    per_sample_tables = create_per_sample_terra_tables(terra_table_path)
    
    for item in per_sample_tables:
        sample_id = item['id']
        local_path = item['path']
        
        dest_key = (
            f"{RAW_PREFIX}/id={sample_id}/workflow={TERRA_WORKFLOW}/"
            f"run={TERRA_SUBMISSIONENTITY}/file={TERRA_TABLE_BASENAME}/"
            f"timestamp={TERRA_SUBMISSIONTIME}/{TERRA_TABLE_BASENAME}"
        )
        
        all_files.append({
            'id': sample_id,
            'workflow': TERRA_WORKFLOW,
            'run': TERRA_SUBMISSIONENTITY,
            'file': TERRA_TABLE_BASENAME,
            'timestamp': int(TERRA_SUBMISSIONTIME),
            'origin': local_path,
            'current': f"s3://{DEST_BUCKET}/{dest_key}",
            'reportable': 'terra_table' in reportable,
            'type': reportable.get('terra_table', None)
        })
    
    log_print(f"Built metadata for {len(all_files)} files")
    return all_files


def gather_metadata(file_rows, terra_table_path, schema):
    """Main entry point for metadata gathering."""
    return build_file_metadata(file_rows, terra_table_path, schema)


# ============================================================================
# File Transfer
# ============================================================================

def transfer_local_file(origin_path, s3_bucket, s3_key):
    """Transfer a local file to S3."""
    try:
        S3.upload_file(origin_path, s3_bucket, s3_key)
        return True
    except Exception as e:
        log_print(f"ERROR: Local file transfer failed: {e}")
        traceback.print_exc()
        return False


def transfer_gcs_file(gcs_uri, s3_bucket, s3_key):
    """Transfer a file from GCS to S3."""
    try:
        gcs_client = gcs_storage.Client(project=TERRA_PROJECT)
        gcs_bucket, gcs_path = parse_uri(gcs_uri)
        gcs_blob = gcs_client.bucket(gcs_bucket).blob(gcs_path)
        
        if not gcs_blob.exists():
            raise FileNotFoundError(f"GCS blob not found: {gcs_uri}")
        
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            gcs_blob.download_to_filename(tmp.name)
            S3.upload_file(tmp.name, s3_bucket, s3_key)
            os.remove(tmp.name)
        
        return True
        
    except Exception as e:
        log_print(f"ERROR: GCS transfer failed: {e}")
        traceback.print_exc()
        return False


def transfer_single_file(file_metadata):
    """
    Transfer a single file to S3.
    Returns (success_metadata, failure_metadata) tuple.
    """
    origin = file_metadata['origin']
    destination = file_metadata['current']
    s3_bucket, s3_key = parse_uri(destination)
    
    log_print(f"Transferring: {origin} -> {destination}")
    
    # Determine transfer method based on origin
    if origin.startswith('gs://'):
        success = transfer_gcs_file(origin, s3_bucket, s3_key)
    else:
        success = transfer_local_file(origin, s3_bucket, s3_key)
    
    if success:
        return file_metadata, None
    else:
        return None, file_metadata


def transfer_files(file_metadata_list):
    """
    Transfer all files to S3 in parallel.
    Returns (success_list, failure_list).
    """
    success_list = []
    failure_list = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        
        # Submit transfer jobs, skipping existing files
        for metadata in file_metadata_list:
            s3_bucket, s3_key = parse_uri(metadata['current'])
            
            if s3_file_exists(s3_bucket, s3_key):
                log_print(f"File already exists, skipping transfer: {metadata['current']}")
                success = data_processing.standardize_data(metadata, FILES_TABLE_KEYS)
                success_list.append(success)
                continue
            
            future = executor.submit(transfer_single_file, metadata)
            futures[future] = metadata
        
        # Collect results
        for future in as_completed(futures):
            metadata = futures[future]
            try:
                success, failure = future.result()
                if success:
                    success = data_processing.standardize_data(success, FILES_TABLE_KEYS)
                    success_list.append(success)
                if failure:
                    failure_list.append(failure)
            except Exception as e:
                log_print(f"ERROR: Transfer failed for {metadata['origin']}: {e}")
                failure_list.append(metadata)
    
    # Upload results
    if success_list:
        df = pd.DataFrame(success_list)
        df = data_processing.clean_dataframe(df)

        io_ops.write_delta(
            df=df, 
            uri=f"s3://{DEST_BUCKET}/{FILES_PREFIX}",
            key_cols=FILES_TABLE_KEYS,
            partition_by=FILES_TABLE_PARTITIONS
            )
        
    return success_list, failure_list


# ============================================================================
# Cache Update
# ============================================================================

def write_cache_status(transfer_status):
    """
    Write transfer status back to cache for the coordinator script to read.
    """

    row = {
        "project": TERRA_PROJECT,
        "workspace": TERRA_WORKSPACE,
        "entity_type": TERRA_SUBMISSIONENTITY,
        "workflow_name": TERRA_WORKFLOW,
        "submission_id": TERRA_SUBMISSIONID,
        "submission_time": str(TERRA_SUBMISSIONTIME),
        "transfer_status": transfer_status,
    }

    io_ops.write_delta(
        df=pd.DataFrame([row]),
        uri=f"s3://{DEST_BUCKET}/{TERRA_CACHE_PREFIX}",
        key_cols=TERRA_CACHE_TABLE_MERGE_KEYS,
        partition_by=TERRA_CACHE_TABLE_PARTITIONS
    )

    log_print("Updated status in cache")


# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    """Execute the complete Terra to S3 transfer pipeline."""
    log_print("="*60)
    log_print(f"Starting Terra transfer for submission: {TERRA_SUBMISSIONID}")
    log_print(f"Workspace: {TERRA_PROJECT}/{TERRA_WORKSPACE}")
    log_print(f"Workflow: {TERRA_WORKFLOW}")
    log_print("="*60)
    
    # Setup
    set_google_credentials()
    
    # Fetch Terra data
    terra_table_path, file_rows = fetch_table()
    
    # Load workflow schema
    schema = load_workflow_schema()
    
    # Build file metadata
    file_metadata = gather_metadata(file_rows, terra_table_path, schema)
    
    if not file_metadata:
        log_print("No files to transfer")
        return
    
    # Transfer files
    success_list, failure_list = transfer_files(file_metadata)
    
    # Determine final status
    if success_list and not failure_list:
        status = "complete"
    elif success_list and failure_list:
        status = "partial"
    else:
        status = "failed"
    
    # Update cache
    write_cache_status(status)
    
    log_print("="*60)
    log_print(f"Transfer finished with status: {status}")
    log_print("="*60)


if __name__ == '__main__':
    main()