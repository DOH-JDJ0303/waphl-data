import argparse
from collections import defaultdict

import pandas as pd
from deltalake import DeltaTable


def get_args():
    parser = argparse.ArgumentParser(
        description="Delete rows from a Delta Lake table matching one or more column=value filters. "
                    "Filters on partition columns are used to prune partitions."
    )
    parser.add_argument("--table", required=True, help="Path to Delta table (local or remote)")
    parser.add_argument(
        "--where",
        required=True,
        action="append",
        metavar="COL=VALUE",
        help="Column/value filter, e.g. --where workflow=phoenix. Repeatable; all conditions are AND'd.",
    )
    parser.add_argument(
        "--backup",
        metavar="PATH",
        help="Optional path to write a CSV of the rows that match (and will be / would be deleted).",
    )
    parser.add_argument(
        "--delete-files",
        action="store_true",
        help="Also delete the S3 objects referenced in the 'current' column of matched rows.",
    )
    parser.add_argument("--vacuum", action="store_true", help="Vacuum table after deletion")
    parser.add_argument("--vacuum-hours", type=int, default=168, help="Retention hours for vacuum (default: 168 / 7 days)")
    parser.add_argument(
        "--execute",
        action="store_true",
        default=False,
        help="Actually perform the delete. Omit to dry run (default).",
    )
    args = parser.parse_args()

    filters = []
    for item in args.where:
        if "=" not in item:
            parser.error(f"Invalid --where '{item}': expected COL=VALUE.")
        col, val = item.split("=", 1)
        col, val = col.strip(), val.strip()
        if not col or val == "":
            parser.error(f"Invalid --where '{item}': column and value must both be non-empty.")
        filters.append((col, val))

    args.filters = filters
    return args


def load_table(table_path):
    return DeltaTable(table_path, storage_options={"AWS_S3_ALLOW_UNSAFE_RENAME": "true"})


def partition_columns(dt):
    return set(dt.metadata().partition_columns or [])


def column_type(dt, column):
    for f in dt.schema().fields:
        if f.name == column:
            return str(f.type).strip('"').lower()
    return None


def split_filters(dt, filters):
    part_cols = partition_columns(dt)
    partition_filters = [(c, v) for c, v in filters if c in part_cols]
    other_filters = [(c, v) for c, v in filters if c not in part_cols]
    return partition_filters, other_filters


def build_predicate(dt, column, value):
    numeric = {"byte", "short", "int", "integer", "long", "float", "double", "decimal"}
    ctype = column_type(dt, column)
    if ctype in numeric or ctype == "boolean":
        return f"{column} = {value}"
    escaped = value.replace("'", "''")
    return f"{column} = '{escaped}'"


def full_predicate(dt, filters):
    return " AND ".join(build_predicate(dt, c, v) for c, v in filters)


def coerce_value(value, series):
    dtype = series.dtype
    try:
        if pd.api.types.is_bool_dtype(dtype):
            return value.strip().lower() in ("true", "1", "yes", "t")
        if pd.api.types.is_integer_dtype(dtype):
            return int(value)
        if pd.api.types.is_float_dtype(dtype):
            return float(value)
    except (ValueError, TypeError):
        pass
    return value


def scoped_read(dt, partition_filters):
    if partition_filters:
        pushdown = [(c, "=", v) for c, v in partition_filters]
        return dt.to_pandas(partitions=pushdown)
    return dt.to_pandas()


def find_affected(dt, filters):
    """Return (affected_df, missing_cols). Reads only within any partition scope."""
    part_f, other_f = split_filters(dt, filters)
    df = scoped_read(dt, part_f)

    missing = [c for c, _ in other_f if c not in df.columns]
    if missing:
        return None, missing

    affected = df
    for col, val in other_f:
        affected = affected[affected[col] == coerce_value(val, affected[col])]
    return affected.copy(), []


def collect_paths(df, column="current"):
    if df is None or column not in df.columns:
        return set()
    return {
        str(v).strip()
        for v in df[column].dropna().tolist()
        if str(v).strip() != ""
    }


def parse_s3_uri(uri):
    if not uri.startswith("s3://"):
        return None
    bucket, _, key = uri[5:].partition("/")
    if not bucket or not key:
        return None
    return bucket, key


def delete_s3_objects(paths, execute):
    import boto3

    by_bucket = defaultdict(list)
    unparseable = []
    for p in paths:
        parsed = parse_s3_uri(p)
        if parsed:
            by_bucket[parsed[0]].append(parsed[1])
        else:
            unparseable.append(p)

    if unparseable:
        print(f"  [warn] {len(unparseable)} path(s) are not s3:// URIs and will be skipped, e.g. {unparseable[:3]}")

    total = sum(len(v) for v in by_bucket.values())
    if not execute:
        print(f"  [DRY RUN] Would delete {total:,} S3 object(s) across {len(by_bucket)} bucket(s).")
        return

    s3 = boto3.client("s3")
    deleted = 0
    for bucket, keys in by_bucket.items():
        for i in range(0, len(keys), 1000):  # delete_objects caps at 1000/request
            chunk = keys[i:i + 1000]
            resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in chunk]})
            deleted += len(resp.get("Deleted", []))
            for err in resp.get("Errors", []):
                print(f"  [error] {bucket}/{err.get('Key')}: {err.get('Message')}")
    print(f"  Deleted {deleted:,} S3 object(s).")


def write_backup(affected, path):
    affected.to_csv(path, index=False)
    print(f"  Backup written : {len(affected):,} row(s) -> {path}")


def dry_run(dt, filters, backup=None, delete_files=False):
    print("\n[DRY RUN] Scanning for rows to delete...")
    part_f, other_f = split_filters(dt, filters)
    if part_f:
        print(f"  Partition scope : {', '.join(f'{c}={v}' for c, v in part_f)}")
    else:
        print("  Partition scope : (none — full table scan)")
    if other_f:
        print(f"  Column filters  : {', '.join(f'{c}={v}' for c, v in other_f)}")

    affected, missing = find_affected(dt, filters)
    if missing:
        print(f"  Column(s) not found: {missing}.")
        return

    count = len(affected)
    print(f"  Predicate       : {full_predicate(dt, filters)}")
    print(f"  Row count       : {count:,}")
    if count > 0:
        print("\nSample (up to 5 rows):")
        print(affected.head())
        if backup:
            print()
            write_backup(affected, backup)
        if delete_files:
            paths = sorted(collect_paths(affected))
            print(f"\n  Files to delete : {len(paths):,}")
            if paths:
                delete_s3_objects(paths, execute=False)
    print("\nRe-run with --execute to perform the delete.")


def delete(dt, filters, backup=None, delete_files=False, vacuum=False, vacuum_hours=168):
    affected, missing = find_affected(dt, filters)
    if missing:
        print(f"[ABORT] Column(s) not found: {missing}. Nothing deleted.")
        return
    if affected.empty:
        print("[SKIP] No rows match the filters. Nothing to delete.")
        return

    if backup:
        print("\n[BACKUP] Saving rows to be deleted...")
        write_backup(affected, backup)

    # Capture file paths BEFORE removing rows.
    paths = sorted(collect_paths(affected)) if delete_files else []

    predicate = full_predicate(dt, filters)
    print(f"\n[EXECUTING] Deleting rows where {predicate} ...")
    dt.delete(predicate)
    print("Done.")

    if delete_files:
        print(f"\n[DELETE FILES] {len(paths):,} object(s) to remove")
        if paths:
            delete_s3_objects(paths, execute=True)

    if vacuum:
        enforce = vacuum_hours > 0
        print(f"\n[VACUUM] Cleaning up files (retention: {vacuum_hours}h)...")
        dt.vacuum(retention_hours=vacuum_hours, enforce_retention_duration=enforce)
        print("Vacuum complete.")


def main():
    args = get_args()
    dt = load_table(args.table)

    if args.execute:
        delete(dt, args.filters, args.backup, args.delete_files, args.vacuum, args.vacuum_hours)
    else:
        dry_run(dt, args.filters, args.backup, args.delete_files)


if __name__ == "__main__":
    main()
