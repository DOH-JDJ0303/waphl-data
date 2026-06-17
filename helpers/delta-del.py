import argparse
from deltalake import DeltaTable


def get_args():
    parser = argparse.ArgumentParser(
        description="Delete data from a Delta Lake table by partition."
    )
    parser.add_argument("--table", required=True, help="Path to Delta table (local or remote)")
    parser.add_argument("--partition-col", required=True, help="Partition column name")
    parser.add_argument("--partition-val", required=True, help="Partition value to delete")
    parser.add_argument("--vacuum", action="store_true", help="Vacuum table after deletion")
    parser.add_argument("--vacuum-hours", type=int, default=168, help="Retention hours for vacuum (default: 168 / 7 days)")
    parser.add_argument(
        "--execute",
        action="store_true",
        default=False,
        help="Actually perform the delete. Omit to dry run (default).",
    )
    return parser.parse_args()


def load_table(table_path):
    return DeltaTable(table_path, storage_options={"AWS_S3_ALLOW_UNSAFE_RENAME": "true"})


def dry_run(dt, partition_col, partition_val):
    print(f"\n[DRY RUN] Scanning for rows to delete...")
    df = dt.to_pandas()
    affected = df[df[partition_col] == partition_val]
    count = len(affected)
    print(f"  Partition : {partition_col} = '{partition_val}'")
    print(f"  Row count : {count:,}")
    if count > 0:
        print(f"\nSample (up to 5 rows):")
        print(affected.head())
    print("\nRe-run with --execute to perform the delete.")


def delete(dt, partition_col, partition_val, vacuum=False, vacuum_hours=168):
    print(f"\n[EXECUTING] Deleting partition {partition_col} = '{partition_val}'...")
    dt.delete(f"{partition_col} = '{partition_val}'")
    print("Done.")

    if vacuum:
        enforce = vacuum_hours > 0
        print(f"\n[VACUUM] Cleaning up files (retention: {vacuum_hours}h)...")
        dt.vacuum(retention_hours=vacuum_hours, enforce_retention_duration=enforce)
        print("Vacuum complete.")


def main():
    args = get_args()

    dt = load_table(args.table)

    if args.execute:
        delete(dt, args.partition_col, args.partition_val, args.vacuum, args.vacuum_hours)
    else:
        dry_run(dt, args.partition_col, args.partition_val)


if __name__ == "__main__":
    main()