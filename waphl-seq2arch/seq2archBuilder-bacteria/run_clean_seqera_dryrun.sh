#!/bin/bash


# Clean Bacteria Production Workspace
python clean_seqera.py --production-level production --seq_project bacteria --keep-days 90 --dry-run
python clean_seqera.py --workflow DOH-JDJ0303/bs-fetch-nf --production-level production --seq_project bacteria --status FAILED --keep-days 2 --dry-run
python clean_seqera.py --production-level production --seq_project bacteria --status CANCELLED --dry-run