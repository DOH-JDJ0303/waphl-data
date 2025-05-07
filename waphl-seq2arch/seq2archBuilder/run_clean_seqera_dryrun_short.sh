#!/bin/bash


# Clean Bacteria Production Workspace


# Clean Dev Space


# Clean Mycosnp
python clean_seqera.py --production-level dev --seq_project bacteria --status FAILED --workflow DOH-JDJ0303/bs-fetch-nf --keep-days 2 --dry-run

# Clean viral production workspace
# Clean Viral Production Workspace
# python clean_seqera.py --production-level production --seq_project virus --keep-days 90 --dry-run
# python clean_seqera.py --workflow DOH-JDJ0303/bs-fetch-nf --production-level production --seq_project virus --status FAILED --keep-days 2 --dry-run
# python clean_seqera.py --production-level production --seq_project virus --status CANCELLED --dry-run

# Clean fungi Production Workspace
#python clean_seqera.py --production-level production --seq_project fungi --keep-days 90 --dry-run
#python clean_seqera.py --workflow DOH-JDJ0303/bs-fetch-nf --production-level production --seq_project fungi --status FAILED --keep-days 2 --dry-run
#python clean_seqera.py --production-level production --seq_project fungi --status CANCELLED --dry-run