#!/bin/bash


# Clean Bacteria Production Workspace
python clean_seqera.py --production-level production --seq_project bacteria --keep-days 90
python clean_seqera.py --workflow DOH-JDJ0303/bs-fetch-nf --production-level production --seq_project bacteria --status FAILED --keep-days 2
python clean_seqera.py --production-level production --seq_project bacteria --status CANCELLED

# # Clean Dev Space
# python clean_seqera.py --production-level dev --seq_project bacteria --status CANCELLED
# python clean_seqera.py --production-level dev --seq_project bacteria --status FAILED --workflow DOH-JDJ0303/bs-fetch-nf --keep-days 2
# python clean_seqera.py --production-level dev --seq_project bacteria --keep-days 90

# # Clean Mycosnp
# python clean_seqera.py --production-level production --seq_project mycosnp --status CANCELLED
# python clean_seqera.py --production-level production --seq_project mycosnp --workflow DOH-JDJ0303/bs-fetch-nf  --status FAILED --keep-days 2
# python clean_seqera.py --production-level production --seq_project mycosnp --keep-days 90

# # Clean Viral Production Workspace
# python clean_seqera.py --production-level production --seq_project virus --keep-days 90
# python clean_seqera.py --workflow DOH-JDJ0303/bs-fetch-nf --production-level production --seq_project virus --status FAILED --keep-days 2
# python clean_seqera.py --production-level production --seq_project virus --status CANCELLED

# # Clean Fungi Production Workspace
# python clean_seqera.py --production-level production --seq_project fungi --keep-days 90
# python clean_seqera.py --workflow DOH-JDJ0303/bs-fetch-nf --production-level production --seq_project fungi --status FAILED --keep-days 2
# python clean_seqera.py --production-level production --seq_project fungi --status CANCELLED