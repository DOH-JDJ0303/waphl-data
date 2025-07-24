#!/bin/bash


# Clean Dev Space
python clean_seqera.py --production-level dev --seq_project bacteria --status CANCELLED --dry-run
python clean_seqera.py --production-level dev --seq_project bacteria --status FAILED --workflow DOH-JDJ0303/bs-fetch-nf --keep-days 2 --dry-run
python clean_seqera.py --production-level dev --seq_project bacteria --keep-days 90 --dry-run