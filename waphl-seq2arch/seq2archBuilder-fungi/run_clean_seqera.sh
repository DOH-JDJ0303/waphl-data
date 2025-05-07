#!/bin/bash


# Clean Fungi Production Workspace
python clean_seqera.py --production-level production --seq_project fungi --keep-days 90
python clean_seqera.py --workflow DOH-JDJ0303/bs-fetch-nf --production-level production --seq_project fungi --status FAILED --keep-days 2
python clean_seqera.py --production-level production --seq_project fungi --status CANCELLED