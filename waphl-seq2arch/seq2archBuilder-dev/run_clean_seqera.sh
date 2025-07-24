#!/bin/bash


# Clean Dev Space
python clean_seqera.py --production-level dev --seq_project bacteria --status CANCELLED
python clean_seqera.py --production-level dev --seq_project bacteria --status FAILED --workflow DOH-JDJ0303/bs-fetch-nf --keep-days 2
python clean_seqera.py --production-level dev --seq_project bacteria --keep-days 90