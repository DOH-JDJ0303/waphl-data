#!/usr/bin/env python3
import os
import pandas as pd
from collections import defaultdict
from typing import Dict, Any, List, Tuple, DefaultDict

from shared import data_processing

# ---------- MYCOSNP ----------
def process_mycosnp(sample_files, global_files, df_queue):
    sample_files_out, global_files_out = {}, {}

    for (sid, id_alt, run), sfiles in sample_files.items():
        row = {}
        # determine Clade
        summary, clade = None, None
        for summary in global_files.get(run, {}).get('summary', []):
            df_summary = data_processing.read_table(summary)

            if df_summary is None or df_summary.empty:
                continue

            if not all(c in df_summary.columns for c in ['Sample', 'Predicted_Subtype']):
                continue
                
            for _sid in df_summary["Sample"].values:
                if sid == _sid:
                    clade = df_summary[df_summary["Sample"] == sid]["Predicted_Subtype"].iloc[0]
                    row["summary"] = [summary]            
                    break

        clade_matcher = f"mycosnp-{clade.lower().replace('_', '-')}" if clade else None
            
        # Process sample files
        for fgroup, filepaths in sfiles.items():
            if fgroup == 'summary':
                continue
            for filepath in filepaths:
                row.setdefault(fgroup, []).append(filepath)

        # Process global files
        for fgroup, filepaths in global_files.get(run, {}).items():
            for filepath in filepaths:
                filepath_origin = df_queue[df_queue['current'] == filepath]['origin'].iloc[0].lower()
                if (clade is not None and clade_matcher in filepath_origin) or ('clade-' not in filepath_origin):
                    row.setdefault(fgroup, []).append(filepath)

        sample_files_out[(sid, id_alt, run)] = row
        
    return sample_files_out, global_files_out


# ---------- VAPER ---------- 
def process_vaper(sample_files, global_files):
    sample_files_out = {}
    for (sid, id_alt, run), sfiles in sample_files.items():
        assemblies = sfiles.get("assembly", [])
        summaries  = sfiles.get("summary", [])
        summary_map = {data_processing.extract_stem(summary): summary for summary in summaries}

        if len(assemblies) == 0:
            row = {k: v for k, v in sfiles.items() if k not in ['assembly', 'summary']}

            row['assembly'] = []
            row['summary']  = []

            sample_files_out[(sid, id_alt, run, None)] = row

        for assembly in assemblies:
            row = {k: v for k, v in sfiles.items() if k not in ['assembly', 'summary']}
            a_stem   = data_processing.extract_stem(assembly)
            ref_name = a_stem.replace(f"{sid}_T1_", '')

            row['assembly'] = [assembly]
            row['summary']  = [summary_map.get(f"{a_stem}.summaryline")]

            sample_files_out[(sid, id_alt, run, ref_name)] = row
     
    return sample_files_out, global_files