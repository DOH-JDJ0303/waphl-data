#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
from collections import defaultdict
from typing import Dict, Any, List, Tuple, DefaultDict

from shared import data_processing, ui

# ---------- VAPER ---------- 
def process_vaper(df_summary: pd.DataFrame) -> pd.DataFrame:
    """
    Expand rows containing multiple assembly files into one row per assembly
    and derive a reference name from each assembly filename.

    The reference is taken as the token after the final underscore in the
    assembly filename's stem.
    """
    df_summary = df_summary.copy()

    if "assembly" not in df_summary.columns:
        ui.push_message(
            "process_vaper: no 'assembly' column found in summary data; "
            "'reference' column will be empty",
            type="warning",
        )
        df_summary["reference"] = None
        return df_summary

    # Normalize assembly values to lists so explode() works consistently
    def _normalize_assembly(x):
        if isinstance(x, list):
            return x

        if pd.isna(x):
            return []

        return [x]

    df_summary["assembly"] = df_summary["assembly"].apply(_normalize_assembly)

    # One row per assembly path
    df_summary = df_summary.explode("assembly", ignore_index=True)

    def _reference_from_assembly(path):
        if pd.isna(path):
            return np.nan

        stem = data_processing.extract_stem(path)
        return stem.rsplit("_", 1)[-1] if stem else np.nan

    df_summary["reference"] = df_summary["assembly"].apply(_reference_from_assembly)

    return df_summary