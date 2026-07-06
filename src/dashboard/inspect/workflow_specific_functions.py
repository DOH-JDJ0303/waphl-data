#!/usr/bin/env python3
import os
import pandas as pd
from collections import defaultdict
from typing import Dict, Any, List, Tuple, DefaultDict

from shared import data_processing, ui

# ---------- VAPER ---------- 
def process_vaper(df_summary: pd.DataFrame) -> pd.DataFrame:
    """
    Extract the reference name from each row's assembly file and add it as a
    'reference' column, to be used later for joining against the 'reference'
    column produced elsewhere (e.g. VAPER summary matching).
    Reference is derived as the token after the final underscore in the
    assembly filename's stem. Rows with no assembly value get reference=None.
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

    def _reference_from_assembly(path):
        if not path or (isinstance(path, float) and pd.isna(path)):
            return None
        a_stem = data_processing.extract_stem(path)
        return a_stem.split("_")[-1] if a_stem else None

    df_summary["reference"] = df_summary["assembly"].apply(_reference_from_assembly)

    return df_summary