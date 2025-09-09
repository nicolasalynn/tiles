from __future__ import annotations

import re
import time
import json
from pathlib import Path
from typing import Tuple, List, Dict, Any, Optional
import pandas as pd

from powerbanktau.utils import process_with_dask_pipeline
from geney.pipelines import max_splicing_delta

# assume you have these elsewhere:
# from your_module import check_and_parse_input, check_valid_mutations


# def tiles_pipeline(input_path: Path) -> Tuple[bool, List[str], List[Dict[str, Any]], Dict[str, Any]]:
def tiles_pipeline(input_path: Path, job_id: str | None = None) -> Tuple[bool, List[str], List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns:
      success (bool),
      errors (list of str),
      rows (list of dicts for CSV rows if you want to surface them; kept empty here),
      meta (dict for JSON)
    """
    started = time.time()
    errors: List[str] = []
    rows: List[Dict[str, Any]] = []

    # 1) Read & validate input
    input_df = check_and_parse_input(input_path)
    if len(input_df) > 5:
        input_df = input_df.sample(n=5, random_state=42).reset_index(drop=True)
    print(input_df)
    if input_df is None:
        return False, ["Failed to parse input file."], rows, {
            "run_successful": False,
            "error_messages": ["Failed to parse input file."],
            "total_mutations": 0,
            "splice_disrupting_mutations": 0,
            "tss_disrupting_mutations": 0,
            "potential_epitopes": 0,
            "high_pathogenicity_count": 0,
            "processing_time": 0.0,
        }

    out_path = Path(f"/tamir2/nicolaslynn/projects/tiles/temporary_files/job{job_id}.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # 2) Run pipeline
    process_with_dask_pipeline(
        items=input_df.mut_id.unique().tolist(),
        process_func=max_splicing_delta,
        output_file=str(out_path),
        cluster_config={"memory_size": "6GB", "queue": "tamirQ", "num_workers": 5, "walltime": "180:00:00"},
        func_kwargs={"splicing_engine": "spliceai"},
        default_result=0,
        batch_size=20,
        save_interval=1,
        item_to_dict=None,
        cluster_type="pbs",
        append_mode=True,
    )

    # 3) Compute meta (prefer from output if available)
    total_mutations = int(input_df["mut_id"].nunique()) if "mut_id" in input_df.columns else len(input_df)
    splice_disrupting = 0
    tss_disrupting = 0

    if out_path.exists():
        try:
            df_out = pd.read_csv(out_path)
            # Fallbacks if columns are missing
            if "mut_id" in df_out.columns:
                if "max_splicing_delta" in df_out.columns:
                    splice_disrupting = int(
                        df_out.loc[df_out["max_splicing_delta"].abs() > 0.5, "mut_id"].nunique()
                    )
                if "max_tss_delta" in df_out.columns:
                    tss_disrupting = int(
                        df_out.loc[df_out["max_tss_delta"].abs() > 0.5, "mut_id"].nunique()
                    )
            rows = df_out
        except Exception as e:
            errors.append(f"Warning: could not read pipeline output for metrics: {e!r}")

    meta = {
        "run_successful": True,
        "error_messages": errors,
        "total_mutations": total_mutations,
        "mutations_processed": 0,
        "splice_disrupting_muts": splice_disrupting,
        "tss_disrupting_muts": tss_disrupting,
        "potential_epitopes": 11,
        "pathogenic_muts": 2,
        "known_pathogenic_muts": 1,
        "lof_mutations": 0,
        "gof_mutations": 1,
        "key_genes_affected": 'BRCA, TP53',
        "mutations_failed": 0,
        "processing_time": round(time.time() - started, 3),
        "job_id": job_id,
        "output_csv": str(out_path),
    }

    return True, errors, rows, meta



def check_and_parse_input(input_path: Path) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Reads a file containing one mutation ID per line and returns (DataFrame, job_id).
    Ensures the DataFrame has a single column named 'mut_id'.
    Lines starting with '#' are ignored.
    """
    input_path = Path(input_path)

    mutations = open(input_path, 'r').readlines()
    try:
        # Try flexible CSV read (handles CSV/TSV/whitespace; single field per row still works)
        df = pd.DataFrame(mutations, columns=["mut_id"])

    except Exception:
        # Fallback: plain text
        lines = [ln for ln in input_path.read_text().splitlines() if ln and not ln.lstrip().startswith("#")]
        df = pd.DataFrame({"mut_id": lines}, dtype=str)

    if df is None or df.empty:
        return None, job_id

    # Normalize basic whitespace
    df["mut_id"] = df["mut_id"].astype(str).str.strip()
    # Drop blank rows
    df = df[df["mut_id"] != ""].reset_index(drop=True)

    df['gene_name'] = df['mut_id'].apply(lambda x: re.split(r'[:\s]', x)[0] if pd.notna(x) else x)
    from geney import available_genes
    valid_genes = [g for g in available_genes('hg38')]
    df = df[df['gene_name'].isin(valid_genes)]
    return df




if __name__ == "__main__":
    # Path to your input file
    input_file = Path("sample_input.txt")

    # Give it a manual job_id (e.g., from timestamp)
    job_id = f"localtest_{int(time.time())}"

    print("Running tiles_pipeline...")
    success, errors, rows, meta = tiles_pipeline(input_file, job_id=job_id)

    print("Success:", success)
    print("Errors:", errors)
    print("Meta:", json.dumps(meta, indent=2))