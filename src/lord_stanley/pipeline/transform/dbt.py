"""
Run NHL pipeline dbt models
"""

import subprocess

from lord_stanley.config import ROOT_DIR


def run_stg_schedule() -> None:
    """
    Run stg_schedule dbt model
    """
    subprocess.run(["dbt", "run", "--select", "stg_schedule"], cwd=ROOT_DIR, check=True)
