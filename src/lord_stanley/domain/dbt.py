"""
Run Lord Stanley domain dbt models
"""

import subprocess

from lord_stanley.config import ROOT_DIR


def run_mart_models() -> None:
    """
    Run dbt mart models
    """
    subprocess.run(["dbt", "run", "--select", "mart"], cwd=ROOT_DIR, check=True)
