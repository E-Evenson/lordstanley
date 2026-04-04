"""
Configuration settings for Lord Stanley
"""

from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent  # repo root
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

CURRENT_SEASON = "20252026"
CUP_HOLDER_START = "FLA"

REFERENCE_DATA_DIR = ROOT_DIR / "reference_data"
