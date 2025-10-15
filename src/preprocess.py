"""Preprocessing transforms raw data into cleaned, normalized tables.

This script expects files in data/raw and writes cleaned outputs to data/processed.
"""
import argparse
from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def ensure_dirs():
    (DATA_DIR / "processed").mkdir(parents=True, exist_ok=True)


def normalize_ibge_municipios(raw_path: Path, out_path: Path):
    df = pd.read_json(raw_path)
    # minimal normalization example
    df = df.rename(columns={"nome": "municipio", "id": "municipio_id"})
    df.to_parquet(out_path, index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw", help="raw data dir")
    parser.add_argument("--output", default="data/processed", help="processed data dir")
    args = parser.parse_args()
    ensure_dirs()
    raw = Path(args.input)
    processed = Path(args.output)
    # example
    ibge_raw = raw / "ibge_municipios.json"
    if ibge_raw.exists():
        normalize_ibge_municipios(ibge_raw, processed / "ibge_municipios.parquet")
        print("Processed IBGE municipalities")


if __name__ == "__main__":
    main()
