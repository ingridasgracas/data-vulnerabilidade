"""Enrich processed IBGE municipalities with population data.

Chooses real SIDRA-normalized CSV if present and non-empty, otherwise uses seed.
Writes enriched parquet and CSV to data/processed.
"""
from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
DATA = BASE / "data"
PROCESSED = DATA / "processed"
RAW = DATA / "raw"
SEEDS = DATA / "seeds"


def choose_population_csv():
    real = RAW / "ibge_population.csv"
    seed = SEEDS / "ibge_population_seed.csv"
    if real.exists():
        try:
            df = pd.read_csv(real)
            if len(df) > 0:
                print("Using real population CSV:", real)
                return real
            else:
                print("Real population CSV empty, falling back to seed")
        except Exception:
            print("Could not read real population CSV, falling back to seed")
    if seed.exists():
        print("Using seed population CSV:", seed)
        return seed
    raise FileNotFoundError("No population CSV found (neither real nor seed)")


def main():
    PROCESSED.mkdir(parents=True, exist_ok=True)
    parquet = PROCESSED / "ibge_municipios.parquet"
    if not parquet.exists():
        # maybe earlier script saved in data/processed relative to project root
        parquet = BASE / "data" / "processed" / "ibge_municipios.parquet"
    if not parquet.exists():
        raise FileNotFoundError(f"Processed parquet not found: {parquet}")

    df = pd.read_parquet(parquet)
    pop_csv = choose_population_csv()
    pop = pd.read_csv(pop_csv)
    # Ensure municipio_id in both
    if 'municipio_id' not in df.columns:
        raise KeyError('municipio_id not in processed IBGE parquet')
    if 'municipio_id' not in pop.columns:
        # try other column names
        for c in pop.columns:
            if c.lower().startswith('mun') and 'id' in c.lower():
                pop = pop.rename(columns={c: 'municipio_id'})
                break
    # merge
    merged = df.merge(pop[['municipio_id', 'populacao']], on='municipio_id', how='left')
    out_parquet = PROCESSED / 'ibge_enriched.parquet'
    out_csv = PROCESSED / 'ibge_enriched.csv'
    merged.to_parquet(out_parquet, index=False)
    merged.to_csv(out_csv, index=False)
    print('Wrote enriched files:', out_parquet, out_csv)
    print('Enriched head:', merged.head().to_dict())


if __name__ == '__main__':
    main()
