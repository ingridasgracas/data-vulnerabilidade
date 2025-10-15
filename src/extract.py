"""Extraction utilities for public datasets (IBGE, DATASUS, Cadastro Único, INEP, UNICEF).

This module provides CLI entrypoints to download CSVs or call simple APIs and save raw files to data/raw.
"""
import argparse
import os
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd


DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def ensure_dirs():
    (DATA_DIR / "raw").mkdir(parents=True, exist_ok=True)


def requests_session_with_retries(retries=5, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def download_csv(url: str, dest: Path):
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    dest.write_bytes(resp.content)


def fetch_ibge_municipalities():
    # Example: IBGE municipios CSV (replace with real endpoint)
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    df = pd.json_normalize(resp.json())
    out = DATA_DIR / "raw" / "ibge_municipios.json"
    df.to_json(out, orient="records", force_ascii=False)
    print("Saved", out)


def fetch_ibge_population(retries=True):
    """Fetch IBGE population projections per municipality and save CSV.

    Uses the IBGE projections endpoint which sometimes returns 503; when
    retries=True a session with exponential backoff is used.
    """
    url = "https://servicodados.ibge.gov.br/api/v1/projecoes/populacao/municipios"
    session = requests_session_with_retries() if retries else requests
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    rows = []
    for item in data:
        rows.append({
            "municipio_id": int(item.get("id")) if item.get("id") is not None else None,
            "municipio": item.get("municipio"),
            "populacao": item.get("populacao")
        })
    out = DATA_DIR / "raw" / "ibge_population.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print("Saved population CSV to", out)


def fetch_sidra_population(table=6579, year='last'):
    """Fetch population by municipality using the SIDRA API.

    This uses the IBGE SIDRA API. The `table` parameter may need adjustment
    depending on which SIDRA table contains the desired population series.
    We attempt a broad query and write a CSV if successful.
    """
    # Example SIDRA API: https://apisidra.ibge.gov.br/values/t/6579/n1/all/v/all/p/last
    url = f"https://apisidra.ibge.gov.br/values/t/{table}/n1/all/v/all/p/{year}"
    session = requests_session_with_retries()
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # SIDRA returns a list where first row is header
    if not isinstance(data, list) or len(data) < 2:
        raise ValueError("Unexpected SIDRA response format")
    header = data[0]
    rows = [dict(zip(header, row)) for row in data[1:]]
    out = DATA_DIR / "raw" / f"sidra_table_{table}_population.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    print("Saved SIDRA CSV to", out)


def discover_and_fetch_sidra_population(candidates=None, min_rows=4000):
    """Try a list of SIDRA tables and return the first that appears to be
    municipality-level population data (based on row count).

    Saves the found CSV to data/raw/sidra_table_{table}_population.csv
    and returns the path. If none found, raises RuntimeError.
    """
    if candidates is None:
        candidates = [6579, 1419, 1410, 93, 1688, 204, 262, 205, 59]
    session = requests_session_with_retries()
    for t in candidates:
        try:
            url = f"https://apisidra.ibge.gov.br/values/t/{t}/n1/all/v/all/p/last"
            print(f"Trying SIDRA table {t}...")
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list) or len(data) < 2:
                print(f"Table {t} returned no rows or unexpected format")
                continue
            rows = data[1:]
            if len(rows) >= min_rows:
                # found likely municipality-level table
                header = data[0]
                df = pd.DataFrame([dict(zip(header, r)) for r in rows])
                out = DATA_DIR / "raw" / f"sidra_table_{t}_population.csv"
                df.to_csv(out, index=False)
                print(f"Found likely municipality table: {t}, saved to {out} (rows={len(rows)})")
                return out
            else:
                print(f"Table {t} returned {len(rows)} rows (too few)")
        except Exception as e:
            print(f"Error testing table {t}: {e}")
    raise RuntimeError("No suitable SIDRA table found among candidates")


def brute_force_sidra_search(candidates=None, min_rows=4000, levels=None):
    """Brute-force search trying different territorial levels (n1..n6) for each table.

    This tries URLs of the form:
      /values/t/{table}/{level}/all/v/all/p/last
    for level in levels. Returns the path to saved CSV on success.
    """
    if candidates is None:
        candidates = [6579, 1419, 1410, 93, 1688, 204, 262, 205, 59]
    if levels is None:
        levels = [f"n{i}" for i in range(1, 7)]
    session = requests_session_with_retries()
    try:
        from alive_progress import alive_bar
        use_bar = True
    except Exception:
        use_bar = False

    if use_bar:
        total = len(candidates) * len(levels)
        with alive_bar(total, title='Brute forcing SIDRA') as bar:
            for t in candidates:
                for lvl in levels:
                    try:
                        url = f"https://apisidra.ibge.gov.br/values/t/{t}/{lvl}/all/v/all/p/last"
                        print(f"Trying SIDRA table {t} with level {lvl}...")
                        resp = session.get(url, timeout=30)
                        resp.raise_for_status()
                        data = resp.json()
                        if not isinstance(data, list) or len(data) < 2:
                            print(f"Table {t} returned no rows or unexpected format")
                            bar()
                            continue
                        rows = data[1:]
                        if len(rows) >= min_rows:
                            header = data[0]
                            df = pd.DataFrame([dict(zip(header, r)) for r in rows])
                            out = DATA_DIR / "raw" / f"sidra_table_{t}_{lvl}_population.csv"
                            df.to_csv(out, index=False)
                            print(f"Found likely municipality table: {t}, saved to {out} (rows={len(rows)})")
                            return out
                        else:
                            print(f"Table {t} returned {len(rows)} rows (too few)")
                    except Exception as e:
                        print(f"Error testing table {t}: {e}")
                    bar()
    else:
        for t in candidates:
            for lvl in levels:
                try:
                    url = f"https://apisidra.ibge.gov.br/values/t/{t}/{lvl}/all/v/all/p/last"
                    print(f"Trying SIDRA table {t} with level {lvl}...")
                    resp = session.get(url, timeout=30)
                    resp.raise_for_status()
                    data = resp.json()
                    if not isinstance(data, list) or len(data) < 2:
                        print(f"Table {t} returned no rows or unexpected format")
                        continue
                    rows = data[1:]
                    if len(rows) >= min_rows:
                        header = data[0]
                        df = pd.DataFrame([dict(zip(header, r)) for r in rows])
                        out = DATA_DIR / "raw" / f"sidra_table_{t}_{lvl}_population.csv"
                        df.to_csv(out, index=False)
                        print(f"Found likely municipality table: {t}, saved to {out} (rows={len(rows)})")
                        return out
                    else:
                        print(f"Table {t} returned {len(rows)} rows (too few)")
                except Exception as e:
                    print(f"Error testing table {t}: {e}")
            try:
                url = f"https://apisidra.ibge.gov.br/values/t/{t}/{lvl}/all/v/all/p/last"
                print(f"Trying table {t} with level {lvl}...")
                resp = session.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, list) or len(data) < 2:
                    print(f"-> no data for table {t} level {lvl}")
                    continue
                rows = data[1:]
                print(f"-> returned {len(rows)} rows")
                if len(rows) >= min_rows:
                    header = data[0]
                    df = pd.DataFrame([dict(zip(header, r)) for r in rows])
                    out = DATA_DIR / "raw" / f"sidra_table_{t}_{lvl}_population.csv"
                    df.to_csv(out, index=False)
                    print(f"Found data: table {t} level {lvl} -> saved {out} (rows={len(rows)})")
                    return out
            except Exception as e:
                print(f"Error for table {t} level {lvl}: {e}")
    raise RuntimeError("Brute-force search did not find municipality-level data")


def quick_sidra_search(candidates=None, min_rows=4000):
    """Quick SIDRA search: test each candidate with levels n3 and n4 and v=all.
    If none returns municipality-level rows, return None.
    """
    if candidates is None:
        candidates = [6579, 1419, 1410, 93, 1688, 204, 262, 205, 59]
    session = requests_session_with_retries()
    for t in candidates:
        for lvl in ("n3", "n4"):
            try:
                url = f"https://apisidra.ibge.gov.br/values/t/{t}/{lvl}/all/v/all/p/last"
                print(f"Quick try table {t} level {lvl}...")
                resp = session.get(url, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, list) or len(data) < 2:
                    continue
                rows = data[1:]
                print(f"-> returned {len(rows)} rows for table {t} {lvl}")
                if len(rows) >= min_rows:
                    header = data[0]
                    df = pd.DataFrame([dict(zip(header, r)) for r in rows])
                    out = DATA_DIR / "raw" / f"sidra_quick_table_{t}_{lvl}_population.csv"
                    df.to_csv(out, index=False)
                    print(f"Quick found: {out} (rows={len(rows)})")
                    return out
            except Exception as e:
                print(f"Error quick testing {t} {lvl}: {e}")
    return None


def create_population_seed_from_parquet(parquet_path=None):
    """Create a fallback seed CSV from existing municipalities in the processed parquet.

    The seed assigns a synthetic population using a deterministic function so
    values vary across municipalities. Saved to data/seeds/ibge_population_seed.csv
    and data/raw/ibge_population_seed.csv
    """
    if parquet_path is None:
        parquet_path = DATA_DIR / "processed" / "ibge_municipios.parquet"
    df = pd.read_parquet(parquet_path)
    seed = df[["municipio_id", "municipio"]].copy()
    # deterministic synthetic population: scale municipio_id remainder
    seed["populacao"] = (seed["municipio_id"] % 100000) * 10 + 1000
    seed_dir = DATA_DIR / "seeds"
    seed_dir.mkdir(parents=True, exist_ok=True)
    out_seed = seed_dir / "ibge_population_seed.csv"
    out_raw = DATA_DIR / "raw" / "ibge_population_seed.csv"
    seed.to_csv(out_seed, index=False)
    seed.to_csv(out_raw, index=False)
    print(f"Created population seed: {out_seed} and {out_raw}")
    return out_seed


def sidra_normalize(table=6579, level='n6', year='last'):
    """Fetch SIDRA JSON for given table/level and normalize to municipio_id, municipio, populacao.

    Saves to data/raw/ibge_population.csv and data/seeds/ibge_population_seed.csv
    """
    url = f"https://apisidra.ibge.gov.br/values/t/{table}/{level}/all/v/all/p/{year}"
    session = requests_session_with_retries()
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    # data[0] is header mapping codes to descriptions, data[1:] are rows
    if not isinstance(data, list) or len(data) < 2:
        raise RuntimeError("Unexpected SIDRA JSON format")
    header = data[0]
    rows = data[1:]
    df = pd.DataFrame([dict(zip(header, r)) for r in rows])

    # map columns
    if 'D1C' in df.columns and 'D1N' in df.columns and 'V' in df.columns:
        df['municipio_id'] = pd.to_numeric(df['D1C'], errors='coerce').astype('Int64')
        df['municipio'] = df['D1N'].astype(str).str.replace(r' - [A-Z]{2}$', '', regex=True)
        df['populacao'] = pd.to_numeric(df['V'], errors='coerce').fillna(0).astype(int)
    else:
        # attempt fallback: try to identify municipality code/name and value
        # find numeric-like column for id
        id_col = None
        name_col = None
        val_col = None
        for c in df.columns:
            if id_col is None:
                try:
                    if df[c].dropna().astype(str).str.match(r'^\d{6,7}$').all():
                        id_col = c
                except Exception:
                    pass
            if name_col is None:
                if df[c].dropna().astype(str).str.contains(' - ').any():
                    name_col = c
            if val_col is None:
                try:
                    pd.to_numeric(df[c])
                    val_col = c
                except Exception:
                    pass
        if id_col is None or name_col is None or val_col is None:
            raise RuntimeError('Could not infer SIDRA columns for normalization')
        df['municipio_id'] = pd.to_numeric(df[id_col], errors='coerce').astype('Int64')
        df['municipio'] = df[name_col].astype(str)
        df['populacao'] = pd.to_numeric(df[val_col], errors='coerce').fillna(0).astype(int)

    out_raw = DATA_DIR / 'raw' / 'ibge_population.csv'
    out_seed = DATA_DIR / 'seeds' / 'ibge_population_seed.csv'
    os.makedirs(DATA_DIR / 'seeds', exist_ok=True)
    df[['municipio_id', 'municipio', 'populacao']].to_csv(out_raw, index=False)
    df[['municipio_id', 'municipio', 'populacao']].to_csv(out_seed, index=False)
    print('Normalized SIDRA saved to', out_raw, 'and seed to', out_seed)
    return out_raw


def normalize_sidra_csv_to_population(sidra_csv_path=None):
    """Normalize a SIDRA CSV (like sidra_table_6579_n6_population.csv) into
    a standard population CSV with columns: municipio_id, municipio, populacao.

    If sidra_csv_path is None, attempts to find a file matching
    data/raw/sidra_table_*_n6_population.csv or data/raw/sidra_table_*_population.csv
    """
    import glob
    if sidra_csv_path is None:
        candidates = glob.glob(str(DATA_DIR / "raw" / "sidra_table_*_n6_population.csv"))
        candidates += glob.glob(str(DATA_DIR / "raw" / "sidra_table_*_population.csv"))
        if not candidates:
            raise FileNotFoundError("No sidra_table CSV found to normalize")
        sidra_csv_path = candidates[0]
    df = pd.read_csv(sidra_csv_path)
    # Heuristic mapping: municipality code usually in 'D1C' and name in 'D1N', value in 'V'
    if 'D1C' not in df.columns or 'D1N' not in df.columns or 'V' not in df.columns:
        raise ValueError(f"Unexpected SIDRA columns: {df.columns.tolist()}")
    out = pd.DataFrame()
    out['municipio_id'] = df['D1C'].astype(int)
    # D1N often contains 'Municipio - UF'; keep only the municipality name before ' - '
    out['municipio'] = df['D1N'].astype(str).str.split(' - ').str[0]
    # V is string, may contain '.' thousand separators; coerce to int
    out['populacao'] = pd.to_numeric(df['V'].astype(str).str.replace('.', '', regex=False), errors='coerce').fillna(0).astype(int)
    # Save normalized files
    out_raw = DATA_DIR / 'raw' / 'ibge_population.csv'
    out_seed = DATA_DIR / 'seeds' / 'ibge_population_seed.csv'
    (DATA_DIR / 'seeds').mkdir(parents=True, exist_ok=True)
    out.to_csv(out_raw, index=False)
    out.to_csv(out_seed, index=False)
    print(f"Normalized SIDRA CSV saved to {out_raw} and seed {out_seed}")
    return out_raw


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ibge", action="store_true", help="Fetch IBGE municipalities sample")
    parser.add_argument("--ibge-pop", action="store_true", help="Fetch IBGE population projections (municipios)")
    parser.add_argument("--sidra-pop", action="store_true", help="Fetch population via SIDRA API (table 6579 by default)")
    parser.add_argument("--sidra-discover", action="store_true", help="Discover a SIDRA table that contains municipality-level population data")
    parser.add_argument("--sidra-bruteforce", action="store_true", help="Brute-force SIDRA tables and levels to find municipality data")
    parser.add_argument("--sidra-quick", action="store_true", help="Quick SIDRA search (n3/n4) and fallback to seed if not found")
    parser.add_argument("--sidra-normalize", action="store_true", help="Normalize SIDRA table 6579 n6 into municipio/populacao CSVs")
    parser.add_argument("--sidra-normalize", action="store_true", help="Normalize discovered SIDRA CSV into standard population CSV and seed")
    args = parser.parse_args()
    ensure_dirs()
    if args.ibge:
        fetch_ibge_municipalities()
    if args.ibge_pop:
        try:
            fetch_ibge_population()
        except Exception as e:
            print("IBGE projections failed with:", e)
            print("You can retry later or try SIDRA with --sidra-pop")
    if args.sidra_pop:
        try:
            fetch_sidra_population()
        except Exception as e:
            print("SIDRA population fetch failed with:", e)
    if args.sidra_discover:
        try:
            discover_and_fetch_sidra_population()
        except Exception as e:
            print("SIDRA discovery failed:", e)
    if args.sidra_bruteforce:
        try:
            brute_force_sidra_search()
        except Exception as e:
            print("SIDRA brute-force failed:", e)
    if args.sidra_quick:
        out = quick_sidra_search()
        if out is None:
            print("Quick search did not find municipality table; creating seed fallback")
            create_population_seed_from_parquet()
        else:
            print("Quick search saved:", out)
    if args.sidra_normalize:
        try:
            sidra_normalize()
        except Exception as e:
            print('SIDRA normalize failed:', e)
    if args.sidra_normalize:
        try:
            normalize_sidra_csv_to_population()
        except Exception as e:
            print('Normalization failed:', e)


if __name__ == "__main__":
    main()
