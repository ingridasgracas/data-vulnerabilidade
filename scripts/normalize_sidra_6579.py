import requests
import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data" / "raw"
SEEDS = BASE / "data" / "seeds"


def fetch_json():
    url = "https://apisidra.ibge.gov.br/values/t/6579/n6/all/v/all/p/last"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def normalize():
    try:
        data = fetch_json()
    except Exception as e:
        print('Fetch error', e)
        return False

    if not isinstance(data, list) or len(data) < 2:
        print('Unexpected format')
        return False

    header = data[0]
    rows = data[1:]
    try:
        from alive_progress import alive_bar
        use_bar = True
    except Exception:
        use_bar = False

    records = []
    if use_bar:
        with alive_bar(len(rows), title='Building rows') as bar:
            for r in rows:
                records.append(dict(zip(header, r)))
                bar()
    else:
        for r in rows:
            records.append(dict(zip(header, r)))
    df = pd.DataFrame(records)

    # Robust heuristics to find columns:
    # - municipio_id: column where most values are digits and have length >= 6 (IBGE codes are 7 digits)
    # - populacao: column where most values are numeric (int/float)
    # - municipio: textual column with many distinct strings
    def is_int_string(s):
        try:
            if s is None:
                return False
            s = str(s).strip()
            return s.isdigit()
        except Exception:
            return False

    def is_float_string(s):
        try:
            if s is None:
                return False
            float(str(s).replace(',', '.'))
            return True
        except Exception:
            return False

    cols = df.columns.tolist()
    scores = {}
    n = len(df)
    for c in cols:
        col = df[c].astype(str).fillna("")
        int_count = col.map(lambda x: x.isdigit()).sum()
        float_count = col.map(lambda x: is_float_string(x)).sum()
        nonempty = (col != "").sum()
        avg_len = col.map(len).mean() if n>0 else 0
        distinct = col.nunique()
        scores[c] = {
            'int_count': int_count,
            'float_count': float_count,
            'nonempty': nonempty,
            'avg_len': avg_len,
            'distinct': distinct
        }

    # choose municipio_id candidate: high int_count proportion and avg length >=6
    code_col = None
    for c, s in scores.items():
        if s['int_count'] >= 0.8 * n and s['avg_len'] >= 6:
            code_col = c
            break
    # fallback: highest int_count
    if code_col is None:
        code_col = max(cols, key=lambda c: scores[c]['int_count'])

    # choose populacao: highest float_count (excluding code_col)
    val_candidates = [c for c in cols if c != code_col]
    value_col = max(val_candidates, key=lambda c: scores[c]['float_count'])

    # choose municipio: highest distinct textual values (exclude numeric columns)
    name_candidates = [c for c in cols if c not in (code_col, value_col)]
    def textual_score(c):
        s = scores[c]
        return s['distinct'] * (s['avg_len']+1)
    if name_candidates:
        name_col = max(name_candidates, key=textual_score)
    else:
        name_col = None

    print('Detected columns:', code_col, name_col, value_col)

    # Build output
    try:
        out = df[[code_col, name_col, value_col]].copy()
    except Exception as e:
        print('Error selecting columns:', e)
        return False
    out.columns = ['municipio_id', 'municipio', 'populacao']

    # Clean municipio_id
    def clean_int(x):
        try:
            s = str(x).strip()
            # remove non-digits
            s2 = ''.join(ch for ch in s if ch.isdigit())
            return int(s2) if s2!='' else None
        except Exception:
            return None

    out['municipio_id'] = out['municipio_id'].map(clean_int)
    # Clean populacao into float
    def clean_float(x):
        try:
            s = str(x).strip().replace('.', '').replace(',', '.')
            return float(s)
        except Exception:
            return None

    out['populacao'] = out['populacao'].map(clean_float)

    # Drop rows without municipio_id
    out = out.dropna(subset=['municipio_id'])
    out['municipio_id'] = out['municipio_id'].astype(int)

    SEEDS.mkdir(parents=True, exist_ok=True)
    RAW.mkdir(parents=True, exist_ok=True)
    out.to_csv(RAW / 'ibge_population.csv', index=False)
    out.to_csv(SEEDS / 'ibge_population_seed.csv', index=False)
    print('Saved normalized population to data/raw/ibge_population.csv and data/seeds/ibge_population_seed.csv')
    print(out.head().to_dict())
    return True


if __name__ == '__main__':
    normalize()
