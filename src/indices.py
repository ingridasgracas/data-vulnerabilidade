"""Compute vulnerability indices (social, economic, educational, territorial).

This module provides a simple standard scaler + PCA approach as a placeholder. Replace with domain-specific formulas.
"""
import argparse
from pathlib import Path
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def compute_index(df: pd.DataFrame, features=None):
    """Compute a normalized index from the provided features.

    If features is None, use all numeric columns in df. Handles cases with
    zero or one numeric column by returning a normalized single column or zeros.
    """
    if features is None:
        # choose numeric columns only
        features = df.select_dtypes(include=["number"]).columns.tolist()

    if len(features) == 0:
        # no numeric features: return zeros
        return pd.Series(0.0, index=df.index)

    X = df[features].fillna(0)
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    if Xs.shape[1] == 1:
        # single numeric column: use the scaled column directly
        score = Xs.ravel()
    else:
        pca = PCA(n_components=1)
        score = pca.fit_transform(Xs).ravel()

    # normalize to 0-1
    if score.max() == score.min():
        return pd.Series(0.0, index=df.index)
    return pd.Series((score - score.min()) / (score.max() - score.min()), index=df.index)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/processed/ibge_municipios.parquet")
    parser.add_argument("--output", default="data/indices/indices.csv")
    args = parser.parse_args()
    df = pd.read_parquet(args.input)
    # compute index using numeric columns by default
    df["vuln_overall"] = compute_index(df)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print("Saved indices to", out)


if __name__ == "__main__":
    main()
