import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="Vulnerability Explorer")

st.title("Vulnerability Explorer")

data_path = Path("data/indices/indices.csv")
if data_path.exists():
    df = pd.read_csv(data_path)
    st.dataframe(df.head())
    st.line_chart(df.select_dtypes('number').iloc[:, :5])
else:
    st.warning("Run the ETL and indices calculation first. File not found: data/indices/indices.csv")
