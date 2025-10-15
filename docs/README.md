# Data Vulnerabilidade - Documentação

Arquitetura
- Extração: `src/extract.py` (APIs e CSVs)
- Pré-processamento: `src/preprocess.py` -> `data/processed`
- Cálculo de índices: `src/indices.py`
- Modelagem analítica: `dbt/` (bronze, silver, gold)
- Visualização: `streamlit_app.py` ou Power BI conectando ao Postgres/BigQuery

Indicadores
- Social: composição de variáveis de composição familiar, acesso a serviços básicos, renda per capita.
- Econômico: taxa de desemprego, rendimento médio, participação formal.
- Educacional: taxa de alfabetização, índice de escolaridade, desempenho por rede (INEP).
- Territorial: densidade populacional, acesso a serviços de saúde (leitos por mil), distância a centros.

Extensão
- Adicione novos `stg_` modelos na pasta `dbt/models/bronze` e crie transformações em `silver`.
