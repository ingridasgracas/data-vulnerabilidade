# data-vulnerabilidade

Framework de análise de vulnerabilidade populacional.

Objetivo
- Fornecer um framework modular e reprodutível para integrar dados públicos (IBGE, DATASUS, Cadastro Único, INEP, UNICEF), pré-processar, modelar com dbt em camadas (bronze/silver/gold), e expor visualizações via Streamlit ou Power BI.

Estrutura do projeto
- `src/` - scripts Python de extração, pré-processamento e cálculo de índices
- `dbt/` - projeto dbt com modelos bronze/silver/gold, testes e documentação
- `notebooks/` - notebooks exploratórios e demonstrativos
- `docs/` - documentação técnica e explicação dos indicadores
- `docker/` - docker-compose e Dockerfiles para orquestração (Postgres, dbt, streamlit, airflow opcional)

Principais comandos
- Rodar infraestrutura local (Postgres + serviços):

```powershell
cd docker
docker-compose up -d
```

- Rodar dbt (no container dbt runner ou local):

```powershell
dbt deps
dbt seed
dbt run
dbt test
```

- Rodar pipeline de extração localmente:

```powershell
python src/extract.py --config config/example.env.yaml
python src/preprocess.py --input data/raw --output data/processed
python src/indices.py --input data/processed --output data/indices
```


