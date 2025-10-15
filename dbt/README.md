DBT for data-vulnerabilidade

This folder contains dbt models and seeds used in the data-vulnerabilidade project.

Quick start (local Postgres):

1. Start Postgres (Docker Compose):
   - from project root run: docker\docker-compose.yml up -d postgres
2. Ensure `profiles.yml` is configured. An example is provided at `profiles.example.yml`.
3. Run dbt seed to load seeds into the dev database:
   - dbt seed --profiles-dir . --project-dir .
4. Run dbt models:
   - dbt run --profiles-dir . --project-dir .

Notes:
- The seed `ibge_population_seed.csv` is a synthetic fallback used when IBGE/SIDRA population fetch fails.
- To use this with the provided Docker Compose Postgres service, ensure the host in `profiles.yml` is `postgres` and credentials match the compose file.
