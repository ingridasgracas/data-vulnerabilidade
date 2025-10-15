-- Bronze: Stage IBGE population seed
select
  cast(municipio_id as integer) as municipio_id,
  municipio,
  cast(populacao as integer) as populacao
from {{ ref('ibge_population_seed') }}
