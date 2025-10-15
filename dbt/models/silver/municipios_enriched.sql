-- Silver: Enriched municipalities with joined socio-economic indicators
select
  m.*,
  coalesce(e.population,0) as population
from {{ ref('stg_ibge_municipios') }} m
left join {{ ref('stg_economic_indicators') }} e on m.cod_ibge = e.cod_ibge
