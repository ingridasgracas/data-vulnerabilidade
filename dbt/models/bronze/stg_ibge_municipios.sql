-- Bronze: Stage IBGE municipalities (loaded from raw files)
select
  municipio_id,
  municipio,
  uf,
  cod_ibge
from {{ source('raw', 'ibge_municipios') }}
