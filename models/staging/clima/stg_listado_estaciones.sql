select 
    Id, 
    nombre, 
    municipio, 
    provincia, 
    altura, 
    latitud, 
    longitud
from {{ source('raw_bi_riesgo_climatico','LISTADO_ESTACIONES')}}