{{ config(
    materialized='incremental',
    unique_key=['estacion','fecha'],
    pre_hook="""
        COPY INTO DATASEGUROCLIMA.DBT_SEGUROCLIMA.STG_RAW_AEMET_CLIMA
        FROM (
            SELECT
                $1::string AS estacion,
                $2::string AS provincia,
                $3::float AS temperatura_media,
                $4::float AS temperatura_max,
                $5::float AS temperatura_min,
                $6::float AS racha_km,
                $7::float AS velocidad_km,
                $8::float AS precipitacion1,
                $9::float AS precipitacion2,
                $10::float AS precipitacion3,
                $11::float AS precipitacion4,
                $12::float AS precipitacion5,
                $13::int AS anio,
                $14::int AS mes,
                TO_DATE($15, 'YYYY-MM-DD') AS fecha,
                $16::string AS fuente,
                METADATA$FILENAME AS file_name,
                CURRENT_TIMESTAMP() AS load_ts
            FROM @DATOS_METEOROLOGICOS_STAGE (FILE_FORMAT => 'MY_CSV_FORMAT')
        )
        PATTERN = '.*\\.csv'
        ON_ERROR = 'CONTINUE'
        FORCE = FALSE;
    """,
    
    post_hook="""
        DELETE FROM DATASEGUROCLIMA.DBT_SEGUROCLIMA.STG_RAW_AEMET_CLIMA
        WHERE (estacion, fecha, load_ts) NOT IN (
            SELECT estacion, fecha, MAX(load_ts)
            FROM DATASEGUROCLIMA.DBT_SEGUROCLIMA.STG_RAW_AEMET_CLIMA
            GROUP BY estacion, fecha
        );
    """
) }}


WITH base AS (
    SELECT
        estacion,
        provincia,
        temperatura_media,
        temperatura_max,
        temperatura_min,
        racha_km,
        velocidad_km,
        precipitacion1,
        precipitacion2,
        precipitacion3,
        precipitacion4,
        precipitacion5,
        anio,
        mes,
        fecha,
        fuente,
        file_name,
        load_ts,
        ROW_NUMBER() OVER (PARTITION BY estacion, fecha ORDER BY load_ts DESC) AS rn
    FROM {{ source('raw_bi_riesgo_climatico','STG_RAW_AEMET_CLIMA') }}
    {% if is_incremental() %}
        WHERE load_ts > (SELECT COALESCE(MAX(load_ts), '1970-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    estacion, provincia, temperatura_media, temperatura_max, temperatura_min,
    racha_km, velocidad_km, precipitacion1, precipitacion2, precipitacion3,
    precipitacion4, precipitacion5, mes, anio, fecha, fuente, file_name, load_ts
FROM base
QUALIFY rn = 1