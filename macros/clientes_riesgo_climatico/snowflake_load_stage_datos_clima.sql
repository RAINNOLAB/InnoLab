{% macro snowflake_load_stage_datos_clima() %}

 
        COPY INTO DATASEGUROCLIMA.DBT_SEGUROCLIMA.DATOS_METEOROLOGICOS_RAW
        (
        estacion, provincia, temperatura_media, temperatura_max, temperatura_min,
        racha_km, velocidad_km, precipitacion1, precipitacion2, precipitacion3,
        precipitacion4, precipitacion5, mes, anio, fecha, fuente, file_name, load_ts
        )
        FROM (
        SELECT
            $1::string,       -- estacion
            $2::string,       -- provincia
            $3::float,        -- temperatura_media
            $4::float,        -- temperatura_max
            $5::float,        -- temperatura_min
            $6::float,        -- racha_km
            $7::float,        -- velocidad_km
            $8::float,        -- precipitacion1
            $9::float,        -- precipitacion2
            $10::float,       -- precipitacion3
            $11::float,       -- precipitacion4
            $12::float,       -- precipitacion5
            $13::int,         -- mes
            $14::int,         -- anio
            TO_DATE($15, 'DD-MM-YYYY'),  -- fecha (ajusta formato)
            $16::string,      -- fuente
            METADATA$FILENAME::string AS file_name,
            CURRENT_TIMESTAMP() AS load_ts
        FROM @DATOS_METEOROLOGICOS_STAGE (FILE_FORMAT => 'MY_CSV_FORMAT')
        )
        PATTERN = '.*\\.csv'
        ON_ERROR = 'CONTINUE'
        FORCE = FALSE;

 
{% endmacro %}