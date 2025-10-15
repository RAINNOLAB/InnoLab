{{ config(
    materialized='incremental',
    unique_key=['CLIENTE_ID'],
    pre_hook="""
        COPY INTO DATASEGUROCLIMA.DBT_SEGUROCLIMA.STG_RAW_CLIENTES
        FROM (
            SELECT
                $1::NUMBER AS CLIENTE_ID,
                $2::STRING AS TIPO_SEGURO,
                $3::STRING AS ZONA,
                $4::NUMBER AS EDAD,
                $5::NUMBER(38,1) AS VALOR_PROPIEDAD,
                $6::NUMBER(38,1) AS VALOR_AUTO,
                $7::NUMBER AS SINIESTRO,
                $8::NUMBER(38,1) AS VALOR_TOTAL,
                $9::STRING AS CIUDAD,
                $10::STRING AS MES,
                $11::NUMBER AS ANYO,
                TO_DATE($12, 'YYYY-MM-DD') AS FECHA,
                CURRENT_TIMESTAMP() AS LOAD_TS
            FROM @DATOS_CLIENTES_STAGE/clientes_con_fecha.csv (FILE_FORMAT => 'my_csv_format')
        )
        ;
    """,
    post_hook="""
        DELETE FROM DATASEGUROCLIMA.DBT_SEGUROCLIMA.STG_RAW_CLIENTES
        WHERE (CLIENTE_ID, LOAD_TS) NOT IN (
            SELECT CLIENTE_ID, MAX(LOAD_TS)
            FROM DATASEGUROCLIMA.DBT_SEGUROCLIMA.STG_RAW_CLIENTES
            GROUP BY CLIENTE_ID
        );
    """
) }}


WITH base AS (
    SELECT
        CLIENTE_ID,
        TIPO_SEGURO, 
        ZONA,
        EDAD,
        VALOR_PROPIEDAD,
        VALOR_AUTO,
        SINIESTRO,
        VALOR_TOTAL,
        CIUDAD,
        MES,
        ANYO,
        FECHA,
        LOAD_TS,
        ROW_NUMBER() OVER (PARTITION BY CLIENTE_ID ORDER BY LOAD_TS DESC) AS rn
    FROM {{ source('raw_bi_riesgo_climatico','STG_RAW_CLIENTES') }}
    {% if is_incremental() %}
        WHERE LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS), '1970-01-01') FROM {{ this }})
    {% endif %}
)

SELECT *
FROM base
QUALIFY rn = 1
