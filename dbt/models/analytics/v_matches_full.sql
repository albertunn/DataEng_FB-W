{{ config(
    materialized='view',
    schema='analytics'
) }}

SELECT *
FROM {{ ref('fact_match') }}
