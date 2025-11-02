{{ config(
    materialized='incremental',
    unique_key='match_id',
    incremental_strategy='delete+insert'
) }}

SELECT * 
FROM {{ ref('stg_fact_match') }}