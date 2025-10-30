{{ config(materialized='view') }}

SELECT 
    halfMD5(toString(teamId)) AS team_key,
    CAST(teamId AS Int32) AS team_id,
    NULLIF(TRIM(name), '') AS team_name,
    NULLIF(TRIM(location), '') AS team_location

FROM {{ source('raw_data', 'bronze_teams') }}