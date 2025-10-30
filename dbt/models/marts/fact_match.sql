SELECT * 
FROM {{ ref('stg_fact_match') }}


/* This was recommended by Gemini AI */
{% if is_incremental() %}
    WHERE match_id NOT IN (SELECT match_id FROM {{ this }})

{% endif %}