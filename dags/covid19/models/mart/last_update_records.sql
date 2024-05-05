WITH last_update_records AS (
    SELECT 
        country_region,
        confirmed_cases,
        death_cases,
        active_cases,
        recovered_cases
    FROM {{ref('global_covid_data_staging')}}
    WHERE last_update_at = (
            SELECT max(last_update_at)
            FROM {{ref('global_covid_data_staging')}}
        )
    ORDER BY death_cases DESC
    LIMIT 10
)
SELECT * FROM last_update_records