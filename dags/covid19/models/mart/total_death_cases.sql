WITH total_death_cases AS (
    SELECT 
        SUM(new_death_cases) as total_death_cases
    FROM {{ref('global_covid_data_staging')}}
)
SELECT * FROM total_death_cases