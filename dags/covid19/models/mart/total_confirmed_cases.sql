WITH total_confirmed_cases AS (
    SELECT 
        SUM(new_confirmed_cases) as total_confirmed_cases
    FROM {{ref('global_covid_data_staging')}}
)
SELECT * FROM total_confirmed_cases