WITH top_10_countries_with_confirmed_cases AS (
    SELECT 
        country_region,
        SUM(new_confirmed_cases) as total_confirmed_cases
    FROM {{ref('global_covid_data_staging')}}
    GROUP BY country_region
    ORDER BY total_confirmed_cases DESC
    LIMIT 10
)
SELECT * FROM top_10_countries_with_confirmed_cases