WITH top_10_countries_with_deaths_cases AS (
    SELECT 
        country_region,
        SUM(new_death_cases) as total_death_cases
    FROM {{ref('global_covid_data_staging')}}
    GROUP BY country_region
    ORDER BY total_death_cases DESC
    LIMIT 10
)
SELECT * FROM top_10_countries_with_deaths_cases