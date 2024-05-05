WITH monthly_cases AS (
    SELECT
        TO_CHAR(last_update, 'YYYY-MM') as month_year,
        SUM(deaths) AS deaths
    FROM {{ref('global_covid_data_source')}}
    GROUP BY month_year
)
SELECT * FROM monthly_cases