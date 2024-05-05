WITH daily_reports AS (
    SELECT
        fips,
        admin2,
        province_state,
        country_region,
        last_update,
        last_update_at,
        latitude,
        longitude,
       
        combined_key,
        incident_rate,
        case_fatality_ratio,
        file_date,

        confirmed_cases,
        death_cases,
        active_cases,
        recovered_cases,
        GREATEST(confirmed_cases - LAG(confirmed_cases) OVER (PARTITION BY combined_key ORDER BY last_update_at), 0) AS new_confirmed_cases,
        GREATEST(death_cases - LAG(death_cases) OVER (PARTITION BY combined_key ORDER BY last_update_at), 0) AS new_death_cases,
        GREATEST(active_cases - LAG(active_cases) OVER (PARTITION BY combined_key ORDER BY last_update_at), 0) AS new_active_cases,
        GREATEST(recovered_cases - LAG(recovered_cases) OVER (PARTITION BY combined_key ORDER BY last_update_at), 0) AS new_recovered_cases
    FROM
        {{ref('global_covid_data_source')}}
)
SELECT * FROM daily_reports
    
    
    