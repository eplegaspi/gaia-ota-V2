WITH initial_source AS (
    SELECT 
        fips,
        admin2,
        province_state,
        country_region,
        last_update,
        lat as latitude,
        long_ as longitude,
        confirmed as confirmed_deaths,
        deaths,
        recovered as recovered_cases,
        active as active_cases,
        combined_key,
        incident_rate,
        case_fatality_ratio
    FROM raw.global_daily_reports_raw
)
SELECT *
FROM initial_source