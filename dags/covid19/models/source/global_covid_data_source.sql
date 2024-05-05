WITH initial_source AS (
    SELECT 
        DISTINCT
        fips,
        admin2,
        province_state,
        country_region,
        last_update,
        CAST(last_update AS DATE) as last_update_at,
        lat as latitude,
        long_ as longitude,
        confirmed as confirmed_cases,
        deaths as death_cases,
        recovered as recovered_cases,
        active as active_cases,
        combined_key,
        incident_rate,
        case_fatality_ratio,
        file_date
    FROM raw.global_daily_reports_raw
)
SELECT *
FROM initial_source