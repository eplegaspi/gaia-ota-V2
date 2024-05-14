WITH initial_source AS (
    SELECT 
        DISTINCT
        province_state,
        country_region,
        last_update,
        lat as latitude,
        long_ as longitude,
        confirmed as confirmed_deaths,
        deaths,
        recovered as recovered_cases,
        active as active_cases,
        fips,
        incident_rate,
        total_test_results,
        case_fatality_ratio,
        uid,
        iso3,
        testing_rate,
        date,
        file_date
    FROM raw.us_daily_reports_raw
)
SELECT *
FROM initial_source