CREATE DATABASE airflow_db;
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
GRANT ALL ON SCHEMA public TO airflow_user;


-- Create the schema
\c covid_data;
CREATE SCHEMA raw;
CREATE SCHEMA source;
CREATE SCHEMA staging;
CREATE SCHEMA prod;


-- Create the table
CREATE TABLE raw.us_daily_reports_raw (
    Province_State VARCHAR(255),
    Country_Region VARCHAR(255),
    Last_Update TIMESTAMP,
    Lat DECIMAL(10, 6),
    Long_ DECIMAL(10, 6),
    Confirmed INT,
    Deaths INT,
    Recovered INT,
    Active INT,
    FIPS INT,
    Incident_Rate DECIMAL(20, 10),
    Total_Test_Results DECIMAL(20, 10),
    People_Hospitalized DECIMAL(20, 10),
    Case_Fatality_Ratio DECIMAL(20, 10),
    UID BIGINT,
    ISO3 VARCHAR(3),
    Testing_Rate DECIMAL(20, 10),
    Hospitalization_Rate DECIMAL(20, 10),
    Date DATE,
    People_Tested DECIMAL(20, 10),
    Mortality_Rate DECIMAL(20, 10)
);

CREATE TABLE raw.global_daily_reports_raw (
    FIPS VARCHAR(255),
    Admin2 VARCHAR(255),
    Province_State VARCHAR(255),
    Country_Region VARCHAR(255),
    Last_Update TIMESTAMP,
    Lat DECIMAL(10, 6),
    Long_ DECIMAL(10, 6),
    Confirmed INT,
    Deaths INT,
    Recovered INT,
    Active INT,
    Combined_Key VARCHAR(255),
    Incident_Rate DECIMAL(20, 10),
    Case_Fatality_Ratio DECIMAL(20, 10)
);