import psycopg2
import pandas as pd

class PostgresConnector:
    def __init__(self, user, password, host, port, database):
        self.connection = self.connect_to_postgresql(user, password, host, port, database)

    @staticmethod
    def connect_to_postgresql(user, password, host, port, database):
        try:
            connection = psycopg2.connect(
                user=user,
                password=password,
                host=host,
                port=port,
                database=database
            )
            return connection
        except psycopg2.Error as e:
            print(f"Error connecting to PostgreSQL: {e}")
            return None

    @staticmethod
    def batch_generator(df, batch_size):
        num_batches = len(df) // batch_size + (1 if len(df) % batch_size > 0 else 0)
        total_batches = num_batches
        print("Total number of batches:", total_batches)
        for i in range(num_batches):
            yield df.iloc[i * batch_size: (i + 1) * batch_size], i + 1, total_batches

    def insert_us_data(self, df, batch_size, file_date):
        if not self.connection:
            print("No connection to PostgreSQL.")
            return

        with self.connection.cursor() as cursor:
            for batch_df, batch_num, total_batches in self.batch_generator(df, batch_size):
                batch_data = []
                for _, row in batch_df.iterrows():
                    row_data = (
                        row.get('Province_State', None),
                        row.get('Country_Region', None),
                        row.get('Last_Update', None),
                        row.get('Lat', None),
                        row.get('Long_', None),
                        row.get('Confirmed', None),
                        row.get('Deaths', None),
                        row.get('Recovered', None),
                        row.get('Active', None),
                        row.get('FIPS', None),
                        row.get('Incident_Rate', None),
                        row.get('Total_Test_Results', None),
                        row.get('People_Hospitalized', None),
                        row.get('Case_Fatality_Ratio', None),
                        row.get('UID', None),
                        row.get('ISO3', None),
                        row.get('Testing_Rate', None),
                        row.get('Hospitalization_Rate', None),
                        row.get('Date', None),
                        row.get('People_Tested', None),
                        row.get('Mortality_Rate', None),
                        file_date
                    )
                    row_data = tuple(None if pd.isna(value) else value for value in row_data)
                    batch_data.append(row_data)

                stmt = """
                        INSERT INTO raw.us_daily_reports_raw
                        (Province_State, Country_Region, Last_Update, Lat, Long_, Confirmed, Deaths, Recovered, Active, FIPS,
                        Incident_Rate, Total_Test_Results, People_Hospitalized, Case_Fatality_Ratio, UID, ISO3, Testing_Rate,
                        Hospitalization_Rate, Date, People_Tested, Mortality_Rate, file_date)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(stmt, batch_data)
                self.connection.commit()
                print(f"Inserted batch {batch_num}/{total_batches} into the database.")

    def insert_global_data(self, df, batch_size, file_date):
        if not self.connection:
            print("No connection to PostgreSQL.")
            return

        with self.connection.cursor() as cursor:
            for batch_df, batch_num, total_batches in self.batch_generator(df, batch_size):
                batch_data = []
                for _, row in batch_df.iterrows():
                    row_data = (
                        row.get('FIPS', None),
                        row.get('Admin2', None),
                        row.get('Province_State', None),
                        row.get('Country_Region', None),
                        row.get('Last_Update', None),
                        row.get('Lat', None),
                        row.get('Long_', None),
                        row.get('Confirmed', None),
                        row.get('Deaths', None),
                        row.get('Recovered', None),
                        row.get('Active', None),
                        row.get('Combined_Key', None),
                        row.get('Incident_Rate', None),
                        row.get('Case_Fatality_Ratio', None),
                        file_date
                    )
                    row_data = tuple(None if pd.isna(value) else value for value in row_data)
                    batch_data.append(row_data)

                stmt = """
                        INSERT INTO raw.global_daily_reports_raw
                        (FIPS, Admin2, Province_State, Country_Region, Last_Update, Lat, Long_, Confirmed, Deaths, Recovered, Active, Combined_Key,
                        Incident_Rate, Case_Fatality_Ratio, file_date)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(stmt, batch_data)
                self.connection.commit()
                print(f"Inserted batch {batch_num}/{total_batches} into the database.")
    

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("PostgreSQL connection closed.")