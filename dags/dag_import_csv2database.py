from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import pandas as pd
import os

MSSQL_CONN_ID = 'mssql_conn_id'
CSV_PATH = '/opt/airflow/files/plandaten.csv'  # Adjust path as needed
SCHEMA_NAME = 'iw20s82105'  # Update with your schema name
TABLE_NAME = 'Plandaten_ETL_test'  # Update with your table name

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
        dag_id='plandaten_etl_pipeline',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False
) as dag:

    @task()
    def extract_plandaten_data():
        """Extract data from CSV file."""
        try:
            # Read CSV with specific delimiter and encoding
            df = pd.read_csv(
                CSV_PATH,
                sep=';',
                encoding='utf-8'
            )
            return df.to_dict('records')
        except Exception as e:
            raise Exception(f"Failed to read CSV file: {str(e)}")

    @task()
    def transform_plandaten_data(raw_data):
        """Transform the extracted data."""
        try:
            # Convert to proper data types
            transformed_data = []
            for row in raw_data:
                transformed_row = {
                    'Mon_ID': str(row['Mon_ID']),
                    'Land_ID': str(row['Land_ID']),
                    'Produkt_ID': str(row['Produkt_ID']),
                    'Umsatzplan': float(row['Umsatzplan'])
                }
                transformed_data.append(transformed_row)
            return transformed_data
        except Exception as e:
            raise Exception(f"Failed to transform data: {str(e)}")

    @task()
    def load_plandaten_data(transformed_data):
        """Load transformed data into MSSQL."""
        mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)

        # Create table if it doesn't exist
        create_table_sql = f"""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[{SCHEMA_NAME}].[{TABLE_NAME}]') AND type in (N'U'))
        BEGIN
            CREATE TABLE [{SCHEMA_NAME}].[{TABLE_NAME}] (
                [Mon_ID] VARCHAR(6),
                [Land_ID] VARCHAR(2),
                [Produkt_ID] VARCHAR(4),
                [Umsatzplan] DECIMAL(10,2),
                [load_timestamp] DATETIME DEFAULT GETDATE()
            )
        END
        """

        try:
            # Create table
            mssql_hook.run(create_table_sql)

            # Insert data
            insert_sql = f"""
            INSERT INTO [{SCHEMA_NAME}].[{TABLE_NAME}] 
            ([Mon_ID], [Land_ID], [Produkt_ID], [Umsatzplan])
            VALUES (?, ?, ?, ?)
            """

            # Prepare batch of records
            records = [
                (row['Mon_ID'], row['Land_ID'], row['Produkt_ID'], row['Umsatzplan'])
                for row in transformed_data
            ]

            # Execute batch insert
            conn = mssql_hook.get_conn()
            cursor = conn.cursor()
            cursor.executemany(insert_sql, records)
            conn.commit()
            cursor.close()
            conn.close()

        except Exception as e:
            raise Exception(f"Failed to load data into MSSQL: {str(e)}")

    # Define the DAG workflow
    extracted_data = extract_plandaten_data()
    transformed_data = transform_plandaten_data(extracted_data)
    load_plandaten_data(transformed_data)

if __name__ == "__main__":
    dag.test()