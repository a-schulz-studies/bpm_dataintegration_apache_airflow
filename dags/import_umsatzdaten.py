import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import pandas as pd
import os
import sqlalchemy as sa
from sqlalchemy import text

MSSQL_CONN_ID = 'mssql_conn_id'
CSV_PATH = '/opt/airflow/files/Belege_2021_1.csv'  # Adjust path as needed
TABLE_NAME = 'Umsatzdaten_ETL'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

logger = logging.getLogger(__name__)
logger.info("This is a log message")

with DAG(
        dag_id='umsatzdaten_etl_pipeline',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False
) as dag:
    #
    @task()
    def extract_data():
        """Extract data from CSV file."""
        try:
            # Read CSV with specific delimiter and encoding
            df = pd.read_csv(
                CSV_PATH,
                sep=',',
                encoding='utf-8',
                dtype=str,
                nrows=100
            )
            df.Preis = df.Preis.astype(float)
            df.Anzahl = df.Anzahl.astype(int)

            return df.to_dict('records')
        except Exception as e:
            raise Exception(f"Failed to read CSV file: {str(e)}")

    def fetch_lookups():
        """
        Fetches lookup dictionaries from the database.

        :return: Tuple of (produktsubkategorie_lookup, produkt_lookup)
        """
        produktsubkategorie_lookup = {}
        produkt_lookup = {}

        mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        conn = mssql_hook.get_conn()

        with conn.cursor() as cursor:
            # Fetch produktsubkategorie_lookup
            cursor.execute("SELECT Subkategorie_ID, Mitarbeiter_ID FROM Produktsubkategorie;")
            for subkategorie_id, mitarbeiter_id in cursor.fetchall():
                produktsubkategorie_lookup[subkategorie_id] = mitarbeiter_id

            # Fetch produkt_lookup
            cursor.execute("SELECT Produkt_ID, Subkategorie_ID FROM Produkt;")
            for produkt_id, subkategorie_id in cursor.fetchall():
                produkt_lookup[produkt_id] = subkategorie_id
        conn.close()
        return produktsubkategorie_lookup, produkt_lookup

    @task()
    def transform_data(raw_data):
        """Transform the extracted data."""
        try:
            # Final list
            transformed_data = []

            # Aggregate data for grouping
            aggregated_data = {}
            produktsubkategorie_lookup, produkt_lookup = fetch_lookups()
            for row in raw_data:
                # Extract necessary fields
                mon_id = datetime.strptime(row['Datum'], "%Y-%m-%d").strftime('%Y%m')
                land_id = row['Fil_ID'][:2]
                produkt_id = row['Prod_ID']
                preis = row['Preis']
                anzahl = row['Anzahl']

                # Lookup Mitarbeiter_ID
                subkategorie_id = produkt_lookup.get(produkt_id)
                mitarbeiter_id = produktsubkategorie_lookup.get(subkategorie_id)

                # Create a unique key for aggregation
                key = (mon_id, land_id, produkt_id, mitarbeiter_id)

                # Aggregate Umsatzbetrag and Umsatzmenge
                if key not in aggregated_data:
                    aggregated_data[key] = {'Umsatzbetrag': 0, 'Umsatzmenge': 0}

                aggregated_data[key]['Umsatzbetrag'] += preis * anzahl
                aggregated_data[key]['Umsatzmenge'] += anzahl

            # Transform aggregated data into the final format
            for (mon_id, land_id, produkt_id, mitarbeiter_id), values in aggregated_data.items():
                transformed_row = {
                    'Mon_ID': mon_id,
                    'Land_ID': land_id,
                    'Produkt_ID': produkt_id,
                    'Mitarbeiter_ID': mitarbeiter_id,
                    'Umsatzbetrag': values['Umsatzbetrag'],
                    'Umsatzmenge': values['Umsatzmenge'],
                }
                transformed_data.append(transformed_row)

            return transformed_data
        except Exception as e:
            raise Exception(f"Failed to transform data: {str(e)}")


    @task()
    def load_data(transformed_data):
        """Load transformed data into MSSQL."""
        mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)

        # Create table if it doesn't exist
        create_table_sql = f"""
        if not exists (select * from sys.objects where name='{TABLE_NAME}' and type = 'U')
        CREATE TABLE {TABLE_NAME} (
            Mon_ID         VARCHAR(6),
            Land_ID        VARCHAR(2),
            Produkt_ID     VARCHAR(4),
            Mitarbeiter_ID VARCHAR(2),
            Umsatzbetrag   MONEY,
            Umsatzmenge    INTEGER
        );"""

        try:
            # Create table
            conn = mssql_hook.get_conn()
            cursor = conn.cursor()

            cursor.execute(create_table_sql)

            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            raise Exception(f"Failed to create table: {str(e)}")

        try:
            # Insert data
            insert_sql = f"""
            INSERT INTO {TABLE_NAME}
            ([Mon_ID], [Land_ID], [Produkt_ID], [Mitarbeiter_ID], [Umsatzbetrag], [Umsatzmenge])
            VALUES (%s, %s, %s, %s, %s, %s)
            """

            # Prepare batch of records
            records = [
                (
                    row['Mon_ID'],
                    row['Land_ID'],
                    row['Produkt_ID'],
                    row['Mitarbeiter_ID'],
                    row['Umsatzbetrag'],
                    row['Umsatzmenge']
                )
                for row in transformed_data
            ]

            conn = mssql_hook.get_conn()
            cursor = conn.cursor()

            cursor.executemany(insert_sql, records)

            conn.commit()
            cursor.close()
            conn.close()

        except Exception as e:
            raise Exception(f"Failed to load data into MSSQL: {str(e)}")


    # Define the DAG workflow
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data)

if __name__ == "__main__":
    dag.test()
