import logging
import os
import shutil
from datetime import datetime
import pandas as pd

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

MSSQL_CONN_ID = 'mssql_conn_id'
CSV_INPUT_PATH = '/opt/airflow/files/'
CSV_ARCHIVE_PATH = '/opt/airflow/files/archived/'
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
        catchup=False,
) as dag:

    @task()
    def check_for_files():
        """Check for files in the input directory."""
        files = [f for f in os.listdir(CSV_INPUT_PATH) if (f.startswith('Belege') and f.endswith('.csv'))]
        if not files:
            logger.info("No files found. Pipeline will terminate successfully.")
            return []
        logger.info(f"Found files: {files}")
        return files

    @task()
    def extract_data(filename):
        """Extract data from a CSV file."""
        try:
            file_path = os.path.join(CSV_INPUT_PATH, filename)
            df = pd.read_csv(
                file_path,
                sep=',',
                encoding='utf-8',
                dtype=str
            )
            df['Preis'] = df['Preis'].astype(float)
            df['Anzahl'] = df['Anzahl'].astype(int)
            return df.to_dict('records')
        except Exception as e:
            raise Exception(f"Failed to read CSV file {filename}: {str(e)}")

    def fetch_lookups():
        """Fetches lookup dictionaries from the database."""
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
            transformed_data = []
            aggregated_data = {}
            produktsubkategorie_lookup, produkt_lookup = fetch_lookups()

            for row in raw_data:
                mon_id = datetime.strptime(row['Datum'], "%Y-%m-%d").strftime('%Y%m')
                land_id = row['Fil_ID'][:2]
                produkt_id = row['Prod_ID']
                preis = row['Preis']
                anzahl = row['Anzahl']
                subkategorie_id = produkt_lookup.get(produkt_id)
                mitarbeiter_id = produktsubkategorie_lookup.get(subkategorie_id)
                key = (mon_id, land_id, produkt_id, mitarbeiter_id)

                if key not in aggregated_data:
                    aggregated_data[key] = {'Umsatzbetrag': 0, 'Umsatzmenge': 0}
                aggregated_data[key]['Umsatzbetrag'] += preis * anzahl
                aggregated_data[key]['Umsatzmenge'] += anzahl

            for (mon_id, land_id, produkt_id, mitarbeiter_id), values in aggregated_data.items():
                transformed_data.append({
                    'Mon_ID': mon_id,
                    'Land_ID': land_id,
                    'Produkt_ID': produkt_id,
                    'Mitarbeiter_ID': mitarbeiter_id,
                    'Umsatzbetrag': values['Umsatzbetrag'],
                    'Umsatzmenge': values['Umsatzmenge'],
                })
            return transformed_data
        except Exception as e:
            raise Exception(f"Failed to transform data: {str(e)}")

    @task()
    def load_data(transformed_data):
        """Load transformed data into MSSQL."""
        mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        create_table_sql = f"""
        IF NOT EXISTS (SELECT * FROM sys.objects WHERE name='{TABLE_NAME}' AND type = 'U')
        CREATE TABLE {TABLE_NAME} (
            Mon_ID         VARCHAR(6),
            Land_ID        VARCHAR(2),
            Produkt_ID     VARCHAR(4),
            Mitarbeiter_ID VARCHAR(2),
            Umsatzbetrag   MONEY,
            Umsatzmenge    INTEGER
        );
        """
        insert_sql = f"""
            INSERT INTO {TABLE_NAME}
            ([Mon_ID], [Land_ID], [Produkt_ID], [Mitarbeiter_ID], [Umsatzbetrag], [Umsatzmenge])
            VALUES (%s, %s, %s, %s, %s, %s)
            """
        try:
            conn = mssql_hook.get_conn()
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            cursor.executemany(insert_sql, [
                (row['Mon_ID'], row['Land_ID'], row['Produkt_ID'], row['Mitarbeiter_ID'],
                 row['Umsatzbetrag'], row['Umsatzmenge']) for row in transformed_data
            ])
            conn.commit()
        except Exception as e:
            raise Exception(f"Failed to load data: {str(e)}")
        finally:
            cursor.close()
            conn.close()

    @task()
    def archive_file(filename):
        """Move the processed file to the archive directory."""
        try:
            source_path = os.path.join(CSV_INPUT_PATH, filename)
            dest_path = os.path.join(CSV_ARCHIVE_PATH, filename)
            shutil.move(source_path, dest_path)
            logger.info(f"Archived file: {filename}")
        except Exception as e:
            raise Exception(f"Failed to archive file {filename}: {str(e)}")

    # Define the DAG workflow
    files = check_for_files()
    for file in files:
        raw_data = extract_data(file)
        transformed_data = transform_data(raw_data)
        load_data(transformed_data)
        archive_file(file)

if __name__ == "__main__":
    dag.test()
