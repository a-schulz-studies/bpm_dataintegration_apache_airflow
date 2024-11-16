from sqlalchemy import create_engine
from sqlalchemy.sql import text
import os

if __name__ == '__main__':
    # # Create a connection to the database
    engine = create_engine(f'{os.getenv('MS_SQL_CONNECT_URL')}?driver=ODBC+Driver+18+for+SQL+Server', echo=True, future=True, connect_args={"TrustServerCertificate": "yes"})
    # # Read data from the database
    with engine.connect() as con:
        statement = text("""SELECT * FROM Plandaten""")
        plandaten = con.execute(statement).fetchall()
        print(plandaten)