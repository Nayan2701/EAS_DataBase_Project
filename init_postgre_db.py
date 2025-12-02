import psycopg2
import pandas as pd
import json
import sqlite3 
from sqlalchemy import create_engine
from urllib.parse import urlparse
from sqlalchemy import text

DATABASE_URL = "postgresql://eas_project_database_user:7c9eN3hXoz2FW0w7A9TDtyrdkYhqPCsx@dpg-d4m9mlpr0fns73a1l40g-a.oregon-postgres.render.com/eas_project_database"

SQLITE_DB = '/Users/nayanpaliwal/Downloads/mini-projecthash2/normalized.db'
POSTGRES_DUMP_FILE = '/Users/nayanpaliwal/Downloads/mini-projecthash2/postgres_data_dump.json'

# --- PostgreSQL Schema Definitions ---
CREATE_SQL = {
    'Region': """CREATE TABLE Region (RegionID INTEGER PRIMARY KEY, Region TEXT NOT NULL);""",
    'Country': """CREATE TABLE Country (CountryID INTEGER PRIMARY KEY, Country TEXT NOT NULL, RegionID INTEGER REFERENCES Region (RegionID));""",
    'Customer': """CREATE TABLE Customer (CustomerID INTEGER PRIMARY KEY, FirstName TEXT NOT NULL, LastName TEXT NOT NULL, Address TEXT NOT NULL, City TEXT NOT NULL, CountryID INTEGER REFERENCES Country (CountryID));""",
    'ProductCategory': """CREATE TABLE ProductCategory (ProductCategoryID INTEGER PRIMARY KEY, ProductCategory TEXT NOT NULL, ProductCategoryDescription TEXT NOT NULL);""",
    'Product': """CREATE TABLE Product (ProductID INTEGER PRIMARY KEY, ProductName TEXT NOT NULL, ProductUnitPrice REAL NOT NULL, ProductCategoryID INTEGER REFERENCES ProductCategory (ProductCategoryID));""",
    'OrderDetail': """CREATE TABLE OrderDetail (OrderID INTEGER PRIMARY KEY, CustomerID INTEGER REFERENCES Customer (CustomerID), ProductID INTEGER REFERENCES Product (ProductID), OrderDate DATE NOT NULL, QuantityOrdered INTEGER NOT NULL);"""
}

def export_data_from_sqlite(db_file, dump_file):
    conn = sqlite3.connect(db_file)
    tables = list(CREATE_SQL.keys())
    all_data = {}
    for table_name in tables:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        # SQLite INTEGER PRIMARY KEY maps to rowid/PK, which we must export for FK mapping
        all_data[table_name] = df.to_dict('records') 
    conn.close()
    
    with open(dump_file, 'w') as f:
        json.dump(all_data, f)
    print("✅ Data dumped from SQLite to JSON.")

def create_and_populate_postgres(url, dump_file):
    
    #  Prepare SQLAlchemy Engine 
    # Pandas requires the SQLAlchemy engine for correct PostgreSQL dialect handling
    sqlalchemy_url = url.replace("postgresql://", "postgresql+psycopg2://")
    engine = create_engine(sqlalchemy_url) 

    try:
        #Preparing Data
        with open(dump_file, 'r') as f:
            all_data = json.load(f)

        tables_in_order = list(CREATE_SQL.keys())

        # Using the Engine to manage the database connection and transaction 
        with engine.connect() as conn:
            # 1. DROP and CREATE TABLES (Executed within a single transaction)
            
            # Since Pandas will try to create the tables if they don't exist, 
            # we primarily need to ensure they are dropped first using the raw connection.
            
            for table_name in reversed(tables_in_order): 
                print(f"Dropping table: {table_name}")
                conn.execute(text(f"DROP TABLE IF EXISTS \"{table_name}\" CASCADE;"))

            #INSERT DATA (The most reliable step)
            for table_name in tables_in_order:
                df = pd.DataFrame(all_data[table_name])
                
                print(f"Inserting {len(df)} rows into {table_name} via SQLAlchemy...")
                
                # df.to_sql is executed against the SQLAlchemy engine
                df.to_sql(
                    table_name.lower(), 
                    conn, 
                    if_exists='replace',
                    index=False
                )
            
            #COMMIT:  This ensures the transaction is finalized
            conn.commit() 
            print(" Database creation and insertion successfully committed!")

    except Exception as e:
        print(f"FATAL MIGRATION ERROR: {e}")
      
        
    print(" Database initialization on Render complete!")

if __name__ == "__main__":
    #Exporting data from local SQLite
    export_data_from_sqlite(SQLITE_DB, POSTGRES_DUMP_FILE)
    
    #Uploading and initializing Render DB
    create_and_populate_postgres(DATABASE_URL, POSTGRES_DUMP_FILE)