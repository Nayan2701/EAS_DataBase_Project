# run_etl.py
from mini_project2 import *

DATA_FILE = '/Users/nayanpaliwal/Downloads/mini-projecthash2/tests/data.csv'  # Assuming your data file is named this
DB_FILE = '/Users/nayanpaliwal/Downloads/mini-projecthash2/normalized.db'

def run_all_etl_steps():
    print("--- Starting ETL and Database Normalization ---")
    
    conn = create_connection(DB_FILE, delete_db=True) 
    if conn: conn.close()
        
    print("Step 1: Creating Region Table...")
    step1_create_region_table(DATA_FILE, DB_FILE)
    
    print("Step 3: Creating Country Table...")
    step3_create_country_table(DATA_FILE, DB_FILE)

    print("Step 5: Creating Customer Table...")
    step5_create_customer_table(DATA_FILE, DB_FILE)
    
    print("Step 7: Creating ProductCategory Table...")
    step7_create_productcategory_table(DATA_FILE, DB_FILE)
    
    print("Step 9: Creating Product Table...")
    step9_create_product_table(DATA_FILE, DB_FILE)
    
    print("Step 11: Creating OrderDetail Table (Final Step)...")
    step11_create_orderdetail_table(DATA_FILE, DB_FILE)
    
    print("--- ETL Complete. Database populated. ---")

if __name__ == "__main__":
    run_all_etl_steps()
