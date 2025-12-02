# connection_test.py
import psycopg2
import time
import sys

# ⚠️ REPLACE WITH YOUR RENDER DATABASE URL ⚠️
DATABASE_URL = "postgresql://eas_project_database_user:7c9eN3hXoz2FW0w7A9TDtyrdkYhqPCsx@dpg-d4m9mlpr0fns73a1l40g-a.oregon-postgres.render.com/eas_project_database" 

def test_connection(url):
    print("Attempting to connect to Render PostgreSQL...")
    start_time = time.time()
    
    try:
        # Use connect_timeout to prevent the script from hanging forever
        conn = psycopg2.connect(url, connect_timeout=15) 
        
        # If the connection succeeds, run a trivial query
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            result = cur.fetchone()[0]
        
        conn.close()
        end_time = time.time()
        
        print(f"\n✅ SUCCESS! Connection successful in {end_time - start_time:.2f} seconds.")
        print(f"Database response to 'SELECT 1;': {result}")
        return True

    except psycopg2.OperationalError as e:
        end_time = time.time()
        print(f"\n❌ FAILURE: Connection timed out or was refused.")
        print(f"Error details: {e}")
        print(f"Time elapsed: {end_time - start_time:.2f} seconds.")
        if "timeout" in str(e).lower():
            print("\nAction: This is a network/firewall issue. Check your Render settings.")
        elif "authentication" in str(e).lower():
            print("\nAction: This is a bad credentials issue. Double-check your username/password in the URL.")
        return False
    
    except Exception as e:
        print(f"\n❌ UNKNOWN ERROR: {e}")
        return False

if __name__ == "__main__":
    if not test_connection(DATABASE_URL):
        sys.exit(1)