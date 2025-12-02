import streamlit as st
import pandas as pd
import psycopg2
import os
import sys
import re
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from google import genai
from google.genai.errors import APIError 
from sqlalchemy import create_engine 
from src.mini_project2 import ex4, ex7, ex9 

RENDER_DB_URL = os.environ.get("RENDER_DB_URL", "postgresql://eas_project_database_user:7c9eN3hXoz2FW0w7A9TDtyrdkYhqPCsx@dpg-d4m9mlpr0fns73a1l40g-a.oregon-postgres.render.com/eas_project_database")

GEMINI_API_KEY = "AIzaSyAquheHuFl2C8W1iEYCLx3BTuQMRpGv5Tk"

SQL_SCHEMA_PATH = "sql_schema.txt" 
CORRECT_PASSWORD = "Nayan" 

# Database Connection Function 
# Using st.cache_resource for persistent connection pooling
@st.cache_resource
def get_db_connection():
    try:
        # We need SQLAlchemy engine for streamlined connection handling
        engine = create_engine(RENDER_DB_URL, connect_args={'connect_timeout': 5})
        conn = engine.raw_connection()
        return conn
    except Exception as e:
        st.error(f"Database connection error. Check RENDER_DB_URL and network: {e}")
        return None

# Custom Password Protection
def login_form():
    if st.session_state.get("logged_in"):
        return True

    st.sidebar.title("Secure Login")
    password = st.sidebar.text_input("Enter Password", type="password")
    
    if st.sidebar.button("Login"):
        if password == CORRECT_PASSWORD:
            st.session_state["logged_in"] = True
            st.rerun()
        else:
            st.sidebar.error("Incorrect password.")
            
    return st.session_state.get("logged_in", False)

#LLM/Gemini Integration 
def get_sql_from_llm(question, schema):
    if not GEMINI_API_KEY:
        st.error("Gemini API Key not configured.")
        return None
        
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
    except Exception as e:
        st.error(f"Error initializing Gemini client: {e}")
        return None
    
    system_prompt = (
        "You are a PostgreSQL expert. Convert the user's question into a single, executable SQL query. "
        "IMPORTANT DATABASE STRUCTURE:\n"
        "1. TABLE NAMES are all LOWERCASE (e.g., \"customer\", \"orderdetail\", \"product\").\n"
        "2. COLUMN NAMES are Case-Sensitive (e.g., \"CustomerID\", \"ProductUnitPrice\", \"FirstName\").\n"
        "3. You MUST use double quotes for ALL table names and column names.\n\n"
        "CRITICAL SQL RULES:\n"
        "1. Output ONLY the SQL query. No markdown.\n"
        "2. POSTGRES ROUNDING FIX: You CANNOT use ROUND(float, 2). You MUST cast the value to NUMERIC first.\n"
        "   - Incorrect: ROUND(SUM(col), 2)\n"
        "   - Correct:   ROUND(CAST(SUM(col) AS NUMERIC), 2)\n"
        f"--- SCHEMA CONTEXT ---\n{schema}\n--- END SCHEMA ---"
    )
    
    try:
        response = client.models.generate_content(
            model='gemini-2.0-flash', 
            contents=[
                {"role": "user", "parts": [{"text": system_prompt + "\n\nUSER QUESTION: " + question}]}
            ],
            config={"temperature": 0.0}
        )
        
        text = response.text.strip()
        
        match = re.search(r"```(?:sql)?\n?(.*?)```", text, re.DOTALL)
        if match:
            sql = match.group(1).strip()
        else:
            lines = text.split('\n')
            sql_lines = [line for line in lines if not line.lower().startswith(("calculate", "here", "note"))]
            sql = "\n".join(sql_lines).strip()
            
        return sql
        
    except APIError as e:
        st.error(f"Gemini API Error (Check Quota/Key): {e}")
        return None
    except Exception as e:
        st.error(f"General Error during API call: {e}")
        return None

def run_app():
    st.set_page_config(layout="wide", page_title="Data Analysis Demo")
    
    if not login_form():
        st.info("Please log in on the sidebar to access the application.")
        return

    conn = get_db_connection()
    if not conn:
        return

    # Loading schema for LLM context
    try:
        with open(SQL_SCHEMA_PATH, 'r') as f:
            schema = f.read()
    except FileNotFoundError:
        st.error(f"Schema file not found at {SQL_SCHEMA_PATH}. Cannot run LLM queries.")
        return

    col1, col2 = st.columns(2)

    with col1:
        st.header("1. 🤖 LLM-to-SQL Interface (Demo Point 5)")
        st.write("Ask a question about the customer or order data.")
        
        user_question = st.text_input(
            "Natural Language Query", 
            "Show the top 5 customers by total sales, listing their full name and total amount."
        )
        
        if st.button("Generate & Run SQL", key="llm_run"):
            with st.spinner("Generating SQL via Gemini..."):
                sql_query = get_sql_from_llm(user_question, schema)
            
            if sql_query:
                st.code(sql_query, language='sql')
                
                try:
                    # Using pandas read_sql with the raw connection
                    df = pd.read_sql(sql_query, conn)
                    st.subheader("Query Results:")
                    st.dataframe(df)
                        
                except Exception as e:
                    st.error(f"Error executing generated SQL. The query may be invalid.")
                    st.code(sql_query, language='sql')
                    st.exception(e) 

    with col2:
        st.header("2. Predefined Queries (Demo Proof)")
        st.write("Run the complex queries designed for the assignment.")
        
        query_choice = st.selectbox(
            "Select Predefined Query to Run:",
            options=["Ex4: Region Totals", "Ex7: Top Country per Region", "Ex9: Top 5 Customers per Q/Y"]
        )

        if st.button("Run Predefined Query", key="predef_run"):
            # Map choice to function
            func_map = {
                "Ex4: Region Totals": ex4,
                "Ex7: Top Country per Region": ex7,
                "Ex9: Top 5 Customers per Q/Y": ex9,
            }
            
            sql_func = func_map[query_choice]
            sql_ex = sql_func(None) 
            
            st.code(sql_ex, language='sql')
            
            try:
                df_ex = pd.read_sql(sql_ex, conn)
                st.subheader(f"Results ({query_choice}):")
                st.dataframe(df_ex)
            except Exception as e:
                st.error(f"Error executing predefined SQL for {query_choice}. Check SQL dialect.")
                st.exception(e)


if __name__ == "__main__":
    run_app()