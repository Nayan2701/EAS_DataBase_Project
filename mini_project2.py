### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error
import datetime

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def get_data_lines(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        # Skip header row
        lines = f.readlines()[1:]
    return lines

def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    conn=create_connection(normalized_database_filename)
    sql_create= """CREATE TABLE IF NOT EXISTS Region(
      RegionID INTEGER NOT NULL PRIMARY KEY,
      Region TEXT NOT NULL
    );"""
    create_table(conn,sql_create,drop_table_name='Region')
    regions=set()
    lines = get_data_lines(data_filename)
    for line in lines:
      cols=line.strip().split('\t')
      regions.add(cols[4])
    sorted_regions=sorted(list(regions))
    data_insert=[(r,) for r in sorted_regions]

    with conn:
      cur=conn.cursor()
      cur.executemany("INSERT INTO Region (Region) VALUES (?)",data_insert)
    conn.close()
# WRITE YOUR CODE HERE

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    conn=create_connection(normalized_database_filename)
    sql="SELECT Region,RegionID FROM Region"
    rows=execute_sql_statement(sql,conn)
    conn.close()
    return {row[0]:row[1] for row in rows}
    
# WRITE YOUR CODE HERe


def step3_create_country_table(data_filename, normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    
    # Get foreign keys
    region_map = step2_create_region_to_regionid_dictionary(normalized_database_filename)

    
    sql_create = """CREATE TABLE IF NOT EXISTS Country (
                        CountryID INTEGER NOT NULL PRIMARY KEY,
                        Country TEXT NOT NULL,
                        RegionID INTEGER NOT NULL,
                        FOREIGN KEY (RegionID) REFERENCES Region (RegionID)
                    );"""
    create_table(conn, sql_create, drop_table_name='Country')

    # 2. Parse Data
    countries = set()
    lines = get_data_lines(data_filename)
    for line in lines:
        cols = line.strip().split('\t')
        country_name = cols[3]
        region_name = cols[4]
        countries.add((country_name, region_name))

    sorted_countries = sorted(list(countries))

    data_to_insert = []
    for c_name, r_name in sorted_countries:
        r_id = region_map[r_name]
        data_to_insert.append((c_name, r_id))

    with conn:
        cur = conn.cursor()
        cur.executemany("INSERT INTO Country (Country, RegionID) VALUES (?, ?)", data_to_insert)
    conn.close()
# WRITE YOUR CODE HERE


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    conn =create_connection(normalized_database_filename)
    sql = "SELECT Country, CountryID FROM Country"
    rows = execute_sql_statement(sql,conn)
    conn.close()
    return {row[0]:row[1] for row in rows}
    
# WRITE YOUR CODE HERE
        
        
def step5_create_customer_table(data_filename, normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    country_map = step4_create_country_to_countryid_dictionary(normalized_database_filename)

    sql_create = """CREATE TABLE IF NOT EXISTS Customer (
                        CustomerID INTEGER NOT NULL PRIMARY KEY,
                        FirstName TEXT NOT NULL,
                        LastName TEXT NOT NULL,
                        Address TEXT NOT NULL,
                        City TEXT NOT NULL,
                        CountryID INTEGER NOT NULL,
                        FOREIGN KEY (CountryID) REFERENCES Country (CountryID)
                    );"""
    create_table(conn, sql_create, drop_table_name='Customer')

    unique_customers = set()
    lines = get_data_lines(data_filename)
    for line in lines:
        cols = line.strip().split('\t')
        full_name = cols[0]
        address = cols[1]
        city = cols[2]
        country = cols[3]
        unique_customers.add((full_name, address, city, country))

    sorted_customers = sorted(list(unique_customers), key=lambda x: x[0])

    data_to_insert = []
    for full_name, address, city, country in sorted_customers:
        parts = full_name.split()
        first_name = parts[0]
        last_name = " ".join(parts[1:])
        
        country_id = country_map[country]
        data_to_insert.append((first_name, last_name, address, city, country_id))

    with conn:
        cur = conn.cursor()
        cur.executemany("""INSERT INTO Customer (FirstName, LastName, Address, City, CountryID) 
                           VALUES (?, ?, ?, ?, ?)""", data_to_insert)
    conn.close()
# WRITE YOUR CODE HERE


def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    conn= create_connection(normalized_database_filename)
    sql = "SELECT FirstName || ' ' || LastName,CustomerID FROM Customer"
    rows = execute_sql_statement(sql,conn)
    conn.close()
    return {row[0]: row[1] for row in rows}
    
# WRITE YOUR CODE HERE
        
def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    conn= create_connection(normalized_database_filename)
    sql_create="""CREATE TABLE IF NOT EXISTS ProductCategory(
      ProductCategoryID INTEGER NOT NULL PRIMARY KEY,
      ProductCategory TEXT NOT NULL,
      ProductCategoryDescription TEXT NOT NULL
    );"""
    create_table(conn,sql_create,drop_table_name='ProductCategory')
    categories = set()
    lines= get_data_lines(data_filename)
    for line in lines:
      cols = line.strip().split('\t')
      cats = cols[6].split(';')
      descs = cols[7].split(';')

      for c,d in zip(cats,descs):
        categories.add((c,d))

    sorted_cats = sorted(list(categories))

    with conn:
      cur = conn.cursor()
      cur.executemany("INSERT INTO ProductCategory(ProductCategory,ProductCategoryDescription)VALUES(?,?)",sorted_cats)
    conn.close()

    
# WRITE YOUR CODE HERE

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    sql = "SELECT ProductCategory, ProductCategoryID FROM ProductCategory "
    rows = execute_sql_statement(sql,conn)
    conn.close()
    return {row[0]: row[1] for row in rows}
    
# WRITE YOUR CODE HERE
        

def step9_create_product_table(data_filename, normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    cat_map = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)

    sql_create = """CREATE TABLE IF NOT EXISTS Product (
                        ProductID INTEGER NOT NULL PRIMARY KEY,
                        ProductName TEXT NOT NULL,
                        ProductUnitPrice REAL NOT NULL,
                        ProductCategoryID INTEGER NOT NULL,
                        FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory (ProductCategoryID)
                    );"""
    create_table(conn, sql_create, drop_table_name='Product')

    products = set()
    lines = get_data_lines(data_filename)
    for line in lines:
        cols = line.strip().split('\t')
       
        p_names = cols[5].split(';')
        p_cats = cols[6].split(';')
        p_prices = cols[8].split(';')

        for name, cat, price in zip(p_names, p_cats, p_prices):
            products.add((name, float(price), cat))

    sorted_products = sorted(list(products)) 

    data_to_insert = []
    for name, price, cat in sorted_products:
        cat_id = cat_map[cat]
        data_to_insert.append((name, price, cat_id))

    with conn:
        cur = conn.cursor()
        cur.executemany("INSERT INTO Product (ProductName, ProductUnitPrice, ProductCategoryID) VALUES (?, ?, ?)", data_to_insert)
    conn.close()

    
# WRITE YOUR CODE HERE


def step10_create_product_to_productid_dictionary(normalized_database_filename):
  conn=create_connection(normalized_database_filename)
  sql="SELECT ProductName,ProductID FROM Product"
  rows=execute_sql_statement(sql,conn)
  conn.close()
  return {row[0]:row[1] for row in rows}
    
# WRITE YOUR CODE HERE

        

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    
    cust_map = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
    prod_map = step10_create_product_to_productid_dictionary(normalized_database_filename)

    sql_create = """CREATE TABLE IF NOT EXISTS OrderDetail (
                        OrderID INTEGER NOT NULL PRIMARY KEY,
                        CustomerID INTEGER NOT NULL,
                        ProductID INTEGER NOT NULL,
                        OrderDate INTEGER NOT NULL,
                        QuantityOrdered INTEGER NOT NULL,
                        FOREIGN KEY (CustomerID) REFERENCES Customer (CustomerID),
                        FOREIGN KEY (ProductID) REFERENCES Product (ProductID)
                    );"""
    create_table(conn, sql_create, drop_table_name='OrderDetail')

    lines = get_data_lines(data_filename)
    data_to_insert = []
    
    for line in lines:
        cols = line.strip().split('\t')
        full_name = cols[0]
        cust_id = cust_map[full_name]

        p_names = cols[5].split(';')
        qtys = cols[9].split(';')
        dates = cols[10].split(';')

        for p_name, qty, date_str in zip(p_names, qtys, dates):
            prod_id = prod_map[p_name]
            formatted_date = datetime.datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
            
            data_to_insert.append((cust_id, prod_id, formatted_date, int(qty)))

    with conn:
        cur = conn.cursor()
        cur.executemany("""INSERT INTO OrderDetail (CustomerID, ProductID, OrderDate, QuantityOrdered) 
                           VALUES (?, ?, ?, ?)""", data_to_insert)
    conn.close()

    
# WRITE YOUR CODE HERE


def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    sql_statement = f"""
    SELECT 
        c.FirstName || ' ' || c.LastName as Name,
        p.ProductName,
        o.OrderDate,
        p.ProductUnitPrice,
        o.QuantityOrdered,
        ROUND(p.ProductUnitPrice * o.QuantityOrdered, 2) as Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p ON o.ProductID = p.ProductID
    WHERE c.FirstName || ' ' || c.LastName = '{CustomerName}'
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    sql_statement = f"""
    SELECT 
        c.FirstName || ' ' || c.LastName as Name,
        ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered), 2) as Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p ON o.ProductID = p.ProductID
    WHERE c.FirstName || ' ' || c.LastName = '{CustomerName}'
    GROUP BY Name
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    
    sql_statement = """
    SELECT 
        c.FirstName || ' ' || c.LastName as Name,
        ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered), 2) as Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p ON o.ProductID = p.ProductID
    GROUP BY Name
    ORDER BY Total DESC
    """
# WRITE YOUR CODE HERE
    return sql_statement


def ex5(conn):
    # Total for all Countries.
    # We added ', cnt.Country ASC' to the ORDER BY clause.
    # This acts as a tie-breaker so the rows match the CSV exactly.
    sql_statement = """
    SELECT 
        cnt.Country,
        ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered), 2) as Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Country cnt ON c.CountryID = cnt.CountryID
    JOIN Product p ON o.ProductID = p.ProductID
    GROUP BY cnt.Country
    ORDER BY Total DESC, cnt.Country ASC
    """
    return sql_statement

def ex6(conn):
    sql_statement = """
    SELECT 
        r.Region,
        cnt.Country,
        ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered), 2) as CountryTotal,
        RANK() OVER (PARTITION BY r.Region ORDER BY SUM(p.ProductUnitPrice * o.QuantityOrdered) DESC) as CountryRegionalRank
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Country cnt ON c.CountryID = cnt.CountryID
    JOIN Region r ON cnt.RegionID = r.RegionID
    JOIN Product p ON o.ProductID = p.ProductID
    GROUP BY r.Region, cnt.Country
    ORDER BY r.Region ASC, CountryRegionalRank ASC, cnt.Country ASC
    """
    return sql_statement



def ex8(conn):
    sql_statement = """
    SELECT 
        'Q' || CAST((strftime('%m', o.OrderDate) + 2) / 3 AS TEXT) as Quarter,
        CAST(strftime('%Y', o.OrderDate) AS INTEGER) as Year,
        c.CustomerID,
        ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered), 2) as Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p ON o.ProductID = p.ProductID
    GROUP BY Quarter, Year, c.CustomerID
    ORDER BY Year ASC, Quarter ASC, c.CustomerID ASC
    """
    return sql_statement


def ex10(conn):
    sql_statement = """
    SELECT 
        CASE strftime('%m', o.OrderDate)
            WHEN '01' THEN 'January'
            WHEN '02' THEN 'February'
            WHEN '03' THEN 'March'
            WHEN '04' THEN 'April'
            WHEN '05' THEN 'May'
            WHEN '06' THEN 'June'
            WHEN '07' THEN 'July'
            WHEN '08' THEN 'August'
            WHEN '09' THEN 'September'
            WHEN '10' THEN 'October'
            WHEN '11' THEN 'November'
            WHEN '12' THEN 'December'
        END as Month,
        ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered), 2) as Total,
        RANK() OVER (ORDER BY SUM(p.ProductUnitPrice * o.QuantityOrdered) DESC) as TotalRank
    FROM OrderDetail o
    JOIN Product p ON o.ProductID = p.ProductID
    GROUP BY Month
    ORDER BY TotalRank ASC
    """
    return sql_statement

def ex11(conn):
    sql_statement = """
    WITH OrderedDates AS (
        SELECT DISTINCT 
            CustomerID, 
            OrderDate 
        FROM OrderDetail 
    ),
    LaggedDates AS (
        SELECT 
            CustomerID, 
            OrderDate, 
            LAG(OrderDate, 1) OVER (PARTITION BY CustomerID ORDER BY OrderDate) as PreviousOrderDate
        FROM OrderedDates
    )
    SELECT 
        c.CustomerID,
        c.FirstName,
        c.LastName,
        cnt.Country,
        ld.OrderDate,
        ld.PreviousOrderDate,
        (julianday(ld.OrderDate) - julianday(ld.PreviousOrderDate)) as MaxDaysWithoutOrder
    FROM LaggedDates ld
    JOIN Customer c ON ld.CustomerID = c.CustomerID
    JOIN Country cnt ON c.CountryID = cnt.CountryID
    WHERE ld.PreviousOrderDate IS NOT NULL
    ORDER BY MaxDaysWithoutOrder DESC, c.CustomerID ASC
    """
    return sql_statement


def ex4(conn):
    sql_statement = """
    SELECT 
        "r"."Region",
        ROUND(CAST(SUM("p"."ProductUnitPrice" * "o"."QuantityOrdered") AS NUMERIC), 2) as "Total"
    FROM "orderdetail" "o"
    JOIN "customer" "c" ON "o"."CustomerID" = "c"."CustomerID"
    JOIN "country" "cnt" ON "c"."CountryID" = "cnt"."CountryID"
    JOIN "region" "r" ON "cnt"."RegionID" = "r"."RegionID"
    JOIN "product" "p" ON "o"."ProductID" = "p"."ProductID"
    GROUP BY "r"."Region"
    ORDER BY "Total" DESC
    """
    return sql_statement

def ex7(conn):
    sql_statement = """
    WITH RankedCountries AS (
        SELECT 
            "r"."Region",
            "cnt"."Country",
            ROUND(CAST(SUM("p"."ProductUnitPrice" * "o"."QuantityOrdered") AS NUMERIC), 2) as "Total",
            RANK() OVER (PARTITION BY "r"."Region" ORDER BY SUM("p"."ProductUnitPrice" * "o"."QuantityOrdered") DESC) as "TotalRank"
        FROM "orderdetail" "o"
        JOIN "customer" "c" ON "o"."CustomerID" = "c"."CustomerID"
        JOIN "country" "cnt" ON "c"."CountryID" = "cnt"."CountryID"
        JOIN "region" "r" ON "cnt"."RegionID" = "r"."RegionID"
        JOIN "product" "p" ON "o"."ProductID" = "p"."ProductID"
        GROUP BY "r"."Region", "cnt"."Country"
    )
    SELECT "Region", "Country", "Total", "TotalRank"
    FROM RankedCountries
    WHERE "TotalRank" = 1
    ORDER BY "Region" ASC
    """
    return sql_statement

def ex9(conn):
    sql_statement = """
    WITH CustomerSales AS (
        SELECT 
            'Q' || CAST(EXTRACT(QUARTER FROM "o"."OrderDate") AS TEXT) as "Quarter",
            CAST(EXTRACT(YEAR FROM "o"."OrderDate") AS INTEGER) as "Year",
            "c"."CustomerID",
            ROUND(CAST(SUM("p"."ProductUnitPrice" * "o"."QuantityOrdered") AS NUMERIC), 2) as "Total"
        FROM "orderdetail" "o"
        JOIN "customer" "c" ON "o"."CustomerID" = "c"."CustomerID"
        JOIN "product" "p" ON "o"."ProductID" = "p"."ProductID"
        GROUP BY "Quarter", "Year", "c"."CustomerID"
    ),
    RankedSales AS (
        SELECT 
            "Quarter", "Year", "CustomerID", "Total",
            RANK() OVER (PARTITION BY "Quarter", "Year" ORDER BY "Total" DESC) as "CustomerRank"
        FROM CustomerSales
    )
    SELECT "Quarter", "Year", "CustomerID", "Total", "CustomerRank"
    FROM RankedSales
    WHERE "CustomerRank" <= 5
    ORDER BY "Year" ASC, "Quarter" ASC, "CustomerRank" ASC
    """
    return sql_statement