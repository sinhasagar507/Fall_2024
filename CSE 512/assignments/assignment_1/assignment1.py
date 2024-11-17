"""
Author: Sagar Rajesh Kumar Sinha 
Date: 2024-09-21

"""
from random import random
import os 
import psycopg2
import random
from datetime import date, timedelta

DATABASE_NAME = 'assignment1'
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD') # Password is user-specific 
SALES_REGION_TABLE = 'sales_region'
LONDON_TABLE = 'london'
SYDNEY_TABLE = 'sydney'
BOSTON_TABLE = 'boston'
SALES_TABLE = 'sales'
SALES_2020_TABLE = 'sales_2020'
SALES_2021_TABLE = 'sales_2021'
SALES_2022_TABLE = 'sales_2022'
REGIONS = ["Boston", "Sydney", "London"]
PRODUCT_NAMES = ["Product_A", "Product_B", "Product_C", "Product_D", "Product_E"]

def create_database():
    """Connect to the PostgreSQL by calling connect_postgres() function
       Create a database named {DATABASE_NAME}
       Close the connection"""
       
    conn = connect_postgres()
    cursor = conn.cursor()
    conn.autocommit=True
    cursor.execute(f'CREATE DATABASE {DATABASE_NAME}')
    cursor.close()
    conn.close()
       
def connect_postgres(dbName=None):
    """Connect to the PostgreSQL using psycopg2 with default database
       Return the connection"""
    
    if dbName is None: # Executed when establishing the connection for the first time using the default 'postgres' database 
        conn = psycopg2.connect(
            user='postgres',
            dbname='postgres',
            host='localhost',
            password=POSTGRES_PASSWORD, 
            port='5433'
        )
    else:
        conn = psycopg2.connect(
            user='postgres',
            dbname=dbName,
            host='localhost',
            password=POSTGRES_PASSWORD, 
            port='5433'
        )

    return conn


def list_partitioning(conn):
    """Function to create partitions of {SALES_REGION_TABLE} based on list of REGIONS.
       Create {SALES_REGION_TABLE} table and its list partition tables {LONDON_TABLE}, {SYDNEY_TABLE}, {BOSTON_TABLE}
       Commit the changes to the database"""
    
    cursor = conn.cursor()

    create_table_sql = """
        CREATE TABLE sales_region (
            id integer,
            amount integer,
            region text
        ) PARTITION BY LIST (region);"""


    cursor.execute(create_table_sql)

    create_partition_tables = f"""
        CREATE TABLE {LONDON_TABLE} PARTITION OF sales_region FOR VALUES IN ('London');
        CREATE TABLE {BOSTON_TABLE} PARTITION OF sales_region FOR VALUES IN ('Boston');
        CREATE TABLE {SYDNEY_TABLE} PARTITION OF sales_region FOR VALUES IN ('Sydney');
        """
    
    cursor.execute(create_partition_tables)

    cursor.close()

def insert_list_data(conn):
    """ Generate 50 rows data for {SALES_REGION_TABLE}
        Execute INSERT statement to add data to the {SALES_REGION_TABLE} table.
        Commit the changes to the database"""
        
    cursor = conn.cursor()
    
    for i in range(1, 51):
        amount = random.randint(100, 1000)
        region = random.choice(REGIONS)

        insert_sales = f"INSERT INTO sales_region (id, amount, region) VALUES ({i}, {amount}, '{region}')"
        cursor.execute(insert_sales)
    
    cursor.close()


def select_list_data(conn):
    """Select data from {SALES_REGION_TABLE}, {BOSTON_TABLE}, {LONDON_TABLE}, {SYDNEY_TABLE} seperately.
       Print each tables' data.
       Commit the changes to the database
    """
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {SALES_REGION_TABLE}")
    sales_region_data = cursor.fetchall()
    
    cursor.execute(f"SELECT * FROM {BOSTON_TABLE}")
    boston_city_data = cursor.fetchall()
   
    cursor.execute(f"SELECT * FROM {LONDON_TABLE}")
    london_city_data = cursor.fetchall()
    
    cursor.execute(f"SELECT * FROM {SYDNEY_TABLE}")
    sydney_city_data = cursor.fetchall()
    
    print("Sales table:")
    for row in sales_region_data:
        print(row)
    print(f"\nBoston data:")
    for row in boston_city_data:
        print(row)
    print(f"\nLondon data:")
    for row in london_city_data:
        print(row)
    print(f"\nSydney data:")
    for row in sydney_city_data:
        print(row)
    

def range_partitioning(conn):
    """Function to create partitions of {SALES_TABLE} based on range of sale_date.
       Create {SALES_REGION_TABLE} table and its range partition tables {SALES_2020_TABLE}, {SALES_2021_TABLE}, {SALES_2022_TABLE}
       Commit the changes to the database
    """
    cursor = conn.cursor()
    create_table = f"""
        CREATE TABLE {SALES_TABLE} (
            id integer,
            product_name text,
            amount integer,
            sale_date date
        ) PARTITION BY RANGE (sale_date);
        """
    cursor.execute(create_table)

    create_partition_tables = f"""
        CREATE TABLE {SALES_2020_TABLE} PARTITION OF sales
        FOR VALUES FROM ('2020-01-01') TO ('2020-12-31');

        CREATE TABLE {SALES_2021_TABLE} PARTITION OF sales
        FOR VALUES FROM ('2021-01-01') TO ('2021-12-31');

        CREATE TABLE {SALES_2022_TABLE} PARTITION OF sales
        FOR VALUES FROM ('2022-01-01') TO ('2022-12-31');
        """
    cursor.execute(create_partition_tables)

    cursor.close()

def insert_range_data(conn):
    """ Generate 50 rows data for {SALES_REGION_TABLE}
        Execute INSERT statement to add data to the {SALES_REGION_TABLE} table.
        Commit the changes to the database"""
        
    cursor = conn.cursor()
    for idx in range(1, 51):
            prod_name = random.choice(PRODUCT_NAMES)
            amount = random.randint(1, 100)
            sale_date = date(2020, 1, 1) + timedelta(days=random.randint(0, 1095))  # Random date between 2020-01-01 and 2022-12-31

            insert_sales = f"INSERT INTO sales (id, product_name, amount, sale_date) VALUES ({idx}, '{prod_name}', {amount}, '{sale_date}')"
            cursor.execute(insert_sales)
    
    cursor.close()


def select_range_data(conn):
    """ Select data from {SALES_TABLE}, {SALES_2020_TABLE}, {SALES_2021_TABLE}, {SALES_2022_TABLE} seperately.
           Print each tables' data.
           Commit the changes to the database
    """
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {SALES_TABLE}")
    sales_data = cursor.fetchall()

    cursor.execute(f"SELECT * FROM {SALES_2020_TABLE}")
    sales_2020_data = cursor.fetchall()

    cursor.execute(f"SELECT * FROM {SALES_2021_TABLE}")
    sales_2021_data = cursor.fetchall()

    cursor.execute(f"SELECT * FROM {SALES_2022_TABLE}")
    sales_2022_data = cursor.fetchall()

    print("\nSales Data:")
    for row in sales_data:
        print(row)
    
    print(f"\nSales 2020 data:")
    for row in sales_2020_data:
        print(row)
        
    print(f"\nSales 2021 data:")
    for row in sales_2021_data:
        print(row)
    
    print(f"\nSales 2022 data:")
    for row in sales_2022_data:
        print(row)

    cursor.close()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    create_database()

    with connect_postgres(DATABASE_NAME) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        list_partitioning(conn)
        insert_list_data(conn)
        select_list_data(conn)
        range_partitioning(conn)
        insert_range_data(conn)
        select_range_data(conn)
        
        print('Done')




