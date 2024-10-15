import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, Float, Table, MetaData
import logging
import tempfile
import time
import sqlite3

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Step 1: Extract data from the flat file

# Read the data from the CSV file with better error handling
def extract_data(file_path):
    try:
        df = pd.read_csv(file_path)
        logging.info("Data extraction completed successfully.")
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except pd.errors.EmptyDataError:
        logging.error("The file is empty.")
        raise
    except Exception as e:
        logging.error(f"Error reading the file: {e}")
        raise


# Step 2: Transform the data
def transform_data(df):
    # Remove leading and trailing spaces from column names and string values
    df.columns = df.columns.str.strip()
    string_columns = ['name', 'department_id', 'date_of_birth']
    for col in string_columns:
        df[col] = df[col].astype(str).str.strip()

    # Convert date_of_birth to datetime format with coercion and error handling
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], format='%Y-%m-%d', errors='coerce')
    invalid_dates = df['date_of_birth'].isna().sum()
    if invalid_dates > 0:
        logging.warning(f"{invalid_dates} rows have invalid date_of_birth and will be removed.")
        df = df[df['date_of_birth'].notna()]

    # Convert salary to float with error handling
    df['salary'] = pd.to_numeric(df['salary'], errors='coerce')
    invalid_salaries = df['salary'].isna().sum()
    if invalid_salaries > 0:
        logging.warning(f"{invalid_salaries} rows have invalid salary and will be removed.")
        df = df[df['salary'].notna()]
    return df


# Step 3: Load the data into the new database
def load_new_data(engine):
    metadata = MetaData()

    # Define employees and departments tables with new schema
    employees = Table('employees', metadata,
                      Column('emp_id', Integer, primary_key=True),
                      Column('full_name', String, nullable=False),
                      Column('dob', Date, nullable=False),
                      Column('salary', Float, nullable=False),
                      Column('department_id', String, nullable=False))

    departments = Table('departments', metadata,
                        Column('dept_id', String, primary_key=True),
                        Column('dept_name', String, nullable=False))

    # Create the tables in the database if they do not exist
    metadata.create_all(engine)
    logging.info("New database tables created successfully.")


# Main ETL process for new database
def etl_process_new(engine, file_path):
    df = extract_data(file_path)
    df = transform_data(df)
    load_new_data(engine)
    logging.info("ETL process for new database completed successfully.")


# Database URI
def get_engine():
    temp_db = tempfile.NamedTemporaryFile(delete=False)
    return create_engine(f'sqlite:///{temp_db.name}', connect_args={"timeout": 30})


# Insert with retry logic
def insert_with_retry(data, engine, retries=3, delay=5):
    for attempt in range(retries):
        try:
            with engine.connect() as conn:
                with conn.begin():
                    data.to_sql('departments', con=conn, if_exists='append', index=False)
            return
        except sqlite3.OperationalError as e:
            if 'database is locked' in str(e):
                logging.warning(f"Database is locked, retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise
    raise Exception("Failed to insert data after retries due to locked database.")


# Run ETL for new database
if __name__ == "__main__":
    etl_process_new(get_engine(), "../resources/task3/employees.csv")
