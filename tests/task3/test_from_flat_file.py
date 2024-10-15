import unittest
import logging

from sqlalchemy import text, MetaData
from sqlalchemy.orm import sessionmaker

from from_flat_file import insert_with_retry, extract_data, transform_data, load_new_data, get_engine


class TestETLProcessTask3(unittest.TestCase):
    def setUp(self):
        # Create a connection and clean up the database before running each test
        self.engine = get_engine()
        self.connection = self.engine.connect()
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        for table in reversed(self.metadata.sorted_tables):
            self.connection.execute(table.delete())
        logging.getLogger().info("Database cleaned up before running unit test for Task 3.")

    def tearDown(self):
        # Close the connection after each test
        self.connection.close()
        logging.getLogger().info("Database connection closed after unit test for Task 3.")

    def setUp(self):
        # Ensure tests run one by one
        self.engine = self.__class__.engine
        self.connection = self.__class__.connection
        self.metadata = self.__class__.metadata

    def test_employees_table(self):
        # Load test data
        df = extract_data("../resources/task3/employees.csv")
        df = transform_data(df)
        load_new_data(self.engine)

        # Insert transformed data
        employees_data = df.rename(columns={'id': 'emp_id', 'name': 'full_name', 'date_of_birth': 'dob'})
        employees_data.to_sql('employees', con=self.engine, if_exists='append', index=False)

        # Check employees table
        Session = sessionmaker(bind=self.engine)
        session = Session()
        result = session.execute(text("SELECT COUNT(*) FROM employees")).scalar()
        self.assertGreater(result, 0, "Employees table should not be empty after ETL process for Task 3.")
        logging.getLogger().info("Employees table for Task 3 has been populated successfully.")

    def test_departments_table(self):
        # Load test data
        df = extract_data("../resources/task3/employees.csv")
        df = transform_data(df)
        load_new_data(self.engine)

        # Insert transformed data
        unique_departments = df[['department_id']].drop_duplicates().reset_index(drop=True)
        unique_departments.rename(columns={'department_id': 'dept_id'}, inplace=True)
        unique_departments['dept_name'] = unique_departments['dept_id'].apply(lambda x: f'Department {x}')
        insert_with_retry(unique_departments, self.engine)

        # Check departments table
        Session = sessionmaker(bind=self.engine)
        session = Session()
        result = session.execute(text("SELECT COUNT(*) FROM departments")).scalar()
        self.assertGreater(result, 0, "Departments table should not be empty after ETL process for Task 3.")
        logging.getLogger().info("Departments table for Task 3 has been populated successfully.")

    def test_data_validation(self):
        # Load test data
        df = extract_data("../resources/task3/employees.csv")
        df = transform_data(df)
        load_new_data(self.engine)

        # Insert transformed data
        employees_data = df.rename(columns={'id': 'emp_id', 'name': 'full_name', 'date_of_birth': 'dob'})
        employees_data.to_sql('employees', con=self.engine, if_exists='append', index=False)

        unique_departments = df[['department_id']].drop_duplicates().reset_index(drop=True)
        unique_departments.rename(columns={'department_id': 'dept_id'}, inplace=True)
        unique_departments['dept_name'] = unique_departments['dept_id'].apply(lambda x: f'Department {x}')
        insert_with_retry(unique_departments, self.engine)

        # Validate data
        Session = sessionmaker(bind=self.engine)
        session = Session()
        result = session.execute(text("""
            SELECT e.emp_id, e.full_name, e.dob, e.salary, e.department_id, d.dept_name
            FROM employees e
            JOIN departments d ON e.department_id = d.dept_id
        """)).fetchall()
        self.assertEqual(len(result), len(df),
                         "Data validation failed: Mismatch between flat file and database records.")
        logging.getLogger().info("Data validation for Task 3 completed successfully.")


if __name__ == '__main__':
    unittest.main()
