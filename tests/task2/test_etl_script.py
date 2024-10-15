import logging
import unittest

from sqlalchemy import MetaData, text
from sqlalchemy.orm import sessionmaker

from etl_script import transform_data, load_data, get_engine, extract_data, insert_with_retry


class TestETLProcess(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create a connection and cleanup the database before running tests
        cls.engine = get_engine()
        cls.connection = cls.engine.connect()
        cls.metadata = MetaData()
        cls.metadata.reflect(bind=cls.engine)
        for table in reversed(cls.metadata.sorted_tables):
            cls.connection.execute(table.delete())
        logging.info("Database cleaned up before running unit tests.")

    @classmethod
    def tearDownClass(cls):
        # Close the connection after all tests are done
        cls.connection.close()
        logging.info("Database connection closed after unit tests.")

    def setUp(self):
        # Ensure tests run one by one
        self.engine = self.__class__.engine
        self.connection = self.__class__.connection
        self.metadata = self.__class__.metadata

    def test_employees_table(self):
        # Load test data
        df = extract_data("../resources/task2/employees.csv")
        df = transform_data(df)
        load_data(self.engine)

        # Insert transformed data
        df.to_sql('employees', con=self.engine, if_exists='append', index=False)

        # Check employees table
        Session = sessionmaker(bind=self.engine)
        session = Session()
        result = session.execute(text("SELECT COUNT(*) FROM employees")).scalar()
        self.assertGreater(result, 0, "Employees table should not be empty after ETL process.")
        logging.info("Employees table has been populated successfully.")

    def test_departments_table(self):
        # Load test data
        df = extract_data("../resources/task2/employees.csv")
        df = transform_data(df)
        load_data(self.engine)

        # Insert transformed data
        unique_departments = df[['department_id']].drop_duplicates().reset_index(drop=True)
        unique_departments['department_name'] = unique_departments['department_id'].apply(lambda x: f'Department {x}')
        insert_with_retry(unique_departments, self.engine)

        # Check departments table
        Session = sessionmaker(bind=self.engine)
        session = Session()
        result = session.execute(text("SELECT COUNT(*) FROM departments")).scalar()
        self.assertGreater(result, 0, "Departments table should not be empty after ETL process.")
        logging.info("Departments table has been populated successfully.")

if __name__ == '__main__':
    unittest.main()
