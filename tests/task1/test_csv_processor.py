import csv
import os
import unittest

from csv_processor import extract_unique_values, count_by_columns, top_n_values, values_in_date_range


class TestCsvProcessor(unittest.TestCase):

    def test_empty_csv_file(self):
        current_dir = os.path.dirname(__file__)
        empty_csv = os.path.join(current_dir, "../../resources/task1/empty.csv")
        with self.assertRaises(ValueError) as context:
            extract_unique_values(empty_csv, "ColumnName")
        self.assertTrue(f"The CSV file '{empty_csv}' is empty." in str(context.exception))

    def test_invalid_csv_file(self):
        current_dir = os.path.dirname(__file__)
        invalid_csv = os.path.join(current_dir, "../../resources/task1/invalid_file.txt")
        with self.assertRaises(ValueError) as context:
            extract_unique_values(invalid_csv, "ColumnName")
        self.assertTrue(f"The file '{invalid_csv}' is not a valid CSV file." in str(context.exception))

    def test_column_not_found(self):
        current_dir = os.path.dirname(__file__)
        test_csv = os.path.join(current_dir, "../../resources/task1/whitespaces.csv")
        with self.assertRaises(ValueError) as context:
            extract_unique_values(test_csv, "NonExistentColumn")
        self.assertTrue(
            "The specified column 'NonExistentColumn' does not exist in the CSV file." in str(context.exception))

    def test_empty_or_null_column_name(self):
        current_dir = os.path.dirname(__file__)
        test_csv = os.path.join(current_dir, "../../resources/task1/whitespaces.csv")
        with self.assertRaises(ValueError) as context:
            extract_unique_values(test_csv, "")
        self.assertTrue("The target column name cannot be empty or null." in str(context.exception))
        with self.assertRaises(ValueError) as context:
            extract_unique_values(test_csv, None)
        self.assertTrue("The target column name cannot be empty or null." in str(context.exception))

    def test_normalized_values(self):
        current_dir = os.path.dirname(__file__)
        breeds_csv = os.path.join(current_dir, "../../resources/task1/whitespaces.csv")
        values = extract_unique_values(breeds_csv, "Breed", lowercase=True)
        expected_values = ["bichonfrise", "chihuahua", "labmix"]
        self.assertCountEqual(expected_values, values)

    def test_values_are_lowercased(self):
        current_dir = os.path.dirname(__file__)
        breeds_csv = os.path.join(current_dir, "../../resources/task1/lowercase.csv")
        values = extract_unique_values(breeds_csv, "Breed", lowercase=True)
        expected_values = ["42!bichonfrise"]
        self.assertCountEqual(expected_values, values)

    def test_extract_all_columns(self):
        current_dir = os.path.dirname(__file__)
        test_csv = os.path.join(current_dir, "../../resources/task1/all_columns.csv")

        with open(test_csv, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            expected_values = {
                'Breed': ['chihuahua', 'bichonfrise', 'goldenretriever'],
                'HasMicrochip': ['false', 'true'],
                'Weight': ['15.5', '30.0', '5.2'],
                'Height': ['24', '10', '7'],
                'DateFormat1': ['03/23/2017', '12/15/2016', '01/12/2020'],
                'DateFormat2': ['2017-03-2314:32', '2020-01-1211:45', '2016-12-1509:58']
            }
            for column in reader.fieldnames:
                unique_values = extract_unique_values(test_csv, column, lowercase=True)
                self.assertCountEqual(expected_values[column], unique_values)

    def test_extract_values_without_normalization(self):
        current_dir = os.path.dirname(__file__)
        test_csv = os.path.join(current_dir, "../../resources/task1/whitespaces.csv")
        unique_values = extract_unique_values(test_csv, "Breed", normalize=False)
        expected_values = ["BICHON FRISE", " CHIHUAHUA", " LAB   MIX"]
        self.assertCountEqual(expected_values, unique_values)

    def test_count_by_columns(self):
        current_dir = os.path.dirname(__file__)
        test_csv = os.path.join(current_dir, "../../resources/task1/test_data.csv")
        breed_license_data = count_by_columns(test_csv, ['Breed', 'LicenseType'], lowercase=True)
        expected_data = {
            ('bichonfrise', 'dogindividualspayedfemale'): 3,
            ('dachshund', 'dogindividualmale'): 5,
            ('gershepherd', 'dogindividualspayedfemale'): 1,
            ('schnoodle', 'dogindividualspayedfemale'): 1
        }
        self.assertEqual(expected_data, breed_license_data)

    def test_top_n_values(self):
        current_dir = os.path.dirname(__file__)
        test_csv = os.path.join(current_dir, "../../resources/task1/test_data.csv")
        top_values = top_n_values(test_csv, 'Breed', n=3)
        expected_top_values = [('DACHSHUND', 5), ('BICHONFRISE', 3), ('GERSHEPHERD', 1)]
        self.assertEqual(expected_top_values, top_values)

    def test_values_in_date_range(self):
        current_dir = os.path.dirname(__file__)
        test_csv = os.path.join(current_dir, "../../resources/task1/test_data.csv")
        date_column = 'ValidDate'
        target_columns = ['DogName', 'Breed', 'LicenseType']
        start_date = "1/4/2017"
        end_date = "2/4/2017"

        expected_results = [{'Breed': 'SCHNOODLE',
                             'DogName': 'RILEY',
                             'LicenseType': 'Dog Individual Spayed Female',
                             'ValidDate': '1/4/2017 8:39'}]

        results = values_in_date_range(test_csv, date_column, start_date, end_date, target_columns)
        self.assertEqual(expected_results, results)


if __name__ == "__main__":
    unittest.main()
