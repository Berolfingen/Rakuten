import csv
import os
from collections import Counter
from datetime import datetime
from typing import List, Set, Dict, Tuple

import pandas as pd


def _validate_csv_and_columns(csv_file: str, required_columns: List[str]) -> None:
    """
        Validate the CSV file and check for required columns.

        This function ensures that the specified CSV file exists, has the correct file extension,
        and contains all the required columns. It raises a ValueError for any validation issues.

        Parameters:
        -----------
        csv_file : str
            The path to the CSV file to be validated.
        required_columns : List[str]
            A list of required column names that must be present in the CSV file.

        Raises:
        -------
        ValueError
            If the file is not a valid CSV, the required columns are not present,
            or if the required column names are empty or invalid.
        FileNotFoundError
            If the specified CSV file does not exist.
        """
    if not csv_file.lower().endswith('.csv'):
        raise ValueError(f"The file '{csv_file}' is not a valid CSV file.")

    try:
        with open(csv_file, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            if not reader.fieldnames:
                raise ValueError(f"The CSV file '{csv_file}' is empty.")

            for column in required_columns:
                if not column or column.strip() == "":
                    raise ValueError("The target column name cannot be empty or null.")
                if column not in reader.fieldnames:
                    raise ValueError(f"The specified column '{column}' does not exist in the CSV file.")
    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{csv_file}' does not exist.")


def extract_unique_values(csv_file: str, target_column: str, normalize: bool = True, lowercase: bool = False) -> List[
    str]:
    """
        Extract unique values from a target column in a CSV file.

        This function reads a CSV file and extracts unique values from the specified target column.
        It optionally normalizes and converts the values to lowercase based on the parameters.

        Parameters:
        -----------
        csv_file : str
            The path to the CSV file to be processed.
        target_column : str
            The column from which unique values will be extracted.
        normalize : bool, optional
            If True, removes spaces from the values before adding them to the unique set (default is True).
        lowercase : bool, optional
            If True, converts the values to lowercase before adding them to the unique set (default is False).

        Returns:
        --------
        List[str]
            A sorted list of unique values extracted from the target column.

        Raises:
        -------
        ValueError
            If the file is not a valid CSV, the required column is not present, or if there is an issue
            with reading the CSV content.
        FileNotFoundError
            If the specified CSV file does not exist.

        Notes:
        ------
        - The returned list is sorted alphabetically.
        """
    _validate_csv_and_columns(csv_file, [target_column])

    unique_values: Set[str] = set()

    try:
        with open(csv_file, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                value = row.get(target_column, "")
                if value:
                    if normalize:
                        value = value.replace(" ", "")
                    if lowercase:
                        value = value.lower()
                    unique_values.add(value)

        return sorted(unique_values)
    except csv.Error:
        raise ValueError(f"The file '{csv_file}' is not a valid CSV file.")


def extract_unique_values_pandas(csv_file: str, target_column: str, normalize: bool = True, lowercase: bool = False) -> \
        List[str]:
    """
        Extract unique values from a target column in a CSV file using pandas.

        This function reads a CSV file using pandas and extracts unique values from the specified target column.
        It optionally normalizes and converts the values to lowercase based on the parameters.

        Parameters:
        -----------
        csv_file : str
            The path to the CSV file to be processed.
        target_column : str
            The column from which unique values will be extracted.
        normalize : bool, optional
            If True, removes spaces from the values before adding them to the unique set (default is True).
        lowercase : bool, optional
            If True, converts the values to lowercase before adding them to the unique set (default is False).

        Returns:
        --------
        List[str]
            A sorted list of unique values extracted from the target column.

        Raises:
        -------
        ValueError
            If the file is not a valid CSV or if there is an issue with reading the CSV content.
        FileNotFoundError
            If the specified CSV file does not exist.

        Notes:
        ------
        - The returned list is sorted alphabetically.
        """
    _validate_csv_and_columns(csv_file, [target_column])

    try:
        df = pd.read_csv(csv_file)
        unique_values = df[target_column].dropna().astype(str).str.strip()

        if normalize:
            unique_values = unique_values.str.replace(" ", "", regex=False)
        if lowercase:
            unique_values = unique_values.str.lower()

        return sorted(unique_values.unique())
    except pd.errors.ParserError:
        raise ValueError(f"The file '{csv_file}' is not a valid CSV file.")


def count_by_columns(csv_file: str, group_by_columns: List[str], normalize: bool = True, lowercase: bool = False) -> \
        Dict[Tuple[str, ...], int]:
    """
        Count occurrences of unique combinations of values across specified columns in a CSV file.

        This function reads a CSV file and counts how often unique combinations of values appear
        across the specified columns. It optionally normalizes and converts the values to lowercase.

        Parameters:
        -----------
        csv_file : str
            The path to the CSV file to be processed.
        group_by_columns : List[str]
            The list of column names used for grouping and counting unique combinations.
        normalize : bool, optional
            If True, removes spaces from the values before grouping (default is True).
        lowercase : bool, optional
            If True, converts the values to lowercase before grouping (default is False).

        Returns:
        --------
        Dict[Tuple[str, ...], int]
            A dictionary where keys are tuples representing unique combinations of column values,
            and values are the count of occurrences for each combination. The dictionary is sorted
            in descending order of counts.

        Raises:
        -------
        ValueError
            If the file is not a valid CSV or if there is an issue with reading the CSV content.
        FileNotFoundError
            If the specified CSV file does not exist.

        Notes:
        ------
        - The counts are returned in descending order of frequency.
        """
    _validate_csv_and_columns(csv_file, group_by_columns)

    count_data = Counter()
    try:
        with open(csv_file, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                key = tuple(
                    row[column].replace(" ", "").lower() if lowercase else row[column].replace(" ", "")
                    if normalize else row[column]
                    for column in group_by_columns
                )
                count_data[key] += 1

        return dict(sorted(count_data.items(), key=lambda item: item[1], reverse=True))
    except csv.Error:
        raise ValueError(f"The file '{csv_file}' is not a valid CSV file.")


def count_by_columns_pandas(csv_file: str, group_by_columns: List[str], normalize: bool = True,
                            lowercase: bool = False) -> \
        Dict[Tuple[str, ...], int]:
    """
        Count occurrences of unique combinations of values across specified columns in a CSV file using pandas.

        This function reads a CSV file using pandas and counts how often unique combinations of values appear
        across the specified columns. It optionally normalizes and converts the values to lowercase.

        Parameters:
        -----------
        csv_file : str
            The path to the CSV file to be processed.
        group_by_columns : List[str]
            The list of column names used for grouping and counting unique combinations.
        normalize : bool, optional
            If True, removes spaces from the values before grouping (default is True).
        lowercase : bool, optional
            If True, converts the values to lowercase before grouping (default is False).

        Returns:
        --------
        Dict[Tuple[str, ...], int]
            A dictionary where keys are tuples representing unique combinations of column values,
            and values are the count of occurrences for each combination. The dictionary is sorted
            in descending order of counts.

        Raises:
        -------
        ValueError
            If the file is not a valid CSV or if there is an issue with reading the CSV content.
        FileNotFoundError
            If the specified CSV file does not exist.

        Notes:
        ------
        - The counts are returned in descending order of frequency.
        """
    _validate_csv_and_columns(csv_file, group_by_columns)

    try:
        df = pd.read_csv(csv_file)
        for column in group_by_columns:
            df[column] = df[column].astype(str).str.strip()
            if normalize:
                df[column] = df[column].str.replace(" ", "", regex=False)
            if lowercase:
                df[column] = df[column].str.lower()

        count_data = df.groupby(group_by_columns).size().reset_index(name='count')
        sorted_count_data = count_data.sort_values(by='count', ascending=False)
        return {tuple(row[group_by_columns]): row['count'] for _, row in sorted_count_data.iterrows()}
    except pd.errors.ParserError:
        raise ValueError(f"The file '{csv_file}' is not a valid CSV file.")


def top_n_values(csv_file: str, target_column: str, n: int = 5, normalize: bool = True, lowercase: bool = False) -> \
        List[Tuple[str, int]]:
    """
        Extract the top N most common values from a target column in a CSV file.

        This function reads a CSV file and extracts the most common values from the specified target column.
        It returns the values along with their respective counts, sorted in descending order of frequency.
        It optionally normalizes and converts the values to lowercase based on the parameters.

        Parameters:
        -----------
        csv_file : str
            The path to the CSV file to be processed.
        target_column : str
            The column from which the top N most common values will be extracted.
        n : int, optional
            The number of most common values to return (default is 5).
        normalize : bool, optional
            If True, removes spaces from the values before counting (default is True).
        lowercase : bool, optional
            If True, converts the values to lowercase before counting (default is False).

        Returns:
        --------
        List[Tuple[str, int]]
            A list of tuples where each tuple contains a unique value from the target column and its count.
            The list is sorted in descending order of frequency.

        Raises:
        -------
        ValueError
            If the file is not a valid CSV or if there is an issue with reading the CSV content.
        FileNotFoundError
            If the specified CSV file does not exist.
        """
    _validate_csv_and_columns(csv_file, [target_column])
    try:
        count_data = count_by_columns(csv_file, [target_column], normalize, lowercase)
        value_counter = Counter({k[0]: v for k, v in count_data.items()})  # Extract the first element of each tuple key
        top_n = value_counter.most_common(n)
        return top_n
    except csv.Error:
        raise ValueError(f"The file '{csv_file}' is not a valid CSV file.")


def top_n_values_pandas(csv_file: str, target_column: str, n: int = 5, normalize: bool = True,
                        lowercase: bool = False) -> List[Tuple[str, int]]:
    """
        Extract the top N most common values from a target column in a CSV file using pandas.

        This function reads a CSV file using pandas and extracts the most common values from the specified
        target column. It returns the values along with their respective counts, sorted in descending order
        of frequency. It optionally normalizes and converts the values to lowercase based on the parameters.

        Parameters:
        -----------
        csv_file : str
            The path to the CSV file to be processed.
        target_column : str
            The column from which the top N most common values will be extracted.
        n : int, optional
            The number of most common values to return (default is 5).
        normalize : bool, optional
            If True, removes spaces from the values before counting (default is True).
        lowercase : bool, optional
            If True, converts the values to lowercase before counting (default is False).

        Returns:
        --------
        List[Tuple[str, int]]
            A list of tuples where each tuple contains a unique value from the target column and its count.
            The list is sorted in descending order of frequency.

        Raises:
        -------
        ValueError
            If the file is not a valid CSV or if there is an issue with reading the CSV content.
        FileNotFoundError
            If the specified CSV file does not exist.
        """
    _validate_csv_and_columns(csv_file, [target_column])

    try:
        df = pd.read_csv(csv_file)
        df[target_column] = df[target_column].astype(str).str.strip()
        if normalize:
            df[target_column] = df[target_column].str.replace(" ", "", regex=False)
        if lowercase:
            df[target_column] = df[target_column].str.lower()

        count_data = df[target_column].value_counts().reset_index()
        count_data.columns = [target_column, 'count']
        top_n = count_data.head(n)
        return list(top_n.itertuples(index=False, name=None))
    except pd.errors.ParserError:
        raise ValueError(f"The file '{csv_file}' is not a valid CSV file.")


def values_in_date_range(csv_file, date_column, start_date, end_date, target_columns):
    """
        Extract rows from a CSV file where the date falls within a specified range.

        This function reads a CSV file and extracts rows where the value in the specified date column falls
        within the given start and end date range. The extracted rows include only the target columns specified.

        Parameters:
        -----------
        csv_file : str
            The path to the CSV file to be processed.
        date_column : str
            The name of the column containing the date values.
        start_date : str
            The start date for the range in the format "%m/%d/%Y".
        end_date : str
            The end date for the range in the format "%m/%d/%Y".
        target_columns : List[str]
            The list of additional columns to extract along with the date column.

        Returns:
        --------
        List[Dict[str, str]]
            A list of dictionaries where each dictionary represents a row from the CSV file that falls
            within the specified date range. Each dictionary contains the date column and the target columns.

        Raises:
        -------
        ValueError
            If the file is not a valid CSV, if the date format is invalid, or if there is an issue with
            reading the CSV content.
        FileNotFoundError
            If the specified CSV file does not exist.

        Notes:
        ------
        - The date column values are expected in the format "%m/%d/%Y %H:%M".
        - The start and end dates are inclusive.
        """
    _validate_csv_and_columns(csv_file, [date_column] + target_columns)
    try:
        results_in_range = []

        with open(csv_file, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            start = datetime.strptime(start_date, "%m/%d/%Y")
            end = datetime.strptime(end_date, "%m/%d/%Y")

            for row in reader:
                date_value = row[date_column]
                if date_value:
                    try:
                        date_obj = datetime.strptime(date_value, "%m/%d/%Y %H:%M")
                        if start <= date_obj <= end:
                            result = {col: row[col] for col in target_columns}
                            result[date_column] = date_value
                            results_in_range.append(result)
                    except ValueError:
                        raise ValueError(f"Invalid date format in column '{date_column}': {date_value}")

        return results_in_range
    except csv.Error:
        raise ValueError(f"The file '{csv_file}' is not a valid CSV file.")


if __name__ == "__main__":
    current_dir = os.path.dirname(__file__)
    csv_file = os.path.join(current_dir, "../resources/task1/2017.csv")

    unique_breeds = extract_unique_values(csv_file, "Breed", normalize=True, lowercase=True)
    print("Unique breeds:", unique_breeds)

    unique_breeds_pandas = extract_unique_values_pandas(csv_file, "Breed", normalize=True, lowercase=True)
    print("Unique breeds using pandas:", unique_breeds_pandas)

    breed_license_counts = count_by_columns(csv_file, ["Breed", "LicenseType"], normalize=True, lowercase=True)
    print("Number of licenses by LicenseType for each unique breed:")
    for breed_license, count in breed_license_counts.items():
        print(f"{breed_license}: {count}")

    breed_license_counts_pandas = count_by_columns_pandas(csv_file, ["Breed", "LicenseType"], normalize=True,
                                                          lowercase=True)
    print("Number of licenses by LicenseType for each unique breed using pandas:")
    for breed_license, count in breed_license_counts_pandas.items():
        print(f"{breed_license}: {count}")

    top_dog_names = top_n_values(csv_file, "DogName", n=5, normalize=True, lowercase=False)
    print("Top 5 popular dog names:")
    for name, count in top_dog_names:
        print(f"{name}: {count}")

    top_dog_names_pandas = top_n_values(csv_file, "DogName", n=5, normalize=True, lowercase=False)
    print("Top 5 popular dog names using pandas:")
    for name, count in top_dog_names_pandas:
        print(f"{name}: {count}")

    start_date = "12/25/2016"
    end_date = "12/31/2016"
    date_column = "ValidDate"
    target_columns = ["DogName", "Breed", "LicenseType"]
    licenses_in_range = values_in_date_range(csv_file, date_column, start_date, end_date, target_columns)

    print("Details of licenses issued between {0} and {1}:".format(start_date, end_date))
    for license_info in licenses_in_range:
        print(license_info)
