"""
This is the entrypoint to the program. 'python main.py' will be executed and the 
expected csv file should exist in ../data/destination/ after the execution is complete.
"""
import csv
from src.some_storage_library import SomeStorageLibrary


def transform_and_dump_data():
    """Read and sort column headers from SOURCECOLUMNS.txt, read data
    from SOURCEDATA.txt, and output the sorted header row and the data
    into a CSV file in a comma-separated format.

    :return: None
    """
    column_order = {}
    with open('data/source/SOURCECOLUMNS.txt', 'r') as source_columns:
        source_columns_reader = csv.reader(source_columns, delimiter='|')
        for column in source_columns_reader:
            column_order[int(column[0])] = column[1]
    column_order = [column_order[key] for key in sorted(column_order)]

    with open('output.csv', 'w') as output:
        output_writer = csv.writer(output, delimiter=',')
        output_writer.writerow(column_order)
        with open('data/source/SOURCEDATA.txt', 'r') as source_data:
            source_data_reader = csv.reader(source_data, delimiter='|')
            for line in source_data_reader:
                output_writer.writerow(line)

    storage_library = SomeStorageLibrary()
    storage_library.load_csv('output.csv')


if __name__ == '__main__':
    """Entrypoint"""
    print('Beginning the ETL process...')
    transform_and_dump_data()
