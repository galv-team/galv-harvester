# SPDX-License-Identifier: BSD-2-Clause
# Copyright  (c) 2020-2023, The Chancellor, Masters and Scholars of the University
# of Oxford, and the 'Galv' Developers. All rights reserved.

import csv
from .exceptions import UnsupportedFileTypeError
from .input_file import InputFile


class ArbinCSVFile(InputFile):
    """
        A class for handling Arbin csv files.
        DelimitedFileInput fails to pick these up because it breaks on the space in the datetime rather than on ,
    """

    def __init__(self, file_path, **kwargs):
        """
        Arbin CSV files have a header row and are comma separated.

        :param file_path: the path to the file

        :param kwargs: additional arguments

        :raises UnsupportedFileTypeError: if the file is not a supported type
        """
        with open(file_path, newline='', encoding='utf-8-sig') as csvfile:
            try:
                reader = csv.reader(
                    csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL
                )
                self.header = next(reader)
                assert [h.lower() for h in self.header[0:3]] == ["data_point", "date_time", "test_time(s)"]
                data = next(reader)
                assert len(self.header) == len(data)
                self.dialect = reader.dialect
            except Exception as e:
                raise UnsupportedFileTypeError() from e

        super().__init__(file_path)
        self.logger.info(f"Type is Arbin CSV")

    def load_data(self, file_path, columns):
        column_names = self.header

        with open(file_path, newline='', encoding='utf-8-sig') as csvfile:
            reader = csv.reader(csvfile, dialect=self.dialect)
            next(reader)
            for row in reader:
                yield {column_names[n]: row[n] for n in range(len(column_names))}

    def get_data_labels(self):
        yield None

    def load_metadata(self):
        # Metadata is just the preamble if it exists
        metadata = {}
        columns_with_data = {name: {"has_data": True} for name in self.header}

        return metadata, columns_with_data
