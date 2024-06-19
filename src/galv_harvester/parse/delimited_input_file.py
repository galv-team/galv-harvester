# SPDX-License-Identifier: BSD-2-Clause
# Copyright  (c) 2020-2023, The Chancellor, Masters and Scholars of the University
# of Oxford, and the 'Galv' Developers. All rights reserved.

import csv
from .exceptions import UnsupportedFileTypeError
from .input_file import InputFile


class DelimitedInputFile(InputFile):
    """
        A class for handling input files delimited by a character
    """

    @staticmethod
    def spin_to_line(file, line):
        file.seek(0)
        for i in range(line):
            file.readline()

    def __init__(self, file_path, **kwargs):
        """
        Attempt to open a file and determine the delimiter and header status.
        Handles cases where files include preamble.

        Attempt to determine the delimiter and header status of the file.
        Uses the csv.Sniffer to determine the delimiter and header status.

        Sets self.dialect and self.has_header, and sets self.header to the column names or column_0, column_1, etc.

        :param file_path: the path to the file

        :param kwargs: additional arguments

        :raises UnsupportedFileTypeError: if the file is not a supported type
        """
        with open(file_path, newline='', encoding='utf-8-sig') as csvfile:
            try:
                csvfile.readline()
            except UnicodeDecodeError as e:
                raise UnsupportedFileTypeError from e

            last_line = 0
            chunk_size = 1024
            lines_to_check = 100
            max_header_lines = 500
            while True:
                try:
                    # Detect the delimiter and header status
                    self.spin_to_line(csvfile, last_line)
                    self.dialect = csv.Sniffer().sniff(csvfile.read(chunk_size))
                    self.spin_to_line(csvfile, last_line)
                    self.has_header = csv.Sniffer().has_header(csvfile.read(chunk_size))
                    self.spin_to_line(csvfile, last_line)

                    reader = csv.reader(csvfile, self.dialect)
                    # We should now find that we have the same length of line for the entire file
                    # We sample the first few lines to check this
                    try:
                        line_length = len(next(reader))
                        for i in range(lines_to_check):
                            if len(next(reader)) != line_length:
                                last_line = last_line + i  # +1 added below
                                raise AssertionError
                    except StopIteration:
                        # Okay up until end of file
                        pass
                except (csv.Error, AssertionError):
                    # Move to the next line and try again
                    last_line += 1
                    if last_line > max_header_lines:
                        raise UnsupportedFileTypeError((
                            f"Could not determine delimiter and header status after {max_header_lines} lines."
                        ))
                    continue

                try:
                    csvfile.seek(0)
                    if last_line > 0:
                        self.preamble = "".join([csvfile.readline() for _ in range(last_line)])
                        self.data_start = last_line
                    else:
                        self.preamble = None
                        self.data_start = 0

                    if self.has_header:
                        self.header = next(reader)
                    else:
                        self.header = [f"column_{i}" for i in range(len(next(reader)))]
                    break
                except Exception as e:
                    raise UnsupportedFileTypeError((
                        f"Identified delimiter [{self.dialect.delimiter}] after {last_line} lines,"
                        f" but could not use `next(reader)`."
                    )) from e

        super().__init__(file_path, **kwargs)
        self.logger.info(f"Type is Delimited [{self.dialect.delimiter}]")

    def load_data(self, file_path, columns):
        if all(isinstance(c, str) for c in columns):
            column_numbers = [self.header.index(c) for c in columns]
        else:
            column_numbers = columns

        if not all(isinstance(c, int) for c in column_numbers):
            raise ValueError("Columns must be either all strings or all integers")

        column_names = [self.header[i] for i in column_numbers]

        with open(file_path, newline='', encoding='utf-8-sig') as csvfile:
            self.spin_to_line(csvfile, self.data_start)
            reader = csv.reader(csvfile, dialect=self.dialect)
            if self.has_header:
                next(reader)
            for row in reader:
                yield { column_names[n]: row[column_numbers[n]] for n in range(len(column_names)) }

    def get_data_labels(self):
        yield None

    def load_metadata(self):
        # Metadata is just the preamble if it exists
        metadata = {
            "preamble": self.preamble,
        }
        columns_with_data = { name: { "has_data": True } for name in self.header }

        return metadata, columns_with_data
