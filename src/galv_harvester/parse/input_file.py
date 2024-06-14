# SPDX-License-Identifier: BSD-2-Clause
# Copyright  (c) 2020-2023, The Chancellor, Masters and Scholars of the University
# of Oxford, and the 'Galv' Developers. All rights reserved.

from .exceptions import UnsupportedFileTypeError
from ..settings import get_logger

# see https://gist.github.com/jsheedy/ed81cdf18190183b3b7d
# https://stackoverflow.com/a/30721460


class InputFile:
    """
        A class for handling input files
    """

    def __init__(self, file_path):
        self.file_path = file_path
        if not hasattr(self, 'logger'):
            self.logger = get_logger(f"InputFile({self.file_path})")
        self.metadata, self.column_info = self.load_metadata()

    def get_test_start_date(self):
        return self.metadata["Date of Test"]

    def load_data(self, file_path, available_desired_columns):
        """
        This method should be implemented by the subclass.

        It should return a generator that yields a dictionary of column_name => value
        """
        raise UnsupportedFileTypeError()

    def load_metadata(self):
        """
            returns a tuple of (metadata, column_info)

            metadata is a dictionary of metadata keys to values

            column_info is a dictionary of column names to dictionaries of column info
        """
        raise UnsupportedFileTypeError()
