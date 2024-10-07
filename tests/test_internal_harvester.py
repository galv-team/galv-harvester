# SPDX-License-Identifier: BSD-2-Clause
# Copyright  (c) 2020-2023, The Chancellor, Masters and Scholars of the University
# of Oxford, and the 'Galv' Developers. All rights reserved.
import unittest
import os

import pandas
from galv_harvester.harvest import InternalHarvestProcessor
from tests.test_harvester import get_test_file_path


class TestInternalHarvester(unittest.TestCase):
    def setUp(self):
        self.path = os.path.join(get_test_file_path(), r"headered.csv")
        self.processor = InternalHarvestProcessor(self.path)

    def test_init(self):
        self.assertEqual(self.processor.file_path, self.path)
        self.assertIsNone(self.processor.monitored_path)
        self.assertEqual(self.processor.parser.__name__, "DelimitedInputFile")

    def test_summarise(self):
        summary = self.processor.summarise_columns()
        self.assertTrue(isinstance(summary, pandas.DataFrame))
        self.assertTupleEqual(summary.shape, (self.processor.summary_row_count, 3))
        for c in ["test_time", "amps", "volts"]:
            self.assertIn(c, summary.columns)

    def test_process(self):
        with self.subTest("No mapping defined"):
            with self.assertRaises(RuntimeError) as e:
                self.processor.process_data()
            self.assertIn("mapping", str(e.exception))
        self.processor.mapping = {}
        with self.subTest("With mapping"):
            self.assertIsNone(self.processor.png_file_name)
            self.assertIsNone(self.processor.data_file_name)
            self.assertIsNone(self.processor.row_count)
            self.assertIsNone(self.processor.partition_count)
            self.processor.process_data()
            self.assertIsNotNone(self.processor.png_file_name)
            self.assertIsNotNone(self.processor.data_file_name)
            self.assertEqual(
                self.processor.row_count, 39
            )  # 40 rows, 1 header in the test file
            self.assertEqual(self.processor.partition_count, 1)


if __name__ == "__main__":
    unittest.main()
