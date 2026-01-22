import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Patch sys.path to import main from load_gold.py
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from load_gold import main


class TestLoadGold(unittest.TestCase):
    @patch("load_gold.SparkSession")
    @patch("os.makedirs")
    def test_main_runs_without_error(self, mock_makedirs, mock_spark_session):
        mock_spark = MagicMock()
        mock_spark_session.builder.appName.return_value.getOrCreate.return_value = (
            mock_spark
        )
        mock_df = MagicMock()
        mock_spark.read.parquet.return_value = mock_df
        mock_df.select.return_value.dropDuplicates.return_value = mock_df
        mock_df.write.mode.return_value.parquet.return_value = None
        mock_df.write.mode.return_value.option.return_value.csv.return_value = None
        mock_df.join.return_value.select.return_value = mock_df

        # Should not raise any exceptions
        main()

        self.assertTrue(mock_makedirs.called)
        self.assertTrue(mock_spark.read.parquet.called)
        self.assertTrue(mock_df.write.mode.called)
        self.assertTrue(mock_df.write.mode.return_value.parquet.called)
        self.assertTrue(mock_df.write.mode.return_value.option.called)
        self.assertTrue(mock_df.write.mode.return_value.option.return_value.csv.called)
        self.assertTrue(mock_df.join.called)
        self.assertTrue(mock_df.join.return_value.select.called)
        self.assertTrue(mock_spark.stop.called)


if __name__ == "__main__":
    unittest.main()
