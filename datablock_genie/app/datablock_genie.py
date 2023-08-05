from typing import List, Literal, Union

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from datablock_genie.data_util.data_generator import FakerGen
from datablock_genie.data_util.utils import DataColumn, DataTemplate, get_logger


class DataBlockGenie:
    def __init__(
        self,
        row_counts: int = 100,
        log_level: Literal["info", "warning", "debug"] = "info",
    ):
        """
        DataBlockGenie generates Dataframes easily so you don't have to create a dummy configuration for a DF
        :param row_counts: the amount of rows will be generated
        :param log_level: log level ["info", "warning", "debug"]
        """
        self.row_counts = row_counts
        self.data_template = DataTemplate(amount=self.row_counts)
        self.logger = get_logger(log_level)
        self._data_gen = FakerGen()
        self.version = "v1"

    def __repr__(self) -> str:
        columns = self.data_template.columns.keys()

        if columns:
            all_columns = "".join([f"\n|-- {col}" for col in columns])
        else:
            all_columns = "-- No Columns added yet."
        return f"Dataframe row counts: {self.row_counts}.\nColumns: {all_columns}"

    def set_row_counts(self, row_count: int) -> None:
        """
        Sets row counts
        :param row_count: Set the amount of rows you'd like to have
        :return: None
        """
        self.row_counts = row_count if row_count != self.row_counts else self.row_counts

    def create_spark_df(self, spark_session: SparkSession) -> DataFrame:
        """
        Generate Spark Dataframe
        :param spark_session: Sparksession
        :return: Spark.Dataframe
        """
        rows, columns = self._generate_data(self.version)
        df = spark_session.createDataFrame(rows, columns)
        self.logger.debug("Spark DF has been created")

        return df

    def create_pandas_df(self) -> pd.DataFrame:
        """
        Generate Pandas Dataframe
        :return: pd.Dataframe
        """
        rows, columns = self._generate_data(self.version)
        if self.version == "v2":
            df = pd.DataFrame(rows)
        else:
            df = pd.DataFrame(data=rows, columns=columns)
        self.logger.debug("Pandas DF has been created")

        return df

    def add_column(
        self,
        name: str,
        category: Literal["datetime", "float", "integer", "name"],
        **args: str,
    ) -> None:
        """
        Add column to object
        :param name: Column Name
        :param category: Column category to be generated ["datetime", "float", "integer", "name"]
        :param args: keyword arguments to pass on to data generator
        :return: None
        """
        self.data_template.columns[name] = DataColumn(name, category, args)

    def remove_columns(self, cols: Union[List[str], str]) -> None:
        """
        This method accepts list of column names or a single column represented by a string
        :param cols: Name of columns as a list of string or a single string
        :return: None
        """
        temp_cols = self.data_template.columns
        if isinstance(cols, List):
            for col in cols:
                if col in temp_cols.keys():
                    del temp_cols[col]
                    self.logger.debug(f'Column "{col}" has been deleted')
                else:
                    self.logger.warning(f'Column "{col}" was not found')
        else:
            if cols in temp_cols.keys():
                del temp_cols[cols]
                self.logger.debug(f'Column "{cols}" has been deleted')
            else:
                self.logger.warning(f'Column "{cols}" was not found')

    def _generate_data(self, version: Literal["v1", "v2"] = "v1") -> tuple:
        """
        This method generates the initial data that will be plugged into pd.Dataframe or spark_session.createDataFrame
        :return: (rows, column_names)
        """
        # amount = self.data_template.amount

        column_names = [
            column_name for column_name in self.data_template.columns.keys()
        ]

        if version == "v2":
            all_values = {}
            for column_name, column_arg in self.data_template.columns.items():
                column_category = column_arg.column_category
                column_args = column_arg.column_args

                col_values_raw = self._data_gen.get_data_v2(
                    column_category, self.row_counts, column_args
                )
                col_values = list(col_values_raw)
                all_values[column_name] = col_values
            return all_values, None
        else:
            rows = []
            for _ in range(self.row_counts):
                row_column = []
                for column_name, column_arg in self.data_template.columns.items():
                    column_category = column_arg.column_category
                    column_args = column_arg.column_args

                    col_value = self._data_gen.get_data(column_category, column_args)
                    row_column.append(col_value)
                rows.append(row_column)
            return rows, column_names
