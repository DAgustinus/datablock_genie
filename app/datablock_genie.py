import pandas as pd

from data_util.utils import DataColumn, DataTemplate, get_logger
from data_util.data_generator import FakerGen

from pyspark.sql import DataFrame, SparkSession
from typing import List, Literal, Union


class DataBlockGenie:
    def __init__(self, row_counts: int = 100, log_level: Literal["info", "warning", "debug"] = "info"):
        self.row_counts = row_counts
        self.data_template = DataTemplate(amount=self.row_counts)
        self.logger = get_logger(log_level)
        self._data_gen = FakerGen()

    def __repr__(self) -> str:
        columns = self.data_template.columns.keys()

        if columns:
            all_columns = "".join([f"\n|-- {col}" for col in columns])
        else:
            all_columns = "-- No Columns added yet."
        return f"Dataframe row counts: {self.row_counts}.\nColumns: {all_columns}"

    def set_row_counts(self, row_count: int) -> None:
        self.row_counts = row_count if row_count != self.row_counts else self.row_counts

    def create_spark_df(self, spark_session: SparkSession) -> DataFrame:
        rows, columns = self._generate_data()
        df = spark_session.createDataFrame(rows, columns)
        self.logger.debug("Spark DF has been created")

        return df
    
    def create_pandas_df(self) -> pd.DataFrame:
        rows, columns = self._generate_data()
        df = pd.DataFrame(data=rows, columns=columns)
        self.logger.debug("Pandas DF has been created")

        return df
    
    def add_columns(self, cols: Union[List[DataColumn], DataColumn]) -> None:
        if isinstance(cols, List):
            for col in cols:
                self.data_template.columns[col.column_name] = col
        else:
            self.data_template.columns[cols.column_name] = cols

    def remove_columns(self, cols: Union[List[str], str]) -> None:
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

    def _generate_data(self) -> tuple:
        amount = self.data_template.amount

        column_names = [column_name for column_name in self.data_template.columns.keys()]

        rows = []
        for _ in range(amount):
            row_column = []
            for column_name, column_arg in self.data_template.columns.items():
                column_category = column_arg.column_category
                column_args = column_arg.column_args

                col_value = self._data_gen.get_data(column_category, column_args)
                row_column.append(col_value)
            rows.append(row_column)

        return rows, column_names
