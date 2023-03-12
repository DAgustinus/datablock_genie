import random

from faker import Faker
from typing import Optional, Literal, List, Union
from datetime import datetime

from resources.constants import (
    DATETIME,
    FLOAT,
    INTEGER,
    NAME,
    INT_MAX,
    INT_MIN,
)


class FakerGen:
    def __init__(self):
        self.faker = Faker()

    def get_data(self, data_category: str, column_args: dict = None):
        if data_category == DATETIME:
            datetime_range = column_args["datetime_range"] if column_args and "datetime_range" in column_args else None
            datetime_format = column_args["datetime_format"] if column_args and "datetime_format" in column_args else None
            return self.generate_datetime(datetime_range=datetime_range, datetime_format=datetime_format)
        elif data_category == FLOAT:
            float_range = column_args["float_range"] if column_args and "float_range" in column_args else None
            return self.generate_float(float_range=float_range)
        elif data_category == INTEGER:
            int_range = column_args["int_range"] if column_args and "int_range" in column_args else None
            return self.generate_int(int_range=int_range)
        elif data_category == NAME:
            name_type = column_args["name_type"] if column_args and "name_type" in column_args else None
            full_name = column_args["full_name"] if column_args and "full_name" in column_args else True
            return self.generate_name(name_type=name_type, full_name=full_name)
        else:
            raise ValueError(f"Please specify data_category arg.")

    def generate_name(self, name_type: Literal["first", "last", None] = None, full_name: bool = True) -> str:
        name = self.faker.name()

        if (name_type and full_name) or name_type:
            return name.split(" ")[0] if name_type == "first" else name.split(" ")[1]
        else:
            return name

    def generate_datetime(
            self, datetime_range: Optional[List[datetime]] = None,
            datetime_format: str = None
    ) -> Union[datetime, str]:
        if datetime_range and (sorted(datetime_range) != datetime_range or len(datetime_range) != 2):
            raise ValueError(f"Please double check datetime_range. Ensure that you only have 2 in the list and they are"
                             f"in ascending order.")

        if datetime_range:
            min_dt = datetime_range[0]
            max_dt = datetime_range[1]
            while True:
                gen_dt = self._generate_dt()
                if gen_dt >= min_dt or max_dt <= gen_dt:
                    if datetime_format:
                        return self._format_dt(gen_dt, datetime_format)
                    else:
                        return gen_dt
        else:
            gen_dt = self._generate_dt()
            if datetime_format:
                return self._format_dt(gen_dt, datetime_format)
            else:
                return gen_dt

    @staticmethod
    def generate_int(int_range: Optional[List[int]] = None) -> int:
        if int_range and (len(int_range) != 2 or sorted(int_range) != int_range or None in int_range):
            raise ValueError(f"Please double check the int_range arg and ensure that its length is 2 and "
                             f"in ascending order.")

        max_range = INT_MAX if not int_range else int_range[1]
        min_range = INT_MIN if not int_range else int_range[0]

        return random.randint(min_range, max_range)

    @staticmethod
    def generate_float(float_range: List[Union[int, float]] = None) -> float:
        if float_range and (len(float_range) != 2 or sorted(float_range) != float_range or None in float_range):
            raise ValueError(f"Please double check the float_range arg and ensure that its length is 2 and "
                             f"in ascending order.")

        max_range = float(INT_MAX) if not float_range else float_range[1]
        min_range = float(INT_MIN) if not float_range else float_range[0]

        return random.uniform(min_range, max_range)

    def _generate_dt(self) -> datetime:
        return datetime.strptime(self.faker.date() + " " + self.faker.time(), "%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _format_dt(dt: datetime, dt_format: str) -> str:
        try:
            formatted_dt = dt.strftime(dt_format)
            return formatted_dt
        except Exception as e:
            raise ValueError(f"Please check format for datetime format. {e}")

