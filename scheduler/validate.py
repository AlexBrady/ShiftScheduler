"""Validate csv input."""

import pandas as pd
import numpy as np

from pandas_schema import Schema, Column
from pandas_schema.validation import InRangeValidation, IsDtypeValidation


qualified_routes = pd.read_csv('../data/qualified_route.csv')
day_off_booked = pd.read_csv('../data/forced_day_off.csv')
pref_day_off = pd.read_csv('../data/pref_day_off.csv')
pref_work_shift = pd.read_csv('../data/pref_work_shift.csv')


class Schemas:
    """Validation schemas."""
    driver_id = Column('driverid', [IsDtypeValidation(np.dtype(np.int))]),

    qualified_routes_schema = Schema([
        Column('driverid', [IsDtypeValidation(np.dtype(np.int))]),
        *[Column(f'route{num}', [InRangeValidation(0, 2)]) for num in range(1, len(qualified_routes.columns))]
        ])
    day_off_booked_schema = Schema([
        Column('driverid', [IsDtypeValidation(np.dtype(np.int))]),
        *[Column(f'day{num}', [InRangeValidation(0, 2)]) for num in range(1, len(day_off_booked.columns))]
        ])
    work_shift_schema = Schema([
        Column('driverid', [IsDtypeValidation(np.dtype(np.int))]),
        *[Column(f'day{num}', [InRangeValidation(0, 3)]) for num in range(1, len(pref_work_shift.columns))]
        ])

    def __init__(self):
        """Initialize."""
        self.qualified_routes = qualified_routes
        self.day_off_booked = day_off_booked
        self.pref_day_off = pref_day_off
        self.pref_work_shift = pref_work_shift
        self._validate()

    def _validate(self):
        """Raise ValueError if input values of csv files are not valid."""
        errors = []
        errors.extend(self.qualified_routes_schema.validate(self.qualified_routes))
        errors.extend(self.day_off_booked_schema.validate(self.day_off_booked))
        errors.extend(self.day_off_booked_schema.validate(self.pref_day_off))
        errors.extend(self.work_shift_schema.validate(self.pref_work_shift))

        for error in errors:
            raise ValueError(f'Invalid input on the csv. error: {error}')

        self.qualified_routes = self.qualified_routes.set_index('driverid')
        self.day_off_booked = self.day_off_booked.set_index('driverid')
        self.pref_day_off = self.pref_day_off.set_index('driverid')
        self.pref_work_shift = self.pref_work_shift.set_index('driverid')
