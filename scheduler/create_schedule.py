""""""

import csv
import random
import typing

from collections import defaultdict

from scheduler.validate import Schemas


class Scheduler:
    """Create a schedule for delivery drivers over a two week period."""

    def __init__(self):
        """Initialize."""
        self.schemas = Schemas()
        self.qualified_drivers = self._get_qualified_drivers_per_route()
        self.driver_days_off = self._get_days_off_per_driver()
        self.driver_days_off_preference = self._get_driver_days_off_preference()
        self.driver_shifts = self._get_driver_shift_preference()
        self.driver_night_shift_counter = {driver: 0 for driver in self.schemas.qualified_routes.index}

    def generate_schedule(self) -> typing.Dict[str, typing.Dict[str, typing.Dict[int, int]]]:
        """Generate the schedule."""
        drivers_per_route = self._get_available_drivers_per_route()
        schedule = {day: {} for day in self.driver_days_off.keys()}

        for day, routes in drivers_per_route.items():
            drivers_for_route = {}
            previous_route_drivers = set()

            for route_name, route_drivers in routes.items():
                possible_route_drivers = []

                # Keep track of riders who have already worked today.
                for drivers in drivers_for_route.values():
                    for driver in drivers.values():
                        previous_route_drivers.add(driver)

                # Check that the driver has not already worked today.
                for driver in route_drivers:
                    if driver not in previous_route_drivers:
                        possible_route_drivers.append(driver)

                # Pick the driver for the night shift and remove from list of possible day shift drivers.
                night_shift = random.choice(self._get_possible_night_shift_drivers(possible_route_drivers, day))
                possible_route_drivers.remove(night_shift)

                day_shift = random.choice(self._get_possible_day_shift_drivers(possible_route_drivers, day))

                drivers_for_route[route_name] = {
                    1: day_shift,
                    2: night_shift
                    }

                # Update counter for night shifts.
                self.driver_night_shift_counter[night_shift] += 1

            schedule[day] = drivers_for_route

        return schedule

    def write_to_csv(self):
        """Write the generated schedule data to a csv."""
        schedule = self.generate_schedule()
        csv_rows = []

        for day, route_data in schedule.items():
            for route_name, route in route_data.items():
                for i in range(1, len(route_data)):
                    csv_rows.append([route[i], day, route_name, i])

        with open('driver_schedule.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(['Driver ID', 'Day', 'Route ID', 'Shift ID'])

            writer.writerows(csv_rows)

    def _get_possible_night_shift_drivers(
            self,
            possible_route_drivers: typing.List[int],
            day: str,
            ) -> typing.List[int]:
        """
        Get the list of possible drivers for a night shift for a route.

        No driver can have more than 4 night shifts for the schedule.
        We want to use the drivers with the lowest numbers of night shifts, and take into account the drivers'
        preferred days off.
        :param possible_route_drivers: The list of drivers available for the given route.
        :return:
        """
        available_night_drivers = {
            driver: self.driver_night_shift_counter[driver]
            for driver in possible_route_drivers if self.driver_night_shift_counter[driver] < 4
            }

        least_used_night_drivers = [
            driver for driver in available_night_drivers
            if available_night_drivers[driver] == min(available_night_drivers.values())
            ]

        night_drivers = [d for d in least_used_night_drivers if d not in self.driver_days_off_preference[day]]
        if not night_drivers:
            night_drivers = least_used_night_drivers

        return night_drivers

    def _get_possible_day_shift_drivers(
            self,
            possible_route_drivers: typing.List[int],
            day: str,
            ) -> typing.List[int]:
        """
        Get a list of the drivers that can drive the day shift
        :param possible_route_drivers:
        :return:
        """
        available_day_drivers = {
            driver: self.driver_night_shift_counter[driver]
            for driver in possible_route_drivers
            }

        most_used_night_shift_drivers = [
            driver for driver in available_day_drivers
            if available_day_drivers[driver] == max(available_day_drivers.values())
            ]

        day_drivers = [d for d in most_used_night_shift_drivers if d not in self.driver_days_off_preference[day]]
        if not day_drivers:
            day_drivers = most_used_night_shift_drivers

        return day_drivers

    def _get_available_drivers_per_route(self) -> typing.Dict[str, typing.Dict[str, typing.List[int]]]:
        """Get the list of drivers qualified to drive, who have not booked the day off per route per day."""
        drivers_per_day = {}

        for day in self.schemas.day_off_booked:
            drivers_for_route = {}

            for route_name, drivers in self.qualified_drivers.items():
                possible_route_drivers = []

                # Check that driver is qualified to drive route, and has not booked the day off.
                for driver in drivers:
                    if driver not in self.driver_days_off[day]:
                        possible_route_drivers.append(driver)

                drivers_for_route[route_name] = possible_route_drivers
                
            # We want to deal with the least amount of available drivers first
            drivers_per_day[day] = dict(sorted(drivers_for_route.items(), key=lambda r: len(r[1]), reverse=False))

        return drivers_per_day

    def _get_qualified_drivers_per_route(self) -> typing.Dict[str, typing.List[int]]:
        """Get drivers that are qualified for each route."""
        qualified_drivers = {}
        for route in self.schemas.qualified_routes:
            qualified_drivers[route] = self.schemas.qualified_routes.index[
                self.schemas.qualified_routes[route] == 1].tolist()

        return qualified_drivers

    def _get_days_off_per_driver(self) -> typing.Dict[int, typing.List[int]]:
        """Get the days off that each driver has booked off."""
        driver_days_off = {}
        for day in self.schemas.day_off_booked:
            driver_days_off[day] = self.schemas.day_off_booked.index[self.schemas.day_off_booked[day] == 1].tolist()

        return driver_days_off

    def _get_driver_days_off_preference(self) -> typing.Dict[str, typing.List[int]]:
        """Get the preferred days off that each driver would like."""
        driver_days_off_preference = {}
        for day in self.schemas.pref_day_off:
            driver_days_off_preference[day] = self.schemas.pref_day_off.index[
                self.schemas.pref_day_off[day] == 1].tolist()

        return driver_days_off_preference

    def _get_driver_shift_preference(self) -> typing.Dict[int, typing.List[typing.Tuple[int, int]]]:
        """Get the preferred shifts that each driver would like for each day."""
        driver_shifts = defaultdict(list)
        for _, day in self.schemas.pref_work_shift.iterrows():
            for i, shift in enumerate(day.values):
                # 1 = morning shift, 2 = night shift.
                if shift == 1 or shift == 2:
                    driver_shifts[day.name].append((i + 1, shift))

        return driver_shifts


Scheduler().write_to_csv()
