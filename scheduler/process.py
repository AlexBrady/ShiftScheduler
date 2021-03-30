""""""

import csv
import random
import typing

from collections import defaultdict

from scheduler.validate import Schemas


class Process:

    def __init__(self):
        self.schemas = Schemas()
        self.qualified_drivers = self._get_qualified_drivers_per_route()
        self.driver_days_off = self._get_days_off_per_driver()
        self.driver_days_off_preference = self._get_driver_days_off_preference()
        self.driver_shifts = self._get_driver_shift_preference()

    def process(self) -> typing.Dict[str, typing.Dict[str, typing.Dict[int, int]]]:
        """Generate the schedule."""
        drivers_per_route = self._get_available_drivers()

        driver_night_shifts = {driver: 0 for driver in self.schemas.qualified_routes.index}
        result = {day: {} for day in self.driver_days_off.keys()}
        for day, routes in drivers_per_route.items():
            print(f'Day: {day}')
            print(f'night counts: {driver_night_shifts}')
            route_result = {}
            previous_route_drivers = set()

            for route_name, route_drivers in routes.items():

                print(f'Route: {route_name}')
                possible_route_drivers = []

                # Keep track of riders who have already worked today.
                for _, vals in route_result.items():
                    for v in vals.values():
                        previous_route_drivers.add(v)

                print(f'Previous drivers: {previous_route_drivers}')

                # Check that the driver has not already worked today.
                for driver in route_drivers:
                    if driver not in previous_route_drivers:
                        possible_route_drivers.append(driver)

                print(f'Potential drivers: {possible_route_drivers}')

                night_drivers = {}
                for driver in possible_route_drivers:
                    if driver_night_shifts[driver] < 4:
                        night_drivers[driver] = driver_night_shifts[driver]

                if not night_drivers:
                    print(result)

                min_value = min(night_drivers.values())
                night_drivers = [k for k in night_drivers if night_drivers[k] == min_value]
                pref = [d for d in night_drivers if d not in self.driver_days_off_preference[day]]
                if not pref:
                    pref = night_drivers

                print(f'MIN night drivers: {pref}')

                # pick driver with less
                night_shift = random.choice(pref)

                possible_route_drivers.remove(night_shift)

                day_drivers = {driver: driver_night_shifts[driver] for driver in possible_route_drivers}

                max_value = max(day_drivers.values())
                day_drivers = [k for k in day_drivers if day_drivers[k] == max_value]

                print(f'MAX day drivers: {day_drivers}')

                day_shift = random.choice(day_drivers)

                route_result[route_name] = {
                    1: day_shift,
                    2: night_shift
                    }
                print(f'Chosen Drivers: {day_shift}, {night_shift}')

                driver_night_shifts[night_shift] += 1

            result[day] = route_result
        return result

    def write_to_csv(self):
        data = self.process()
        cool_list = []

        for day, route_data in data.items():
            for route_name, route in route_data.items():
                for i in range(1, 3):
                    cool_list.append([route[i], day, route_name, i])

        with open('output.csv', 'w') as f:
            writer = csv.writer(f)
            writer.writerow(['Driver ID', 'Day', 'Route ID', 'Shift ID'])

            writer.writerows(cool_list)

    def _get_available_drivers(self) -> typing.Dict[str, typing.Dict[str, typing.List[int]]]:
        """
        Get the list of drivers qualified to drive, who have not booked the day off per route per day.
        :return:
        """
        drivers_per_route = {}

        for day in self.schemas.day_off_booked:
            route_result = {}

            for route_name, drivers in self.qualified_drivers.items():
                possible_route_drivers = []

                # Check that driver is qualified to drive route, and has not booked the day off.
                for driver in drivers:
                    if driver not in self.driver_days_off[day]:
                        possible_route_drivers.append(driver)

                route_result[route_name] = possible_route_drivers

            drivers_per_route[day] = dict(sorted(route_result.items(), key=lambda r: len(r[1]), reverse=False))

        return drivers_per_route

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

    def _get_driver_days_off_preference(self) -> typing.Dict[int, typing.List[int]]:
        """Get the preferred days off that each driver would like."""
        driver_days_off_preference = {}
        for day in self.schemas.pref_day_off:
            driver_days_off_preference[day] = self.schemas.pref_day_off.index[
                self.schemas.pref_day_off[day] == 1].tolist()

        return driver_days_off_preference

    def _get_driver_shift_preference(self) -> typing.Dict[int, typing.List[typing.Tuple[int, int]]]:
        """Get the preferred shifts that each driver would like."""
        driver_shifts = defaultdict(list)
        for _, day in self.schemas.pref_work_shift.iterrows():
            for i, shift in enumerate(day.values):
                # 1 = morning shift, 2 = night shift.
                if shift == 1 or shift == 2:
                    driver_shifts[day.name].append((i + 1, shift))

        return driver_shifts


process = Process()
process.write_to_csv()
