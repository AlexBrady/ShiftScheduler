""""""

import csv
import random
import typing

from collections import defaultdict

from scheduler.validate import Schemas


def process() -> typing.DefaultDict[str, typing.Any]:
    try:
        schemas = Schemas()
    except ValueError:
        raise

    qualified_drivers = {}
    for route in schemas.qualified_routes:
        qualified_drivers[route] = schemas.qualified_routes.index[schemas.qualified_routes[route] == 1].tolist()

    driver_days_off = {}
    for day in schemas.day_off_booked:
        driver_days_off[day] = schemas.day_off_booked.index[schemas.day_off_booked[day] == 1].tolist()

    driver_days_off_preference = {}
    for day in schemas.pref_day_off:
        driver_days_off_preference[day] = schemas.pref_day_off.index[schemas.pref_day_off[day] == 1].tolist()

    driver_shifts = defaultdict(list)
    for _, day in schemas.pref_work_shift.iterrows():
        for i, shift in enumerate(day.values):
            if shift == 1 or shift == 2:
                driver_shifts[day.name].append((i + 1, shift))

    result = defaultdict(list)
    drivers_per_route = {}
    driver_night_shifts = {driver: 0 for driver in schemas.qualified_routes.index}

    for day in schemas.day_off_booked:
        route_result = {}

        for route_name, drivers in qualified_drivers.items():
            possible_route_drivers = []

            # Check that driver is qualified to drive route, and has not booked the day off.
            for driver in drivers:
                if driver not in driver_days_off[day]:
                    possible_route_drivers.append(driver)

            route_result[route_name] = possible_route_drivers

            drivers_per_route[day]  = route_result
        result[day] = drivers_per_route[day]

        drivers_per_route[day] = dict(sorted(route_result.items(), key=lambda i: len(i[1]), reverse=False))

    for day, routes in drivers_per_route.items():
        route_result = {}
        previous_route_drivers = []
        leftover_drivers_per_route = {}

        for route_name, route_drivers in routes.items():
            possible_route_drivers = []

            # Keep track of riders who have already worked today.
            for _, vals in route_result.items():
                previous_route_drivers.extend(list(vals.values()))

            # Check that driver is qualified to drive route, and has not booked the day off.
            for driver in route_drivers:
                if driver not in previous_route_drivers:
                    possible_route_drivers.append(driver)

            night_drivers = {}
            for driver in possible_route_drivers:
                if driver_night_shifts[driver] < 4:
                    night_drivers[driver] = driver_night_shifts[driver]

            min_value = min(night_drivers.values())
            night_drivers = [k for k in night_drivers if night_drivers[k] == min_value]

            # pick better drivers
            night_shift = random.choice(night_drivers)

            possible_route_drivers.remove(night_shift)
            day_shift = random.choice(possible_route_drivers)

            route_result[route_name] = {
                1: day_shift,
                2: night_shift
                }

            leftover_drivers_per_route[route_name] = possible_route_drivers
            driver_night_shifts[night_shift] += 1

        result[day] = route_result
    return result

def write_to_csv():
    data = process()
    cool_list = []

    for day, route_data in data.items():
        for route_name, route in route_data.items():
            for i in range(1, 3):
                cool_list.append([route[i], day, route_name, i])

    with open('output.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Driver ID', 'Day', 'Route ID', 'Shift ID'])

        writer.writerows(cool_list)

write_to_csv()
