""""""

import csv
import typing
import pandas as pd
from collections import defaultdict
import dask.dataframe as dd


qualified_routes = pd.read_csv('../data/qualified_route.csv', index_col='driverid').astype('bool')
day_off_booked = pd.read_csv('../data/forced_day_off.csv', index_col='driverid').astype('bool')
pref_day_off = pd.read_csv('../data/pref_day_off.csv', index_col='driverid').astype('bool')
pref_work_shift = pd.read_csv('../data/pref_work_shift.csv', index_col='driverid')

joined = dd.read_csv('../data/*csv')

all_dfs = pd.concat([qualified_routes, day_off_booked, pref_day_off, pref_work_shift])

qualified_drivers = {}
for route in qualified_routes:
    qualified_drivers[route] = qualified_routes.index[qualified_routes[route]].tolist()

driver_days_off = {}
for day in day_off_booked:
    driver_days_off[day] = day_off_booked.index[day_off_booked[day] == True].tolist()

driver_days_off_preference = {}
for day in pref_day_off:
    driver_days_off_preference[day] = pref_day_off.index[pref_day_off[day] == True].tolist()

driver_shifts = defaultdict(list)
for _, day in pref_work_shift.iterrows():
    for i, shift in enumerate(day.values):
        if shift == 1 or shift == 2:
            driver_shifts[day.name].append((i+1, shift))


def process() -> typing.DefaultDict[str, typing.Any]:
    result = defaultdict(list)

    for day in day_off_booked:
        route_result = {}
        previous_route_drivers = []
        leftover_drivers_per_route = {}

        for route, _ in qualified_drivers.items():
            possible_route_drivers = []

            # Check that driver is qualified to drive route, and has not booked the day off.
            for driver in qualified_routes.index:
                if driver in qualified_drivers[route] and driver not in driver_days_off[day]:
                    possible_route_drivers.append(driver)

            # Keep track of riders who have already worked today.
            for _, vals in route_result.items():
                previous_route_drivers.extend(list(vals.values()))

            for driver in list(possible_route_drivers):
                if driver in previous_route_drivers:
                    # Replace driver if they have already worked today.
                    for route_name, vals in route_result.items():
                        if driver in list(vals.values()) and leftover_drivers_per_route.get(route_name):
                            route_result.get(route_name)[1] = leftover_drivers_per_route.get(route_name).pop(0)

                    if len(possible_route_drivers) > 2:
                        possible_route_drivers.remove(driver)
                        continue

            route_result[route] = {
                1: possible_route_drivers[0],
                2: possible_route_drivers[1]
                }
            leftover_drivers_per_route[route] = possible_route_drivers[2:]

        result[day] = route_result
    print(result)
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
