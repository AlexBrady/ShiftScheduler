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
    driver_night_shifts = {driver_id: 0 for driver_id in qualified_routes.index}

    for day in day_off_booked:
        route_result = []
        previous_route = None
        previous_route_drivers = []
        leftover_drivers_per_route = {}

        for route in qualified_drivers:
            temp_route_drivers = []

            for driver in qualified_routes.index:
                if driver in qualified_drivers[route] and driver not in driver_days_off[day]:
                    temp_route_drivers.append(driver)

            for r in route_result:
                previous_route_drivers.extend(r.get(previous_route, []))

            for driver in list(temp_route_drivers):
                if driver in previous_route_drivers:
                    for temp_route in route_result:
                        for route_name in qualified_routes:
                            if driver in temp_route.get(route_name, []) and leftover_drivers_per_route.get(route_name):
                                temp_route.get(route_name)[0] = leftover_drivers_per_route.get(route_name).pop(0)

                    if len(temp_route_drivers) > 2:
                        temp_route_drivers.remove(driver)
                        continue

            route_result.append({route: temp_route_drivers[:2]})
            leftover_drivers_per_route[route] = temp_route_drivers[2:]
            previous_route = route

        result[day] = route_result
    print(result)
    return result


def write_to_csv():
    data = process()
    routes = [route for route in qualified_routes]
    cool_list = []

    for day, route_data in data.items():
        for route in route_data:
            for route_name in routes:
                drivers = route.get(route_name)
                if drivers:
                    for i in range(2):
                        cool_list.append([drivers[i], day, route_name, i+1])

    with open('output.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Driver ID', 'Day', 'Route ID', 'Shift ID'])

        writer.writerows(cool_list)

write_to_csv()
#
# first = [{day1: [{route1:[1,2]}]}]
# second = [{day1: [{route1:[1,2]}, {route2:[2,1]}]} ]
