""""""

import csv
import random
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
    driver_night_shifts = {driver: 0 for driver in qualified_routes.index}

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
                        if driver in list(vals.values()):
                            leftover_drivers = leftover_drivers_per_route.get(route_name)
                            if leftover_drivers:
                                # Ensure that we replace the correct driver on the correct shift.
                                if driver == route_result.get(route_name)[1]:
                                    route_result.get(route_name)[1] = leftover_drivers.pop(random.randrange(len(leftover_drivers)))
                                else:
                                    night_drivers = [driver for driver in leftover_drivers if driver_night_shifts[driver] < 5]
                                    new_driver = night_drivers.pop(random.randrange(len(night_drivers)))
                                    route_result.get(route_name)[2] = new_driver
                                    driver_night_shifts[new_driver] += 1
                                    driver_night_shifts[driver] -= 1

                    if len(possible_route_drivers) > 2:
                        possible_route_drivers.remove(driver)
                        continue

            day_shift = possible_route_drivers.pop(random.randrange(len(possible_route_drivers)))
            night_shift = possible_route_drivers.pop(random.randrange(len(possible_route_drivers)))

            route_result[route] = {
                1: day_shift,
                2: night_shift
                }

            leftover_drivers_per_route[route] = possible_route_drivers
            driver_night_shifts[night_shift] += 1

        result[day] = route_result
    return result

# def process() -> typing.DefaultDict[str, typing.Any]:
#     result = defaultdict(list)
#     drivers_per_route = {}
#     driver_night_shifts = {driver: 0 for driver in qualified_routes.index}
#
#     for day in day_off_booked:
#         route_result = {}
#         previous_route_drivers = []
#         leftover_drivers_per_route = {}
#
#         for route, _ in qualified_drivers.items():
#             possible_route_drivers = []
#
#             # Check that driver is qualified to drive route, and has not booked the day off.
#             for driver in qualified_routes.index:
#                 if driver in qualified_drivers[route] and driver not in driver_days_off[day]:
#                     possible_route_drivers.append(driver)
#
#             route_result[route] = possible_route_drivers
#
#         drivers_per_route[day] = route_result
#
#     days = defaultdict(list)
#     for day, routes in drivers_per_route.items():
#         route_per_day = {}
#         drivers_driven = []
#         for route_name, drivers in routes.items():
#             best_drivers = []
#             if len(drivers) == 2:
#                 if driver_night_shifts[drivers[1]] > 4:
#                     drivers[0], drivers[1] = drivers[1], drivers[0]
#                 route_per_day[route_name] = {1: drivers[0], 2: drivers[1]}
#                 driver_night_shifts[drivers[1]] += 1
#                 drivers_driven.extend(drivers[:2])
#
#             else:
#                 shift = {}
#                 for index, driver in enumerate(list(drivers)):
#                     shift[index+1] = driver
#                     if index + 1 == 2:
#                         driver_night_shifts[driver] += 1
#
#                     if len(shift.values()) == 2:
#                         break
#
#                 drivers_driven.extend(shift.values())
#
#                 route_per_day[route_name] = shift
#
#
#         days[day] = route_per_day
#
#     print(days)
#     print(driver_night_shifts)
#     print(driver_night_shifts.values())
#     print(sum(driver_night_shifts.values()))
#     return result


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
