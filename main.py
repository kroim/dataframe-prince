import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
from concurrent import futures
import json

output_df = pd.DataFrame(
    {
        "case_id": [1, 2, 3, 4, 5],
        "output_date": ["2021-01-20", "2021-02-21", "2020-02-08", "2020-01-07", "2020-09-05"],
        "output_carId": ["001", "001", "003", "001", "002"],
        "output_result": ["FAIL", "SUCCESS", "FAIL", "FAIL", "SUCCESS"],
    },
    columns=["case_id", "output_date", "output_carId", "output_result"],
)
input_df = pd.DataFrame(
    {
        "case_id": [1, 2, 3, 4, 5],
        "input_date": ["2021-01-20", "2021-02-21", "2020-02-08", "2020-01-07", "2020-09-05"],
        "input_carId": ["001", "001", "003", "001", "002"],
    },
    columns=["case_id", "input_date", "input_carId"],
)


def check_unique(x, old_input):
    mask = x['output_result'] == 'FAIL'
    count = mask.sum()
    if count == 0:
        return old_input
    indexes = x.loc[mask]
    while True:
        arr = np.arange(1, count + 1) * 7
        np.random.shuffle(arr)
        td = pd.to_timedelta(arr, unit='d')
        delta_date = pd.to_datetime(old_input.loc[indexes.index, 'input_date']) + td
        delta_date = pd.to_datetime(delta_date).dt.strftime("%Y-%m-%d")
        flag, delta_date = check_api_call(count, delta_date)
        if not flag:
            continue
        old_input.loc[indexes.index, 'input_date'] = delta_date
        new_dates = delta_date.unique()
        old_dates = old_input['input_date'].unique()
        if np.any(np.in1d(new_dates, old_dates)):
            break
    return old_input


def check_api_call(count, dates):
    length = dates.values.__len__()
    executor = futures.ThreadPoolExecutor()
    for i in range(length):
        date = dates.values[i]
        pool = executor.submit(task_api, date)
        response = pool.result()
        while not response:
            count = count + 1
            day_value = count * 7
            td = pd.to_timedelta(day_value, unit='d')
            delta_date = datetime.strptime(date, "%Y-%m-%d") + td
            new_date = delta_date.strftime("%Y-%m-%d")
            pool = executor.submit(task_api, new_date)
            response = pool.result()
            if not response:
                continue
            dates.values[i] = new_date
    return True, dates


def task_api(date):
    url = "http://127.0.0.1:3330/api/ecat-test-response?date=" + date
    response = requests.get(url)
    if response.status_code == 404:
        return False
    else:
        return True



def change_input_df(input_data, name, change_path, change_value):
    input_len = len(input_data)
    paths = change_path.split(".")
    new_input_data = []
    for i in range(input_len):
        input_item = input_data[i]
        if not name == input_item['name']:
            new_input_data.append(input_item)
            continue
        path_data = {}
        keys = input_item.keys()
        new_item_data = {}
        for j in range(len(paths)):
            if j == 0:
                if paths[j] not in keys:
                    return False
                else:
                    for key in keys:
                        if key == paths[j]:
                            path_data = input_item[paths[j]]
                            if type(path_data) is not dict:
                                path_data = json.loads(path_data)
                            new_item_data[key] = path_data
                        else:
                            new_item_data[key] = input_item[key]
                    keys = path_data.keys()
            else:
                if paths[j] not in keys:
                    return False
                if j == len(paths) - 1:
                    path_data[paths[j]] = change_value
                new_path_data = {}
                new_path_keys = {}
                for key in keys:
                    if j == 1:
                        new_item_data[paths[0]][key] = path_data[key]
                    elif j == 2:
                        new_item_data[paths[0]][paths[1]][key] = path_data[key]
                    if key == paths[j] and j < len(paths) - 1:
                        new_path_data = path_data[key]
                        new_path_keys = new_path_data.keys()
                if j < len(paths) - 1:
                    path_data = new_path_data
                    keys = new_path_keys
        new_input_data.append(new_item_data)
    print(new_input_data)
    return new_input_data


group_by = output_df.groupby('output_carId', group_keys=False)
input_len = input_df.__len__()
group_len = group_by.groups.__len__()
apply_input = group_by.apply(lambda x: check_unique(x, input_df))
new_input = apply_input[input_len * (group_len - 1):input_len * group_len]
print(new_input)
