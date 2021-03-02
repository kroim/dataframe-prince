import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
from concurrent import futures

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


group_by = output_df.groupby('output_carId', group_keys=False)
input_len = input_df.__len__()
group_len = group_by.groups.__len__()
apply_input = group_by.apply(lambda x: check_unique(x, input_df))
new_input = apply_input[input_len * (group_len - 1):input_len * group_len]
print(new_input)
