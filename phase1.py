import pandas as pd
import numpy as np
import json
import requests
from concurrent import futures
from datetime import datetime, timedelta
import random


def check_unique_for_input_date(x, old_input):
    count = len(x)
    if count < 1:
        return old_input
    while True:
        arr = np.arange(1, count + 1) * 7
        for (i, item) in enumerate(arr):
            if item > 450:
                arr[i] = arr[i] % 450
        np.random.shuffle(arr)
        td = pd.to_timedelta(arr, unit='d')
        delta_date = pd.to_datetime(old_input.loc[x.index, 'testData.usecase_runconfig.start_date']) + td
        delta_date = pd.to_datetime(delta_date).dt.strftime("%m/%d/%Y")
        flag, delta_date = check_api_call(count, delta_date)
        if not flag:
            continue
        old_input.loc[x.index, 'testData.usecase_runconfig.start_date'] = delta_date
        new_dates = delta_date.unique()
        old_dates = old_input['testData.usecase_runconfig.start_date'].unique()
        if np.any(np.in1d(new_dates, old_dates)):
            break
    return old_input


def check_unique_for_input_employee(x, old_input):
    count = len(x)
    if count < 1:
        return old_input
    new_employeeId = get_valid_employee_id_from_merlin('', x.iloc[0]['testData.usecase_runconfig.employee.group'])
    old_input.loc[x.index, 'testData.usecase_runconfig.employee.id'] = new_employeeId
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
            day_value = count * 7 % 450
            td = pd.to_timedelta(day_value, unit='d')
            delta_date = datetime.strptime(date, "%m/%d/%Y") + td
            new_date = delta_date.strftime("%m/%d/%Y")
            pool = executor.submit(task_api, new_date)
            response = pool.result()
            if not response:
                continue
            dates.values[i] = new_date
    return True, dates


def task_api(date):
    url = "http://127.0.0.1:3330/api/ecat-test-response?date=" + date
    response = requests.get(url, headers={"Accept-Encoding": "gzip, deflate, br", "UserToken": "WordEventsQA"})
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


def check_output(out_data):
    if out_data['status'] == "PASSED":
        return True
    else:
        return False


def change_date(input_data):
    input_df = pd.json_normalize(input_data)
    group_by = input_df.groupby('testData.usecase_runconfig.employee.id', group_keys=False)
    input_len = input_df.__len__()
    group_len = group_by.groups.__len__()
    apply_input = group_by.apply(lambda x: check_unique_for_input_date(x, input_df))
    new_input = apply_input[input_len * (group_len - 1):input_len * group_len]
    return new_input


def change_employee(input_data):
    input_df = pd.json_normalize(input_data)
    group_by = input_df.groupby('testData.usecase_runconfig.employee.id', group_keys=False)
    input_len = input_df.__len__()
    group_len = group_by.groups.__len__()
    apply_input = group_by.apply(lambda x: check_unique_for_input_employee(x, input_df))
    new_input = apply_input[input_len * (group_len - 1):input_len * group_len]
    return new_input


def change_date_from_output(x, old_input):
    mask = x['status'] == 'FAILED'
    count = mask.sum()
    if count == 0:
        return old_input
    indexes = x.loc[mask]
    while True:
        arr = np.arange(1, count + 1) * 7
        np.random.shuffle(arr)
        td = pd.to_timedelta(arr, unit='d')
        delta_date = pd.to_datetime(old_input.loc[indexes.index, 'testData.usecase_runconfig.start_date']) + td
        delta_date = pd.to_datetime(delta_date).dt.strftime("%m/%d/%Y")
        flag, delta_date = check_api_call(count, delta_date)
        if not flag:
            continue
        old_input.loc[indexes.index, 'testData.usecase_runconfig.start_date'] = delta_date
        new_dates = delta_date.unique()
        old_dates = old_input['testData.usecase_runconfig.start_date'].unique()
        if np.any(np.in1d(new_dates, old_dates)):
            break
    return old_input


def change_employee_from_output(x, old_input):
    mask = x['status'] == 'FAILED'
    count = mask.sum()
    if count == 0:
        return old_input
    indexes = x.loc[mask]
    new_employeeId = get_valid_employee_id_from_merlin('', indexes.iloc[0]['testData.usecase_runconfig.employee.group'])
    old_input.loc[indexes.index, 'testData.usecase_runconfig.employee.id'] = new_employeeId
    return old_input


def change_failed_case(input_data, output_data, change_case):
    input_df = pd.json_normalize(input_data)
    output_df = pd.json_normalize(output_data)
    if change_case == "dates":
        group_by = output_df.groupby('employee_id', group_keys=False)
        input_len = input_df.__len__()
        group_len = group_by.groups.__len__()
        apply_input = group_by.apply(lambda x: change_date_from_output(x, input_df))
        new_input = apply_input[input_len * (group_len - 1):input_len * group_len]
    elif change_case == "employee":
        group_by = output_df.groupby('employee_id', group_keys=False)
        input_len = input_df.__len__()
        group_len = group_by.groups.__len__()
        apply_input = group_by.apply(lambda x: change_employee_from_output(x, input_df))
        new_input = apply_input[input_len * (group_len - 1):input_len * group_len]
    else:
        print("Unknown change case")
        new_input = {}
    return new_input


def print_json_from_input_df(data_frame, output_file):
    output = data_frame.to_json(orient="records")
    json_data = json.loads(output)
    new_output_data = ''
    for i in range(len(json_data)):
        item = json_data[i]
        employee_data = {}
        employee_data['id'] = item['testData.usecase_runconfig.employee.id']
        employee_data['group'] = item['testData.usecase_runconfig.employee.group']
        usecase_runconfig = {}
        usecase_runconfig['start_date'] = item['testData.usecase_runconfig.start_date']
        usecase_runconfig['end_date'] = item['testData.usecase_runconfig.end_date']
        usecase_runconfig['is_holiday'] = item['testData.usecase_runconfig.is_holiday']
        usecase_runconfig['employee'] = employee_data
        test_data = {}
        test_data['id'] = item['testData.id']
        test_data['description'] = item['testData.description']
        test_data['usecase_runconfig'] = usecase_runconfig
        test_data['days'] = item['testData.days']
        test_data = str(test_data).replace('\'', '\\"')
        item_data = {}
        item_data['description'] = item['description']
        item_data['name'] = item['name']
        item_data['testData'] = test_data
        item_data = str(item_data).replace('\'', '\"')
        item_data = item_data.encode('utf-8').decode('unicode-escape')
        if i == 0:
            new_output_data += item_data
        else:
            new_output_data += '\n' + item_data
    with open(output_file, 'w') as f:
        f.write(new_output_data)


def read_json_file(filename):
    data = []
    with open(filename, 'r') as f:
        lines = [json.loads(_.replace('}]}"},', '}]}"}')) for _ in f.readlines()]
    for line in lines:
        line['testData'] = json.loads(line['testData'])
        data.append(line)
    return data


def read_output_file(filename):
    with open(filename) as of:
        str_data = '['
        str_data += of.read().replace("}\n{\n", "}, {\n") + "]"
        data = json.loads(str_data)
    return data


def get_valid_employee_id_from_merlin(presetId, populationId):
    new_employeeId = random.randint(1000, 9999)
    return new_employeeId


# module 1
def change_all_values(input_file, change_case):
    input_data = read_json_file(input_file)
    if change_case == "dates":
        # change date
        new_input = change_date(input_data)
        print_json_from_input_df(new_input, "new_output")
    elif change_case == "employee":
        # change employeeId
        new_input = change_employee(input_data)
        print_json_from_input_df(new_input, "new_output")
    else:
        print("Undefined change case")


# module 2
def change_failed_values(input_file, output_file, change_case):
    input_data = read_json_file(input_file)
    output_data = read_output_file(output_file)
    new_input_data = change_failed_case(input_data, output_data, change_case)
    print_json_from_input_df(new_input_data, "new_output_from_failed_date")


# change_all_values("eg_input", "employee")
change_failed_values("eg_input", "eg_output", "dates")
