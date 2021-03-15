import pandas as pd
import numpy as np
import json
import requests
from concurrent import futures
from datetime import datetime, timedelta
import random
import holidays


filename = "failed_cases"


# change input date of all data for each group by employeeID with unique date in a group
def check_unique_for_input_date(x, old_input):
    count = len(x)
    if count < 1:
        return old_input
    employee_id = x.iloc[0]['testData.usecase_runconfig.employee.id']
    holiday_mask = x['testData.usecase_runconfig.is_holiday']
    holiday_count = holiday_mask.sum()
    while True:
        arr = np.arange(1, count + 1) * 7
        for (i, item) in enumerate(arr):
            if item > 450:
                arr[i] = arr[i] % 450
        np.random.shuffle(arr)
        td = pd.to_timedelta(arr, unit='d')
        delta_date = pd.to_datetime(old_input.loc[x.index, 'testData.usecase_runconfig.start_date']) + td
        holiday_index = []
        if holiday_count > 0:
            holiday_items = x.loc[holiday_mask]
            holiday_dates = select_holiday(holiday_count)
            delta_date.loc[holiday_items.index] = holiday_dates
            holiday_index = holiday_items.index
        delta_date = pd.to_datetime(delta_date).dt.strftime("%Y-%m-%d")
        flag, delta_date = check_api_call(count, delta_date, employee_id, holiday_index)
        if not flag:
            continue
        old_input.loc[x.index, 'testData.usecase_runconfig.start_date'] = delta_date
        new_dates = delta_date.unique()
        old_dates = old_input['testData.usecase_runconfig.start_date'].unique()
        if np.any(np.in1d(new_dates, old_dates)):
            break
    return old_input


# change input employeeID of all data for each group by employeeID
def check_unique_for_input_employee(x, old_input):
    count = len(x)
    if count < 1:
        return old_input
    new_employeeId = get_valid_employee_id_from_merlin('', x.iloc[0]['testData.usecase_runconfig.employee.group'])
    old_input.loc[x.index, 'testData.usecase_runconfig.employee.id'] = new_employeeId
    return old_input


# create processes for requests
def check_api_call(count, dates, employee_id, holiday_index):
    length = dates.values.__len__()
    holiday_indexes = []
    for k in range(len(holiday_index)):
        holiday_indexes.append(holiday_index[k])
    # print("holiday index: ", holiday_indexes)
    executor = futures.ThreadPoolExecutor()
    for i in range(length):
        date = dates.values[i]
        index = dates.index[i]
        pool = executor.submit(task_api, (date, employee_id))
        response = pool.result()
        while not response:
            if index in holiday_indexes:
                new_date = select_holiday(1)[0]
            else:
                count = count + 1
                day_value = count * 7 % 450
                td = pd.to_timedelta(day_value, unit='d')
                delta_date = datetime.strptime(date, "%Y-%m-%d") + td
                new_date = delta_date.strftime("%Y-%m-%d")
            pool = executor.submit(task_api, (new_date, employee_id))
            response = pool.result()
            if not response:
                continue
            dates.values[i] = new_date
    # print(dates)
    return True, dates


# api request function: args(date, employeeID)
def task_api(args):
    date = args[0]
    employee_id = args[1]
    url = "http://127.0.0.1:3330/api/ecat-test-response/employees/employeeid:" + str(employee_id) + "/?date=" + date
    response = requests.get(url, headers={"Accept-Encoding": "gzip, deflate, br", "UserToken": "WordEventsQA"})
    if response.status_code == 404:
        return False
    else:
        return True


# check output status
def check_output(out_data):
    if out_data['status'] == "PASSED":
        return True
    else:
        return False


# change input data when case is dates for all data
def change_date(input_data):
    input_df = pd.json_normalize(input_data)
    group_by = input_df.groupby('testData.usecase_runconfig.employee.id', group_keys=False)
    input_len = input_df.__len__()
    group_len = group_by.groups.__len__()
    apply_input = group_by.apply(lambda x: check_unique_for_input_date(x, input_df))
    new_input = apply_input[input_len * (group_len - 1):input_len * group_len]
    return new_input


# change employeeID when case is employee for all data
def change_employee(input_data):
    input_df = pd.json_normalize(input_data)
    group_by = input_df.groupby('testData.usecase_runconfig.employee.id', group_keys=False)
    input_len = input_df.__len__()
    group_len = group_by.groups.__len__()
    apply_input = group_by.apply(lambda x: check_unique_for_input_employee(x, input_df))
    new_input = apply_input[input_len * (group_len - 1):input_len * group_len]
    return new_input


# change input data when case is date for failed output
def change_date_from_output(x, old_input):
    mask = x['status'] == 'FAILED'
    count = mask.sum()
    if count == 0:
        return old_input
    indexes = x.loc[mask]
    employee_id = indexes.iloc[0]['employee_id']
    holiday_mask = x['testData.usecase_runconfig.is_holiday']
    holiday_count = holiday_mask.sum()
    while True:
        arr = np.arange(1, count + 1) * 7
        np.random.shuffle(arr)
        td = pd.to_timedelta(arr, unit='d')
        delta_date = pd.to_datetime(old_input.loc[indexes.index, 'testData.usecase_runconfig.start_date']) + td
        holiday_index = []
        if holiday_count > 0:
            holiday_items = x.loc[holiday_mask]
            holiday_dates = select_holiday(holiday_count)
            delta_date.loc[holiday_items.index] = holiday_dates
            holiday_index = holiday_items.index
        delta_date = pd.to_datetime(delta_date).dt.strftime("%Y-%m-%d")
        flag, delta_date = check_api_call(count, delta_date, employee_id, holiday_index)
        if not flag:
            continue
        old_input.loc[indexes.index, 'testData.usecase_runconfig.start_date'] = delta_date
        new_dates = delta_date.unique()
        old_dates = old_input['testData.usecase_runconfig.start_date'].unique()
        if np.any(np.in1d(new_dates, old_dates)):
            break
    return old_input


# change input data when case is employee for failed output
def change_employee_from_output(x, old_input):
    mask = x['status'] == 'FAILED'
    count = mask.sum()
    if count == 0:
        return old_input
    indexes = x.loc[mask]
    new_employeeId = get_valid_employee_id_from_merlin('', indexes.iloc[0]['testData.usecase_runconfig.employee.group'])
    old_input.loc[indexes.index, 'testData.usecase_runconfig.employee.id'] = new_employeeId
    return old_input


# change input data from failed output
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


# make a json file from dataframe for input data
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
        start_date = item['testData.usecase_runconfig.start_date']
        usecase_runconfig['start_date'] = datetime.strptime(start_date, '%Y-%m-%d').strftime('%m/%d/%Y')
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
        item_data = str(item_data).replace('\'', '\"').replace('False', 'false').replace('True', 'true')
        item_data = item_data.encode('utf-8').decode('unicode-escape').replace(': \\"None\\"', ': None')
        if i == 0:
            new_output_data += item_data
        else:
            new_output_data += '\n' + item_data
    with open(output_file, 'w') as f:
        f.write(new_output_data)


# make a dataframe data from input json file
def read_json_file(filename):
    data = []
    with open(filename, 'r') as f:
        lines = [json.loads(_.replace('}]}"},', '}]}"}')) for _ in f.readlines()]
    for line in lines:
        line_test_data = line['testData']
        line_test_data = line_test_data.replace(": None", ": \"None\"").replace("\n", "")
        # print(line_test_data)
        line['testData'] = json.loads(line_test_data)
        data.append(line)
    return data


# make a dataframe data from output json file
def read_output_file(filename):
    with open(filename) as of:
        str_data = '['
        str_data += of.read().replace("}\n{\n", "}, {\n") + "]"
        data = json.loads(str_data)
    return data


def get_valid_employee_id_from_merlin(presetId, populationId):
    new_employeeId = random.randint(1000, 9999)
    return new_employeeId


def select_holiday(num):
    _holidays = []
    for date in holidays.UnitedStates(years=2021).items():
        _holidays.append(str(date[0]))
    res = []
    for i in range(num):
        rnd = random.randint(0, len(_holidays) - 1)
        res.append(_holidays[rnd])
    return res


# module 1
def change_all_values(input_file, change_case):
    input_data = read_json_file(input_file)
    if change_case == "dates":
        # change date
        new_input = change_date(input_data)
        print_json_from_input_df(new_input, "new_output_from_all_date")
    elif change_case == "employee":
        # change employeeId
        new_input = change_employee(input_data)
        print_json_from_input_df(new_input, "new_output_from_employee")
    else:
        print("Undefined change case")


# module 2
def change_failed_values(input_file, output_file, change_case):
    input_data = read_json_file(input_file)
    output_data = read_output_file(output_file)
    new_input_data = change_failed_case(input_data, output_data, change_case)
    print_json_from_input_df(new_input_data, "new_output_from_failed_date")


# change_all_values(filename, "dates")
# change_all_values("eg_input", "dates")
# change_failed_values("eg_input", "eg_output", "dates")
dataframe = read_json_file(filename)
print(dataframe)
