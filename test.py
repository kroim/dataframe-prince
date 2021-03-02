import json
import pandas as pd

# a = {"description": "Day Shift - Scheduled - Strict - Worked during shift time",
#     "name": "3",
#     "testData": {'id': '3', 'description': 'Day Shift - Scheduled - Strict - Worked during shift time', 'usecase_runconfig': {'start_date': '06/08/2020', 'end_date': '', 'is_holiday': False, 'employee': {'id': '500006740', 'group': 'Amazon_Part_Time_Hourly_TRD-ITALY-PN'}}, 'days': [{'day': 1, 'shifts': [{'shift_id': '1', 'intervals': [{'interval_id': '1', 'interval_type': 'Regular Strict', 'start_time': '08:00', 'end_time': '12:00', 'pay_modifier': '', 'duration_in_minutes': ''}]}], 'punches': [{'timestamp': '08:00', 'inouthint': 'in'}, {'timestamp': '12:00', 'inouthint': 'out'}], 'timeoffs': [], 'policies': [{'name': 'RegularHours-Italy-PN', 'value': '4', 'shift_interval_id': '1_1'}, {'name': 'MealVoucher-Italy-PN', 'value': '0', 'shift_interval_id': '1_1'}], 'anomalies': [], 'infractions': []}]}
#     }
# print(a['testData']['days'][0]['shifts'][0]['intervals'][0])
lst = []
raw = []
# with open('eg_input') as f:
#     lst = [json.loads(_.replace('}]}"},', '}]}"}')) for _ in f.readlines()]

with open('eg_output') as of:
    str_data = '['
    pre_line = ''
    str_data += of.read().replace("}\n{\n", "}, {\n") + "]"
    print(str_data)
    # for line in of.readlines():
    #     print(line)
    #     if pre_line == '}\n' and line == '{\n':
    #         str_data += ", " + line
    #     else:
    #         str_data += line
    #     pre_line = line
    # str_data += "]"
    # print(str_data)
    data = json.loads(str_data)
    print(data)


# print(data)

