import pandas as pd
import numpy as np
import json
import requests
from concurrent import futures
from datetime import datetime, timedelta
import random
import holidays
filename = "failed_cases"
output_filename = "output_failed_cases.json"


def read_failed_cases(file_name):
    with open(filename, 'r') as f:
        lines = [json.loads(_) for _ in f.readlines()]
    data = pd.json_normalize(lines)
    res_data = []
    for name, group in data.groupby(data['failureReason.Cause'].str[:36], group_keys=False):
        item = {"name of error type": name, "length": len(group), "full list of case_id": group['testId'].tolist()}
        res_data.append(item)
    return res_data


groups = read_failed_cases(filename)
with open(output_filename, 'w') as outfile:
    json.dump(groups, outfile)

