import pandas as pd
import numpy as np
import json
import requests
from concurrent import futures
from datetime import datetime, timedelta
import random
import holidays
filename = "failed_cases"


def read_failed_cases(file_name):
    data = []
    with open(filename, 'r') as f:
        lines = [json.loads(_) for _ in f.readlines()]
    data = pd.json_normalize(lines)
    res = data.groupby(data['failureReason.Cause'].str[:36], group_keys=False)
    return res


groups = read_failed_cases(filename)
print(groups.groups)
