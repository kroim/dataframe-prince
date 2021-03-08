import json
import pandas as pd
import holidays
from datetime import datetime
import random


def select_holiday(num):
    _holidays = []
    for date in holidays.UnitedStates(years=2021).items():
        print(date)
        date_string = str(date[0])
        date_string = datetime.strptime(date_string, "%Y-%m-%d").strftime('%m/%d/%y')
        _holidays.append(date_string)
    res = []
    for i in range(num):
        rnd = random.randint(0, len(_holidays) - 1)
        res.append(_holidays[rnd])
    return res


res = select_holiday(2)
print(res)

