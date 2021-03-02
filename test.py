import json
import pandas as pd
import holidays
import random


def select_holiday(num):
    _holidays = []
    for date in holidays.UnitedStates(years=2021).items():
        print(date)
        _holidays.append(str(date[0]))
    res = []
    for i in range(num):
        rnd = random.randint(0, len(_holidays) - 1)
        res.append(_holidays[rnd])
    return res


res = select_holiday(2)
print(res)
