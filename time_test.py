import boto3
import pandas as pd
import math
import matplotlib.pyplot as plt
import os
import json
import time
import datetime
from dateutil import tz


to_zone = tz.gettz('Europe/Berlin')
first_time = '2022-09-08 16:22:25.115000'
last_time = '2022-09-08 23:41:30.592000'
first_ts = datetime.datetime.strptime(first_time, "%Y-%m-%d %H:%M:%S.%f")
first_ts = first_ts.replace(tzinfo=to_zone)
last_ts = datetime.datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S.%f")
last_ts = last_ts.replace(tzinfo=to_zone)

# dt = datetime.combine(date.today(), time(23, 55)) + timedelta(minutes=30)

print(first_ts + datetime.timedelta(seconds=1))