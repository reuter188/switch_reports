import boto3
import pandas as pd
import math
import matplotlib.pyplot as plt
import os
import json
import time
import datetime
from dateutil import tz
import matplotlib.ticker as plticker
from scipy import signal

BUCKET_NAME = 'dev-switch-data-coll-bucket'


def download_directory_from_s3(bucket_name, remote_directory_name):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=remote_directory_name):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket.download_file(obj.key, str(obj.key + '.json'))


def build_data_base(directory, download):
    df = pd.DataFrame()
    if download:
        download_directory_from_s3(BUCKET_NAME, directory)
    search_dir = str(directory)
    for subdir, dirs, files in os.walk(search_dir):
        for file in files:
            filename = os.path.join(subdir, file)
            df = pd.concat([df, pd.read_json(str(filename), lines=True)], ignore_index=True)
    # df['timestamp'].tz_convert('Europe/Berlin')
    return df


main_data = build_data_base('measurements/2022/11/30', download=True)
# main_data = pd.read_csv('data_frame.csv', infer_datetime_format=True)
# main_data.to_csv('data_frame.csv', index=False)
# # The file id.csv contains all device IDs for the devices installed
test_device = 'nrf-350916066834204'
# # The following formate of date is required to use the functions
test_date = 'Nov/30'
x_array = []
y_array = []
z_array = []
# time_array = []
time_list = []
event_rms = {'x': [], 'y': [], 'z': []}
events = {'x': [], 'y': [], 'z': []}
for x in range(len(main_data.index)):
    for i in range(len(main_data['payload'][x])):
        x_array.append(main_data['payload'][x][i]['x'])


t = x_array.index
xn = x_array

b, a = signal.butter(N=6, Wn=0.05, btype='low', analog=False)
zi = signal.lfilter_zi(b, a)
z, _ = signal.lfilter(b, a, xn, zi=zi*xn[0])
y = signal.filtfilt(b, a, xn)


fig, axes = plt.subplots(3, 1)
plt.subplots_adjust(wspace=0.2, hspace=0.5)
axes[0].plot(xn, 'b', alpha=0.75)
axes[0].set_title("x-axis")

axes[1].plot(z, 'r--')
axes[1].set_title("y-axis")

axes[2].plot(y, 'k')
axes[2].set_title("z-axis")

# plt.figure
# plt.
# plt.
# plt.
# plt.legend(('noisy signal', 'lfilter, once', 'lfilter, twice',
#             'filtfilt'), loc='best')
plt.grid(True)
plt.show()

