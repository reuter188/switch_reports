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
import numpy as np

BUCKET_NAME = 'dev-switch-data-coll-bucket'
RMS_BUCKET = 'dev-switch-rms-data-coll'
# KEY = 'measurements/2022/07/07/09/dev-switch-firehose-10-2022-07-07-09-59-41-88bc2375-c0fe-442a-b87e-7b3612ef3f6a'

def rms_value(array):
    n = len(array)
    square = 0.0
    for j in range(0, n):
        square += (array[j] ** 2)
    return math.sqrt(square/float(n))


def download_directory_from_s3(bucket_name, remote_directory_name):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=remote_directory_name):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket.download_file(obj.key, str(obj.key + '.json'))


def build_data_base(bucket, directory, download):
    df = pd.DataFrame()
    if download:
        download_directory_from_s3(bucket, directory)
    search_dir = str(directory)
    for subdir, dirs, files in os.walk(search_dir):
        for file in files:
            filename = os.path.join(subdir, file)
            df = pd.concat([df, pd.read_json(str(filename), lines=True)], ignore_index=True)
    # df['timestamp'].tz_convert('Europe/Berlin')
    return df


def get_devices_on_df(data_frame):
    return data_frame.groupby(['device_id']).count().index


def get_dates_on_df(data_frame):
    dates_df = data_frame.groupby(['timestamp']).count()
    return list(dict.fromkeys(dates_df.groupby(['timestamp']).count().index.strftime('%b/%d')))


def get_dates_for_device(bucket, device, data_frame):
    dates = []
    for i in range(len(data_frame.index)):
        if data_frame['device_id'][i] == device:
            # print(i)
            if bucket == RMS_BUCKET:
                # print(data_frame['ts'][i])
                # print(type(data_frame['ts'][i]))
                if data_frame['ts'][i] != 'NaT' and data_frame['ts'][i] != 'nan' and not np.isnan(data_frame['ts'][i]):
                    dates.append(pd.to_datetime(data_frame['ts'][i], utc=True, unit='ms').strftime('%b/%d'))
            else:
                dates.append(data_frame['timestamp'][i].strftime('%b/%d'))
    dates = list(dict.fromkeys(dates))
    return dates


def get_devices_for_date(date, data_frame):
    devs = []
    timestamp = []
    for j in range(len(data_frame.index)):
        if data_frame['timestamp'][j].strftime('%b/%d') == date:
            devs.append(data_frame['device_id'][j])
            timestamp.append(data_frame['timestamp'][j])
    number_of_devs = len(devs)
    devs = list(dict.fromkeys(devs))
    return devs, number_of_devs, timestamp


def plot_data(device, date, data_frame, report):
    # Creation of lists to contain data. The simple lists are used to calculate the RMS values, the dictionary is used
    # to store and plot raw data.
    x_array = []
    y_array = []
    z_array = []
    # time_array = []
    time_list = []
    event_rms = {'device': device, 'timestamp': [], 'x': [], 'y': [], 'z': []}
    events = {'x': [], 'y': [], 'z': []}

    # The following two commands create a subset of the main data frame containing only the information from the chosen
    # device id
    device_data_frame = data_frame.loc[data_frame['device_id'] == device]
    device_data_frame = device_data_frame.reset_index(drop=True)
    in_between_df = pd.DataFrame()

    ts_list = get_devices_for_date(date, data_frame)

    # This loop removes the rows where the date doesn't match the date specified in the function call.
    for x in range(len(device_data_frame.index)):
        if not ts_list[2].__contains__(device_data_frame['timestamp'][x]):
            device_data_frame = device_data_frame.drop([x])
    device_data_frame = device_data_frame.reset_index(drop=True)
    # print(device_data_frame['timestamp'][0])
    from_zone = tz.tzutc()
    to_zone = tz.gettz('Europe/Berlin')
    first_time = '2022-09-15 17:32:49.932000'
    last_time = '2022-09-15 19:24:50.807000'
    first_ts = datetime.datetime.strptime(first_time, "%Y-%m-%d %H:%M:%S.%f")
    first_ts = first_ts.replace(tzinfo=to_zone)
    last_ts = datetime.datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S.%f")
    last_ts = last_ts.replace(tzinfo=to_zone)

    for i in range(len(device_data_frame.index)):
        sample_time = device_data_frame['timestamp'][i]
        sample_time = sample_time.replace(tzinfo=from_zone)
        sample_time_central = sample_time.astimezone(to_zone)
        if first_ts <= sample_time_central <= last_ts:
            in_between_df = in_between_df.append(device_data_frame.iloc[i], ignore_index=True)
            time_list.append(sample_time_central)
    adjusted_time_list = []
    i = 0
    while i <= len(time_list) - 1:
        for j in range(0, 5):
            adjusted_time_list.append((time_list[i] + datetime.timedelta(seconds=j)).strftime('%H:%M:%S'))
            i += 1
    # print(len(adjusted_time_list))
    # This loop populates the lists with data.
    # (There must be way more efficient ways of doing this, but this works fairly quickly)
    for x in range(len(in_between_df.index)):
        for i in range(len(in_between_df['payload'][x])):
            x_array.append(in_between_df['payload'][x][i]['x'])
            events['x'].append(in_between_df['payload'][x][i]['x'])
            y_array.append(in_between_df['payload'][x][i]['y'])
            events['y'].append(in_between_df['payload'][x][i]['y'])
            z_array.append(in_between_df['payload'][x][i]['z'])
            events['z'].append(in_between_df['payload'][x][i]['z'])
            # The next line creates a continuous "artificial" array of time.
            # time_array.append(device_data_frame['timestamp'][x] + datetime.timedelta(microseconds=i*250))
        event_rms['x'].append(rms_value(x_array))
        event_rms['y'].append(rms_value(y_array))
        event_rms['z'].append(rms_value(z_array))
        event_rms['timestamp'].append(in_between_df['timestamp'])
        x_array = []
        y_array = []
        z_array = []
    rms_df = pd.DataFrame
    # The following commands plots the raw data.
    # print(len(event_rms['x']))
    fig, axes = plt.subplots(3, 1, sharex='all', figsize=(7, 5.6))
    plt.subplots_adjust(wspace=0.2, hspace=0.305, top=0.89, bottom=0.1, left=0.105, right=0.93)
    axes[0].plot(events['x'])
    axes[0].set_title("x-axis")

    axes[1].plot(events['y'], color='r')
    axes[1].set_title("y-axis")

    axes[2].plot(events['z'], color='b')
    axes[2].set_title("z-axis")

    for axis in axes:
        axis.yaxis.set_major_locator(plticker.MultipleLocator(base=50.0))
        axis.set_axisbelow(True)
        axis.grid(axis='y', color='0.75')

    fig.supxlabel('Samples')
    fig.supylabel('Acceleration (m/$s^2$)')
    fig.suptitle(str('Raw acceleration from device ' + device + ' on ' + date))
    # plt.savefig('figuras/Figure_1.pdf', dpi=300)
    plt.savefig('figuras/' + device + '_raw.pdf', dpi=300)
    plt.show()

    # The following commands plots the RMS values bar graphs.
    px = 1 / plt.rcParams['figure.dpi']
    fig, axes = plt.subplots(3, 1, sharex='all', figsize=(7, 5.6))
    plt.subplots_adjust(wspace=0.275, hspace=0.305, top=0.89, bottom=0.205, left=0.105, right=0.93)
    plt.xticks(rotation=90)
    axes[0].bar(adjusted_time_list, event_rms['x'])
    axes[0].set_title("x-axis")

    axes[1].bar(adjusted_time_list, event_rms['y'], color='r')
    axes[1].set_title("y-axis")

    axes[2].bar(adjusted_time_list, event_rms['z'], color='b')
    axes[2].set_title("z-axis")

    for axis in axes:
        axis.yaxis.set_major_locator(plticker.MultipleLocator(base=3.0))
        axis.set_axisbelow(True)
        axis.grid(axis='y', color='0.75')
    # fig.grid()

    fig.supxlabel('Time')
    fig.supylabel('RMS Acceleration (m/$s^2$)')
    fig.suptitle(str('RMS acceleration for ' + device + ' on ' + date + ' for each individual event'))
    # plt.savefig('figuras/Figure_2.pdf')
    plt.savefig('figuras/' + device + '_rms.pdf', dpi=300)
    plt.show()
    events = list(dict.fromkeys(time_list))
    print(len(events))
    # The following commands populate the variables needed for the function generate_report() and call the function
    # if specified.
    if report:
        switches = pd.read_csv('id.csv', index_col=0)
        switch_id = switches['Switch'][device]
        # events = len(ts_list[2])
        report_values = ["{:.2f}".format(max(event_rms['x'])), "{:.2f}".format(max(event_rms['y'])),
                         "{:.2f}".format(max(event_rms['z'])), date, device, switch_id, str(events)]
        print(report_values)
        # generate_report(report_values)


def impact_detection(data):
    sample_counter = 0
    first_sample = 0
    end_sample = 0
    start_flag = False
    end_flag = False
    hit_info = []
    for i in range(len(data['time'])):
        if data['z'][i] > 300.0:
            if not start_flag:
                first_sample = data['time'][i]
                start_flag = True
            sample_counter = sample_counter + 1
        if data['z'][i] < 300.0 and sample_counter > 0:
            end_sample = data['time'][i]
            hit_info.append([sample_counter, first_sample, end_sample])
            sample_counter = 0
            first_sample = 0
            end_sample = 0
            start_flag = False
    print(hit_info)


def plot_data_timestamp(device, timestamp, data_frame, report):
    # Creation of lists to contain data. The simple lists are used to calculate the RMS values, the dictionary is used
    # to store and plot raw data.
    x_array = []
    y_array = []
    z_array = []
    # time_array = []
    time_list = []
    event_rms = {'x': [], 'y': [], 'z': []}
    events = {'time': [], 'x': [], 'y': [], 'z': []}
    event_rms_df = pd.DataFrame()

    # The following two commands create a subset of the main data frame containing only the information from the chosen
    # device id
    device_data_frame = data_frame.loc[data_frame['device_id'] == device]
    device_data_frame = device_data_frame.reset_index(drop=True)

    ts_list = get_devices_for_date(timestamp, data_frame)

    # This loop removes the rows where the date doesn't match the date specified in the function call.
    for x in range(len(device_data_frame.index)):
        if not ts_list[2].__contains__(device_data_frame['timestamp'][x]):
            device_data_frame = device_data_frame.drop([x])
    device_data_frame = device_data_frame.reset_index(drop=True)

    for i in range(len(device_data_frame.index)):
        time_list.append(device_data_frame['timestamp'][i].strftime('%H:%M:%S'))

    d = 0
    # This loop populates the lists with data.
    # (There must be way more efficient ways of doing this, but this works fairly quickly)
    for x in range(len(device_data_frame.index)):
        for i in range(len(device_data_frame['payload'][x])):
            x_array.append(device_data_frame['payload'][x][i]['x'])
            events['x'].append(device_data_frame['payload'][x][i]['x'])
            y_array.append(device_data_frame['payload'][x][i]['y'])
            events['y'].append(device_data_frame['payload'][x][i]['y'])
            z_array.append(device_data_frame['payload'][x][i]['z'])
            events['z'].append(device_data_frame['payload'][x][i]['z'])
            # The next line creates a continuous "artificial" array of time.
            events['time'].append(d/4000)
            d = d + 1
            # time_array.append(device_data_frame['timestamp'][x] + datetime.timedelta(microseconds=i*250))
        event_rms['x'].append(rms_value(x_array))
        event_rms['y'].append(rms_value(y_array))
        event_rms['z'].append(rms_value(z_array))
        x_array = []
        y_array = []
        z_array = []

    z_axis_test = pd.DataFrame(events)
    keys = ['time', 'z']
    # print(z_axis_test.head())
    # z_axis_test[keys].to_csv('data.csv', index=False)
    # impact_detection(events)
    # The following commands plots the raw data.
    fig, axes = plt.subplots(3, 1)
    plt.subplots_adjust(wspace=0.2, hspace=0.5)
    axes[0].plot(events['x'])
    axes[0].set_title("x-axis")

    axes[1].plot(events['y'], color='r')
    axes[1].set_title("y-axis")
    # major_ticks = np.arange(-901, 901, 100)
    # minor_ticks = np.arange(-901, 901, 50)

    # axes.set_xticks(major_ticks)
    # axes.set_xticks(minor_ticks, minor=True)
    # axes[0].set_yticks(major_ticks)
    # axes[0].set_yticks(minor_ticks, minor=True)
    #
    # axes[0].grid(which='both')

    axes[2].plot(events['z'], color='b')
    axes[2].set_title("z-axis")
    for axis in axes:
        # axis.yaxis.set_major_locator(plticker.MultipleLocator(base=5.0))
        axis.set_axisbelow(True)
        axis.grid(axis='y', color='0.75')


    fig.supxlabel('Samples')
    fig.supylabel('Acceleration (m/$s^2$)')
    fig.suptitle(str('Raw acceleration from device ' + device + ' on ' + timestamp))
    plt.savefig('figuras/Figure_1.pdf', dpi=300)
    plt.show()

    # The following commands plots the RMS values bar graphs.
    fig, axes = plt.subplots(3, 1, sharex='all', figsize=(9, 9))
    plt.subplots_adjust(wspace=0.2, hspace=0.5)
    plt.xticks(rotation=90)
    axes[0].bar(time_list, event_rms['x'])
    axes[0].set_title("x-axis")

    axes[1].bar(time_list, event_rms['y'], color='r')
    axes[1].set_title("y-axis")

    axes[2].bar(time_list, event_rms['z'], color='b')
    axes[2].set_title("z-axis")

    fig.supxlabel('Time')
    fig.supylabel('RMS Acceleration (m/$s^2$)')
    fig.suptitle(str('RMS acceleration for ' + device + ' on ' + timestamp + ' for each individual event'))
    plt.savefig('figuras/Figure_2.pdf')
    plt.show()

    events_df = pd.DataFrame(events)
    events_df.to_csv('887_raw_data.csv')

    # The following commands populate the variables needed for the function generate_report() and call the function
    # if specified.
    if report:
        switches = pd.read_csv('id.csv', index_col=0)
        switch_id = switches['Switch'][device]
        events = len(ts_list[2])
        report_values = ["{:.2f}".format(max(event_rms['x'])), "{:.2f}".format(max(event_rms['y'])),
                         "{:.2f}".format(max(event_rms['z'])), timestamp, device, switch_id, str(events)]
        generate_report(report_values)


def generate_report(values):
    place_holders = ['rms_x_avg', 'rms_y_avg', 'rms_z_avg',
                     'date', 'dev_id', 'switch_id', '_events']

    with open('report_template.tex', 'r') as file:
        file_data = file.read()

    for i in range(len(place_holders)):
        file_data = file_data.replace(place_holders[i], values[i])

    with open('report.tex', 'w') as file:
        file.write(file_data)
        file.close()
    os.system("pdflatex report.tex")


def create_page_iterator():
    df = pd.DataFrame()
    client = boto3.client('s3', region_name='eu-central-1')

    paginator = client.get_paginator('list_objects')

    # Create a PageIterator from the Paginator
    operation_parameters = {'Bucket': BUCKET_NAME,
                            'Prefix': 'measurements/2022/07'}
    page_iterator = paginator.paginate(**operation_parameters)
    # page_iterator = paginator.paginate(Bucket=BUCKET_NAME)

    for page in page_iterator:
        contents = page['Contents']
        for s3_obj in contents:
            obj = client.get_object(Bucket=BUCKET_NAME, Key=s3_obj['Key'])
            # df = pd.concat([df, pd.read_json(obj['Body'], lines=True)], ignore_index=True)
            # j = pd.read_json(obj['Body'], lines=True) #json.loads(obj['Body'].read())
            data_str = obj['Body'].read().decode('utf-8')
            print(data_str)
            if data_str.find('nrf-351358817716720') > 0:
                print(data_str)


def get_max_rms():
    df = pd.read_csv('comparison/837-852.csv')
    print(df.head())
    max_values = list()
    max_values.append(max(df['Transversal']))
    max_values.append(max(df['Longitudinal']))
    max_values.append(max(df['Vertical']))
    critical = max_values.index(max(max_values))
    print(max_values)
    print(critical)


# Function to get data from s3 bucket. Use download=True in the first run and everytime a refresh is needed.
# Caution: this function is going to download all files in the bucket and store locally on the same path
# where the script is running.
main_data = build_data_base(BUCKET_NAME, 'measurements/2023/07/05', download=True)
# print(type(main_data['timestamp'][0]))
# arr = np.arange(start=0, stop=main_data.shape[0], step=0.00015625, dtype=float)
# z_axis_df = pd.concat([main_data, arr], axis=1)
# main_data.to_csv('test.csv', columns=[])
# main_data = pd.read_csv('data_frame.csv', infer_datetime_format=True)
# main_data.to_csv('data_frame.csv', index=False)
# # The file id.csv contains all device IDs for the devices installed
test_device = 'nrf-350916066832695'
# # The following formate of date is required to use the functions
test_date = 'Jul/05'

installed = pd.read_csv('new_dep.csv', index_col=0)
field_devices = installed.groupby(['id']).count().index
#
# Print the list of devices that sent data and the dates when this happened
id_list = get_devices_on_df(main_data)
for devices in range(len(id_list)):
    if field_devices.__contains__(id_list[devices]):
        print(id_list[devices] + ' ' + str(get_dates_for_device(BUCKET_NAME, id_list[devices], main_data)))

# Print a list of devices that sent data at least once for the given date: test_date
# print(get_devices_for_date(test_date, main_data))

# Print a list of dates when there was data sent from the given device: test_device
# print(dates)
# total = 0
# for devices in field_devices:
#     dates = get_dates_for_device(devices, main_data)
#     for date in dates:
#         total += len(get_devices_for_date(date, main_data)[2])
#     print(total)
#     total = 0


# Plot for test_device and test_date. To generate report, use variable report=True
# plot_data_timestamp(test_device, test_date, main_data, report=False)

# for i in range(1, 7):
#     test_date = 'Nov/1' + str(i)

# devices = pd.read_csv('db.csv', index_col=0)
# data_frame = pd.read_csv('data_frame.csv', index_col=0)
# rms_df = pd.DataFrame()
# print(data_frame.index)
# for device in data_frame.index:
#     if device in devices['device']:






