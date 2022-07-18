import datetime

import boto3
import pandas as pd
import math
import matplotlib.pyplot as plt
import os

BUCKET_NAME = 'dev-switch-data-coll-bucket'
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


def build_data_base(directory, download):
    df = pd.DataFrame()
    if directory == 'measurements/2022/07/':
        if download:
            download_directory_from_s3(BUCKET_NAME, directory)
        search_dir = str(directory)
        for subdir, dirs, files in os.walk(search_dir):
            for file in files:
                filename = os.path.join(subdir, file)
                df = pd.concat([df, pd.read_json(str(filename), lines=True)], ignore_index=True)
    return df


def get_devices_on_df(data_frame):
    return data_frame.groupby(['device_id']).count().index


def get_dates_on_df(data_frame):
    dates_df = data_frame.groupby(['timestamp']).count()
    return list(dict.fromkeys(dates_df.groupby(['timestamp']).count().index.strftime('%b/%d')))


def get_dates_for_device(device, data_frame):
    dates = []
    for i in range(len(data_frame.index)):
        if data_frame['device_id'][i] == device:
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
    time_array = []
    event_rms = {'x': [], 'y': [], 'z': []}
    events = {'x': [], 'y': [], 'z': []}

    # The following two commands create a subset of the main data frame containing only the information from the chosen
    # device id
    device_data_frame = data_frame.loc[data_frame['device_id'] == device]
    device_data_frame = device_data_frame.reset_index(drop=True)

    # Following commands create a time array to separate each individual event.
    ts_list = get_devices_for_date(date, data_frame)
    time = []
    for i in range(len(ts_list[2])):
        time.append(ts_list[2][i].strftime('%H:%M:%S'))

    # This loop removes the rows where the date doesn't match the date specified in the function call.
    for x in range(len(device_data_frame.index)):
        if not ts_list[2].__contains__(device_data_frame['timestamp'][x]):
            device_data_frame = device_data_frame.drop([x])
    device_data_frame = device_data_frame.reset_index(drop=True)

    # This loop populates the lists with data.
    # (There must be way more efficient ways of doing this, but this works fairly quickly)
    for x in range(len(device_data_frame.index)):
        for i in range(len(data_frame['payload'][x])):
            x_array.append(data_frame['payload'][x][i]['x'])
            events['x'].append(data_frame['payload'][x][i]['x'])
            y_array.append(data_frame['payload'][x][i]['y'])
            events['y'].append(data_frame['payload'][x][i]['y'])
            z_array.append(data_frame['payload'][x][i]['z'])
            events['z'].append(data_frame['payload'][x][i]['z'])
            # The next line creates a continuous "artificial" array of time.
            time_array.append(device_data_frame['timestamp'][x] + datetime.timedelta(microseconds=i*250))
        event_rms['x'].append(rms_value(x_array))
        event_rms['y'].append(rms_value(y_array))
        event_rms['z'].append(rms_value(z_array))
        x_array = []
        y_array = []
        z_array = []

    # The following commands plots the raw data.
    fig, axes = plt.subplots(3, 1)
    plt.subplots_adjust(wspace=0.2, hspace=0.5)
    axes[0].plot(events['x'])
    axes[0].set_title("x-axis")

    axes[1].plot(events['y'], color='r')
    axes[1].set_title("y-axis")

    axes[2].plot(events['z'], color='b')
    axes[2].set_title("z-axis")

    fig.supxlabel('Samples')
    fig.supylabel('Acceleration (m/$s^2$)')
    fig.suptitle(str('Raw acceleration from device ' + device + ' on ' + date))
    plt.savefig('figuras/Figure_1.pdf', dpi=300)
    plt.show()

    # The following commands plots the RMS values bar graphs.
    fig, axes = plt.subplots(3, 1, sharex=True, figsize=(9, 9))
    plt.subplots_adjust(wspace=0.2, hspace=0.5)
    plt.xticks(rotation=45)
    axes[0].bar(time, event_rms['x'])
    axes[0].set_title("x-axis")

    axes[1].bar(time, event_rms['y'], color='r')
    axes[1].set_title("y-axis")

    axes[2].bar(time, event_rms['z'], color='b')
    axes[2].set_title("z-axis")

    fig.supxlabel('Time')
    fig.supylabel('RMS Acceleration (m/$s^2$)')
    fig.suptitle(str('RMS acceleration for ' + device + ' on ' + date + ' for each individual event'))
    plt.savefig('figuras/Figure_2.pdf')
    plt.show()

    # The following commands populate the variables needed for the function generate_report() and call the function
    # if specified.
    if report:
        switches = pd.read_csv('id.csv', index_col=0)
        switch_id = switches['Switch'][device]
        events = len(ts_list[2])
        report_values = ["{:.2f}".format(max(event_rms['x'])), "{:.2f}".format(max(event_rms['y'])),
                         "{:.2f}".format(max(event_rms['z'])), date, device, switch_id, str(events)]
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


# Function to get data from s3 bucket. Use download=True in the first run and everytime a refresh is needed.
# Caution: this function is going to download all files in the bucket and store locally on the same path
# where the script is running.
main_data = build_data_base('measurements/2022/07/', download=False)

# The file id.csv contains all device IDs for the devices installed
test_device = 'nrf-351358817717033'
# The following formate of date is required to use the functions
test_date = 'Jul/15'

# Print the list of devices that sent data and the dates when this happened
# id_list = get_devices_on_df(main_data)
# for devices in range(len(id_list)):
#     print(id_list[devices] + ' ' + str(get_dates_for_device(id_list[devices], main_data)))

# Print a list of devices that sent data at least once for the given date: test_date
# print(get_devices_for_date(test_date, main_data))

# Print a list of dates when there was data sent from the given device: test_device
# print(get_dates_for_device(test_device, main_data))

# Plot for test_device and test_date. To generate report, use variable report=True
plot_data(test_device, test_date, main_data, report=True)
