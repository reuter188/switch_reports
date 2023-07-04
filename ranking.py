import pandas as pd
import os
import os.path
import matplotlib
import matplotlib.pyplot as plt
import datetime
from dateutil import tz
import numpy as np
import matplotlib.dates as dates

df_list = []
df_id = pd.read_csv('id.csv', index_col=0)
DIR = 'comparison'
th = {'Transversal': 50.0, 'Longitudinal': 90.0, 'Vertical': 130.0}
score = {'Transversal': 0, 'Longitudinal': 0, 'Vertical': 0}
rank_df = pd.DataFrame()
rank_df_axis = pd.DataFrame()
events_score = 0
severity_score = 0
for subdir, dirs, files in os.walk(DIR):
    for file in files:
        filename = os.path.join(subdir, file)
        df_list.append(pd.read_csv(filename))

for df in df_list:
    data = df[["Transversal", "Longitudinal", "Vertical"]]
    for column in data.columns:
        for i in range(len(data.index)):
            if data[column][i] > th[column]:
                events_score += 1
                score[column] += 1
                if data[column][i] / th[column] > 1.5:
                    severity_score += 1
        rank_data = pd.DataFrame(data={'ID': [df['device_id'][0]],
                                       'Total RMS values': [i + 1],
                                       'Events': [events_score],
                                       'Severity': [severity_score],
                                       'Switch ID': [df_id['Switch'][df['device_id'][0]]]})
        # rank_data_axis = pd.DataFrame(data={'ID': [df['device_id'][0]],
        #                                     'Total RMS values': [i + 1],
        #                                     'Above threshold X': [score[]],
        #                                     'Severity': [severity_score],
        #                                     'Switch ID': [df_id['Switch'][df['device_id'][0]]]})
    rank_data_axis = pd.DataFrame(data={'ID': [df['device_id'][0]],
                                        'Total RMS values': [i + 1],
                                        'Above threshold X': [score['Transversal']],
                                        'Above threshold Y': [score['Longitudinal']],
                                        'Above threshold Z': [score['Vertical']],
                                        'Severity': [severity_score],
                                        'Switch ID': [df_id['Switch'][df['device_id'][0]]]})
    for pos in score:
        score[pos] = 0
    events_score = 0
    severity_score = 0
    # rank_df = rank_df.append(rank_data, ignore_index=True)
    rank_df = pd.concat([rank_df, rank_data], ignore_index=True)
    rank_df_axis = pd.concat([rank_df_axis, rank_data_axis], ignore_index=True)
print(rank_df)
rank_df_axis.to_csv('ranking_per_axis.csv', index=False)
print(rank_df_axis)
# df = pd.read_csv('comparison/837_complete.csv')
# i = 0
# adjusted_time_list = []
# time_list = []
# from_zone = tz.tzutc()
# to_zone = tz.gettz('Europe/Berlin')
#
# for i in range(len(df.index)):
#     sample_time = datetime.datetime.strptime(df['time'][i], "%Y-%m-%d %H:%M:%S.%f")
#     sample_time = sample_time.replace(tzinfo=from_zone)
#     sample_time_central = sample_time.astimezone(to_zone)
#     time_list.append(sample_time_central)
#
# print(len(time_list))
# i = 0
# while i <= len(time_list) - 1:
#     for j in range(0, 5):
#         adjusted_time_list.append((time_list[i] + datetime.timedelta(seconds=j)).strftime('%Y-%m-%d %H:%M:%S.%f'))
#         i += 1
#
# # plt.scatter(adjusted_time_list, df['Transversal'])
# x = dates.date2num(adjusted_time_list)
# z = np.polyfit(x, df['Transversal'], 1)
# p = np.poly1d(z)
#
# plt.plot(adjusted_time_list, df['Transversal'])
# plt.xlabel('Date')
# plt.ylabel('Value')
#
# # x_fit = np.linspace(x.min(), x.max())
# # plt.plot(dates.num2date(x_fit), p(x_fit), "r--")
#
# # plt.plot(x, p(x))
# plt.show()
