import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as plticker
import numpy as np
from datetime import datetime
from sympy import S, symbols, printing

id_df = pd.read_csv('Latest Readings nrf-351358817716837-data-2023-06-12 11_45_31.csv')

# print(id_df)
id_df = id_df.sort_values(by=['Time'])
# print(id_df.head())



# arr = np.array(id_df['rms_z'], np.std(id_df['rms_z']))
#
# quantile = np.quantile(arr, 0.5, axis=1)
#
# print(quantile)
X = np.arange(0, len(id_df['Time'].index), 1)
X_ex = np.arange(0, len(id_df['Time'].index)+2500, 1)
# z_x = np.polyfit(X, 1.8*id_df['rms_x']/9.80665, 20)
# p_x = np.poly1d(z_x)
#
# z_y = np.polyfit(X, 1.8*id_df['rms_y']/9.80665, 20)
# p_y = np.poly1d(z_y)
#
z_z = np.polyfit(X, 1*id_df['rms_z']/9.80665, 3)
p_z = np.poly1d(z_z)
print(p_z)

# x = symbols("x")
# poly = sum(S("{:6.2f}".format(v))*x**i for i, v in enumerate(p_z[::-1]))
# eq_latex = printing.latex(poly)

fig, axes = plt.subplots(1, 1, sharex='all', figsize=(7, 5.6))
plt.subplots_adjust(wspace=0.2, hspace=0.305, top=0.89, bottom=0.15, left=0.05, right=0.93)
# axes[0].scatter(id_df['Time'], id_df['rms_x']/9.80665, marker=".", alpha=.5)
# # axes[0].plot(X, p_x(X))
# axes[0].set_title("Transversal")
# axes[0].grid(axis='y')
#
# axes[1].scatter(id_df['Time'], id_df['rms_y']/9.80665, color='r', marker=".", alpha=.5)
# # axes[1].plot(X, p_y(X), color='r')
# axes[1].set_title("Longitudinal")
# axes[1].grid(axis='y')

axes.scatter(id_df['Time'], id_df['rms_z']/9.80665, color='b', marker=".", alpha=.5)
axes.plot(id_df['Time'], p_z(X), color='b')
# axes.plot(X_ex, p_z(X_ex), color='r')
axes.set_title("Vertical")
axes.grid(axis='y')
#
major_ticks = np.arange(0, 15512, 1000)
# minor_ticks = np.arange(0, 15512, 500)
#
axes.set_xticks(major_ticks)
# axes[0].set_xlim(2933, 0)
# axes.set_xticks(minor_ticks, minor=True)
#
# axes.scatter(id_df['Time'], id_df['rms_z'], color='b')
plt.xticks(rotation=25)
fig.supxlabel('Time')
fig.supylabel('Acceleration (g)')
fig.suptitle('RMS acceleration from 2022-12-28 to 2023-03-28 at point machine on switch 73W1')
plt.show()
