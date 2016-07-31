'''
This module is used to create .png plots from the data created by wiki_stats.py.
plotter.py is run separately from wiki_states.py.
'''

import pandas as pd;
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns


def make_time_lambdas():
    '''
    Creates lambdas for converting individual year, month,
     day, hour columns to a combined datetime objects.

    Returns:
        :return: hour_lambda, day_lambda, month_lambda, year_lambda
    '''
    hour_lambda = lambda row: datetime(int(row['year']), int(row['month']),
                                       int(row['day']), int(row['hour']))
    day_lambda = lambda row: datetime(int(row['year']), int(row['month']),
                                      int(row['day']))
    month_lambda = lambda row: datetime(int(row['year']), int(row['month']), 1)
    year_lambda = lambda row: datetime(int(row['year']), 01, 01)
    return hour_lambda, day_lambda, month_lambda, year_lambda


def make_plot_file(top, file_name, time_func, rank_name, rank_max):
    """
    Creates a scatter plot of 'pageviews' depending on time
    with a combination of 'pagename' and 'projectcode' as the keys to plot.

    :param top: Pandas dataframe with pages ranked within each occurance of the current timeframe.
    :param file_name: The name of the png plot file to create.
    :param time_func: The lambda to create the time data.
    :param rank_name: The column with the rank.
    :param rank_max: The highest (least 'pageviews') rank to include; exclusive.
    :return:
    """
    #Get only the rows with rank less than rank_max
    top = top[top[rank_name] < rank_max].copy(deep=True)
    #Create time column
    top['time'] = top.apply(time_func, axis=1)
    #Replace blank page names with "main page"
    top['pagename'] = top['pagename'].fillna("main page")
    #Create key for the legend and plotting.
    top['pagename_projectcode'] = top['pagename']+"_"+top['projectcode']
    #Create and configure seaborn plot.
    g = sns.swarmplot(x="time", y="pageviews", hue="pagename_projectcode", data=top);
    lgd = plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
    plt.xticks(rotation=45)
    fig = plt.figure(1)
    #Create file
    fig.savefig(file_name, bbox_extra_artists=(lgd,), bbox_inches='tight')
    #Clear plot data
    plt.clf()


#Read csv files into Pandas dataframes
top_hourly = pd.read_csv('results/hourlyResult.csv', encoding="utf-8-sig")
top_daily = pd.read_csv('results/dailyResult.csv', encoding="utf-8-sig")
top_monthly = pd.read_csv('results/monthlyResult.csv', encoding="utf-8-sig")
top_yearly = pd.read_csv('results/yearlyResult.csv', encoding="utf-8-sig")

#Create functions to make a time column
hour_lambda, day_lambda, month_lambda, year_lambda = make_time_lambdas()

#Create visualizations
make_plot_file(top_hourly,"hourly", hour_lambda, "hour_rank", 26)
make_plot_file(top_daily,"daily", day_lambda, "day_rank", 26)
make_plot_file(top_monthly,"monthly", month_lambda, "month_rank", 26)
make_plot_file(top_yearly,"yearly", year_lambda, "year_rank", 26)



