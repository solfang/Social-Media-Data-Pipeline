import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.ndimage.filters import gaussian_filter1d  # ysmoothed = gaussian_filter1d(y, sigma=2)
import numpy as np
import os
import seaborn as sn


def smooth_data(data_arr, smooth_factor=3):
    return gaussian_filter1d(data_arr, sigma=smooth_factor)


def plot_correlation(df, cmap="Reds", fpath=None):
    """
    Creates and shows a correlation plot
    :param df: pandas dataframe
    """
    plt.figure()
    corr = df.corr()
    sn.heatmap(corr, annot=True, cmap=cmap)  # other decebt cmaps: YlGnBu OrRd Blues
    if fpath:
        plt.tight_layout()  # to avoid text getting cut off
        plt.savefig(fpath)
    else:
        plt.show()


def get_timeseries_labels(grouped_agg, freq, label_string=""):
    """
    label_string: choose maunal label date format using https://pandas.pydata.org/docs/reference/api/pandas.Period.strftime.html
    """

    if label_string == "":
        if freq == "Y":
            label_string = "%Y"
        elif freq == "Q":
            label_string = '%Y %b'  ### %q (quarter) broken for some reason
        elif freq == "M":
            label_string = "%Y %b"  # %b = month name abbreviated
        elif freq == "W":
            label_string = "%Y %b %W"
        elif freq == "D":
            label_string = "%Y %b %d"

    l = [el.to_pydatetime().strftime(label_string) for el in list(grouped_agg.index)]
    return l


def group_and_aggregate(data, grouper, aggregate):
    """

    :param data: pandas series or df
    :param grouper: pd.Grouper or column string or list of any of them
    :param aggregate: one of ["count", "sum", "mean", "median"]
    :return: grouped and aggregated data
    """
    grouped = data.groupby(grouper)
    if aggregate == "count":
        grouped_agg = grouped.count()
    elif aggregate == "sum":
        grouped_agg = grouped.sum()
    elif aggregate == "mean":
        grouped_agg = grouped.mean()
    elif aggregate == "median":
        grouped_agg = grouped.median()
    return grouped_agg


def plot_timeseries_histogram(series: pd.Series, freq="M", aggregate="count", xlabel="", ylabel="", title="",
                              label_every=1, label_string="", trendline=False, fpath=None):
    """
    Plots a bar chart of data over time. Handles nicely formatting dates on the X-axis.
    :param series: series of data with datetime index
    :param freq: frequency to group the data by, has to be in ["Y", "Q", "M", "W", "D", "DwY"]
    :param aggregate: aggregate function, has to be in ["count", "sum", "mean", "median"]
    :param xlabel: plot xlabel
    :param ylabel: plot ylabel
    :param title: plot title
    :param label_every: default=n=1, label the x axis every n ticks
    :param label_string: manual label string for the xaxis, used by strftime
    :param trendline: Whether to add a trendline to the plot, default=False
    :param fpath: if not None, save the aggregated data and plot to this file
    :return: pyplot axis object
    """
    assert aggregate in ["count", "sum", "mean", "median"]
    assert freq in ["Y", "Q", "M", "W", "D", "DwY"]  # DwY = omit year in label

    grouped_agg = group_and_aggregate(series, pd.Grouper(freq=freq), aggregate)
    l = get_timeseries_labels(grouped_agg, freq, label_string)
    y = list(grouped_agg)

    plt.figure()
    ax = sns.barplot(x=l, y=y, color="royalblue")

    if trendline:
        ysmoothed = smooth_data(y, smooth_factor=3)
        plt.plot(l, ysmoothed, c="tomato", linewidth=3)

    if title:
        plt.title(title, fontsize=30)
    if xlabel:
        plt.xlabel(xlabel, fontsize=20)
    if ylabel:
        plt.ylabel(ylabel, fontsize=20)
    plt.xticks(rotation=90)
    ax.xaxis.set_tick_params(labelsize='large')

    # only show every n-th (n=label_every) label
    for idx, label in enumerate(ax.xaxis.get_ticklabels()):
        if idx % label_every != 0:
            label.set_visible(False)

    if fpath:
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        # pd.DataFrame(data=[l, y]).T.to_csv(output_fname + ".csv", index=False)
        plt.tight_layout()  # to avoid text getting cut off
        plt.savefig(fpath)

    return ax


def dualplot_timeseries(ser1: pd.Series, ser2: pd.Series, freq="M", agg1="count", agg2="mean", label1="", label2="",
                        title="", ylabel1="", ylabel2="", label_string="", label_every=1,
                        fpath=None):
    """
    Plots aggregated data of two variables over time.
    :param ser1: series of data with datetime index
    :param ser2: series of data with datetime index
    :param freq: frequency to group the data by, has to be in ["Y", "Q", "M", "W", "D", "DwY"]
    :param agg1: aggregate function for series1, has to be in ["count", "sum", "mean", "median"]
    :param agg2: aggregate function for series2, has to be in ["count", "sum", "mean", "median"]
    :param label1: plot-label for series1
    :param label2: plot-label for series2
    :param title: plot title
    :param ylabel1: plot ylabel for series1
    :param ylabel2: plot ylabel for series2
    :param label_string: manual label string for series1 for the xaxis, used by strftime
    :param label_every: manual label string for series2 for the xaxis, used by strftime
    :param fpath: if not None, save the aggregated data and plot to this file
    :return: pyplot figure
    """

    agg1 = group_and_aggregate(ser1, pd.Grouper(freq=freq), agg1)
    agg2 = group_and_aggregate(ser2, pd.Grouper(freq=freq), agg2)

    # add bins that are present in one aggregate but not in the other
    agg_combined = pd.DataFrame(agg1.T)
    agg_combined = agg_combined.join(agg2, how="outer", lsuffix="a")
    agg_combined = agg_combined.fillna(0)
    agg1 = agg_combined.iloc[:, 0]
    agg2 = agg_combined.iloc[:, 1]

    l = get_timeseries_labels(agg1, freq, label_string)

    fig = plt.figure()
    ax1 = fig.add_subplot(111)

    ax1.plot(l, list(agg1), color="royalblue", label=label1, linewidth=2.5)
    plt.xticks(rotation=90, fontsize=14)  # modifies the 'last' axis. Here, the send axis comes last and is invisible.
    plt.yticks(fontsize=14)
    ax2 = plt.twinx()
    ax2.plot(l, list(agg2), color="darkorange", label=label2, linewidth=2.5)
    plt.yticks(fontsize=14)

    if ylabel1:
        ax1.set_ylabel(ylabel1, fontsize=20)
    if ylabel2:
        ax2.set_ylabel(ylabel2, fontsize=20)

    if title:
        plt.title(title, fontsize=30)
    # hide grid of the second plot to avoid overlapping grids
    ax2.grid(None)

    # add dual legend
    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    lines = lines_1 + lines_2
    labels = labels_1 + labels_2
    ax1.legend(lines, labels, loc="upper left", fontsize=14)

    # only show every n-th (n=label_every) label
    for idx, label in enumerate(ax1.xaxis.get_ticklabels()):
        if idx % label_every != 0:
            label.set_visible(False)

    if fpath:
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        pd.DataFrame(data=[l, list(agg1), list(agg2)]).T.to_csv(fpath + ".csv", index=False)
        plt.tight_layout()  # to avoid text getting cut off
        plt.savefig(fpath)


def catplot(df: pd.DataFrame, groupby, freq="M", aggregate="count", xlabel="", ylabel="", title="",
            relative_to_group=False, label_string="", id_variable="id", fpath=None):
    """
    Plots a grouped categorical variable over time
    :param df: dataframe with at least two columns and a datetime index
    :param groupby: the categorical variable by which to group the data
    :param freq: frequency to group the data by, has to be in ["Y", "Q", "M", "W", "D", "DwY"]
    :param aggregate: aggregate function, has to be in ["count", "sum", "mean", "median"]
    :param xlabel: plot xlabel
    :param ylabel: plot ylabel
    :param title: plot title
    :param relative_to_group: divide the data in each bin by the group total (group=one category of the cat. variable)
    :param label_string: manual label string for the xaxis, used by strftime
    :param id_variable: column with identifiers, used for relative_to_group stuff
    :param fpath: if not None, save the aggregated data and plot to this file
    :return: pyplot axis object
    """
    # TODO: add label_every like in timeseries plot (but: 'FacetGrid' object has no attribute 'xaxis')
    # maybe helpful
    # 'FacetGrid' object has no attribute 'xaxis'
    # https://stackoverflow.com/questions/43727278/how-to-set-readable-xticks-in-seaborns-facetgrid

    # id_variable is used to count the observations
    assert aggregate in ["count", "sum", "mean"]
    assert freq in ["Y", "Q", "M", "W", "D", "DwY"]

    # agg_funcs = {"count": pd.DataFrame.count, "sum": pd.DataFrame.sum, "mean": pd.DataFrame.mean}
    # agg_func = agg_funcs[aggregate]

    grouped_agg = group_and_aggregate(df, [pd.Grouper(freq=freq), groupby], aggregate)

    grouped_agg = grouped_agg.reset_index([1])
    l = get_timeseries_labels(grouped_agg, freq, label_string)

    # add xlabel list as column so seaborn can access it
    grouped_agg["Post Date"] = l

    grouped_agg = grouped_agg.rename(columns={id_variable: "Post Count"})

    # optional: correlation
    # piv = grouped_agg.reset_index().pivot(index="Post Date",columns=groupby, values=["Post Count"])
    # print(piv.corr())

    if relative_to_group:
        for cat in grouped_agg[groupby].unique():
            grouped_agg.loc[grouped_agg[groupby] == cat, "Post Count"] /= sum(
                grouped_agg.loc[grouped_agg[groupby] == cat, "Post Count"])

    ax = sns.catplot(x="Post Date", y="Post Count", hue=groupby, kind="point", data=grouped_agg, height=5, aspect=4,
                     palette="bright")

    if title:
        plt.title(title, fontsize=30)
    if xlabel:
        plt.xlabel(xlabel, fontsize=20)
    if ylabel:
        plt.ylabel(ylabel, fontsize=20)
    plt.xticks(rotation=90)
    # ax.xaxis.set_tick_params(labelsize='large')

    # if output_folder:
    #     os.makedirs(output_folder, exist_ok=True)
    #     output_fname = os.path.join(output_folder, "timeseries_catplot_{}".format(groupby))
    #     grouped_agg.to_csv(output_fname + ".csv", index=False)
    #     plt.savefig(output_fname + ".png")
    if fpath:
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        # pd.DataFrame(data=[l, y]).T.to_csv(output_fname + ".csv", index=False)
        plt.tight_layout()  # to avoid text getting cut off
        plt.savefig(fpath)

    return ax


def catplot_area(df: pd.Series, groupby, id_variable, freq="M", aggregate="count", xlabel="", ylabel="", title="",
                 relative_to_group=False, label_string="", positions={},
                 fpath=None, do_print=False):
    """
    Same as catplot but stacks the lines into an areachart.
    :param df: dataframe with at least two columns and a datetime index. To avoid errors only try to include columns that are required by this function
    :param groupby: the categorical variable by which to group the data
    :param freq: frequency to group the data by, has to be in ["Y", "Q", "M", "W", "D", "DwY"]
    :param aggregate: aggregate function, has to be in ["count", "sum", "mean", "median"]
    :param xlabel: plot xlabel
    :param ylabel: plot ylabel
    :param title: plot title
    :param relative_to_group: divide the data in each bin by the group total (group=one category of the cat. variable)
    :param label_string: manual label string for the xaxis, used by strftime
    :param id_variable: column with identifiers, used for relative_to_group stuff
    :param positions: dict of (category, index) for manually sorting the categories in the plot
    :param fpath: if not None, save the aggregated data and plot to this file
    :return: lines: list of PolyCollection
    """
    assert aggregate in ["count", "sum", "mean"]
    assert freq in ["Y", "Q", "M", "W", "D"]

    grouped_agg = group_and_aggregate(df, [pd.Grouper(freq=freq), groupby], aggregate)

    grouped_agg = grouped_agg.reset_index([1])

    # add xlabel list as column so seaborn can access it
    grouped_agg["Post Date"] = grouped_agg.index

    grouped_agg = grouped_agg.rename(columns={id_variable: "Post Count"})

    #  pivot data into form (example):
    #     Group1 Group2 ...
    # Jan 1         2
    # Feb 6         4
    # ...
    piv = grouped_agg.reset_index().pivot(index="Post Date", columns=groupby, values=["Post Count"])

    piv = piv.fillna(0)

    if positions:
        # x[1] since we have a double column index, the first part is 'Post Count' the second part is the label
        piv = piv.reindex(sorted(piv.columns, key=lambda x: positions[x[1]]), axis=1)

    piv.columns = piv.columns.droplevel()  # remove double column index

    if relative_to_group:
        piv = piv.divide(piv.sum(axis=1), axis=0)

    # important to turn dates into printable string only after pivoting, else index gets re-sorted alphabetically which sorts any month names, e.g. (Jan, Aug) alphabetically
    x = get_timeseries_labels(piv, freq, label_string)
    data = piv.values.T  # no idea why I have to transpose. Pyplot docu says the data should be of shape (observationsxgroups), which it is without the .T
    # labels = list(
    #     map(lambda l: l[1], piv.columns))  # column has double index (Post Count, group) we only want the group
    labels = piv.columns

    # if relative_to_group:
    #     data = data / data.sum(axis=0)

    lines = plt.stackplot(x, data, labels=labels)
    plt.legend(loc='upper left', fontsize=15)

    if title:
        plt.title(title, fontsize=30)
    if xlabel:
        plt.xlabel(xlabel, fontsize=20)
    if ylabel:
        plt.ylabel(ylabel, fontsize=20)
    plt.xticks(rotation=90)
    # ax.xaxis.set_tick_params(labelsize='large')

    # if output_folder:
    #     os.makedirs(output_folder, exist_ok=True)
    #     output_fname = os.path.join(output_folder, "timeseries_catplotarea_{}".format(groupby))
    #     pd.DataFrame(data=data.T, index=x, columns=labels).to_csv(output_fname + ".csv", index=False)
    #     plt.savefig(output_fname + ".png")
    if fpath:
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        # pd.DataFrame(data=[l, y]).T.to_csv(output_fname + ".csv", index=False)
        piv.index = x
        plt.tight_layout()  # to avoid text getting cut off
        plt.savefig(fpath)
    if do_print:
        piv.index = x
        if relative_to_group:  # format as %
            for c in piv.columns:
                piv[c] = piv[c].map("{:.1%}".format)
        print(piv)

    return lines


def stacked_barchart(data, relative_to_group=True, title="", xtick_rotation=0, patch_y_offset=-0.06,
                     fpath=None):
    """
    :param data:
    Expected structure: pandas dataframe with:
    index: x-axis of the chart -> grouper1_col
    columns: segments of the bars -> grouper2_col
    values: height of the bars -> value_col

          col1 col2 col3 ...
    ind1  ...
    ind2
    ind3
    ...

    -> To get the data into this shape you may want to use
    1. groupby([grouper1_col, grouper2_col])[value_col].count().reset_index() (make sure to use reset_index)
    2. pivot(index=(grouper1_col, columns=grouper2_col, values=value_col)
    :param relative_to_group: show bars as absolute numbers (False) or relative to group (True)
    :param output_folder: if not None, save the aggregated data and plot into this folder
    :return: pyplot axis object
    """

    # from https://www.pythoncharts.com/matplotlib/stacked-bar-charts-labels/
    # I adapted it a little to work with relative charts

    fig, ax = plt.subplots()

    data_used = data.copy()

    if relative_to_group:
        data_used = data.divide(data.sum(axis=1), axis=0)

    bottom = np.zeros(len(data_used))

    data_used = data_used.fillna(0)

    # plot the bars
    for i, col in enumerate(data_used.columns):
        # print("coldata", data_normalized[col])
        ax.bar(data_used.index, data_used[col], bottom=bottom, label=col)
        bottom += np.array(data_used[col])

    # add totals to top of bars
    totals = data.sum(axis=1)
    total_data = data_used.sum(axis=1)
    y_offset = 0.01
    for i, total in enumerate(totals):
        ax.text(totals.index[i], total_data.iloc[i] + y_offset, round(total), ha='center',
                weight='bold', size=20)

    # remember height of each plotted bar for label inside the bar
    heights = np.array(data_used).ravel(order="F")  # F=column-wise

    # For each patch add a label.
    # negative offset for the annotations inside the bar
    for i, bar in enumerate(ax.patches):
        bar_text = "{:.1%}".format(heights[i]) if heights[i] > 0 else ""  # prevents overlapping tetx from 'empty' bars
        # offset text but only if the bar is high enough for the text to fit
        current_y_offset = patch_y_offset if bar.get_height() > 0.05 else patch_y_offset / 2
        ax.text(
            # Put the text in the middle of each bar. get_x returns the start
            # so we add half the width to get to the middle.
            bar.get_x() + bar.get_width() / 2,
            # Vertically, add the height of the bar to the start of the bar,
            # along with the offset.
            bar.get_height() + bar.get_y() + current_y_offset,
            # This is actual value we'll show.
            bar_text,
            # Center the labels and style them a bit.
            ha='center',
            color='w',
            weight='bold',
            size=15
        )

    if title:
        ax.set_title(title, pad=20, fontdict={"fontsize": 20})
    ax.legend(bbox_to_anchor=(0.06, 1))  # x<=0.6 snaps to left of the chart
    if 1 > xtick_rotation < 90:
        rotation = xtick_rotation
    else:
        rotation = 3 * len(data) if len(data) > 3 else 0  # automatic label rotation, works okay so far
    plt.xticks(rotation=rotation)

    # if output_folder:
    #     os.makedirs(output_folder, exist_ok=True)
    #     output_fname = os.path.join(output_folder, "timeseries_stackedbar")
    #     data.to_csv(output_fname + ".csv", index=True)
    #     plt.savefig(output_fname + ".png")
    if fpath:
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        # pd.DataFrame(data=[l, y]).T.to_csv(output_fname + ".csv", index=False)
        plt.tight_layout()  # to avoid text getting cut off
        plt.savefig(fpath)

    return ax
