import pandas as pd
import numpy as np
from ast import literal_eval
import os
import Exploration.plotting as pl
import seaborn as sns
import matplotlib.pyplot as plt
from pandas.api.types import is_numeric_dtype

sns.set(rc={'figure.figsize': (20, 6)})
sns.set(font_scale=1.5)


def summarize(df, do_print=True, fpath=None, head=True, values=True):
    """
    Summarizes basic info abou the dataframe and prints or saves them to file
    :param df: dataframe
    :param do_print: print the results
    :param fpath: if given, output will be saved at this path
    :param head: if true, include the dataframe head() in the summary
    :param values: if true, summarize the values in each column
    """

    s = "# Summary"
    s += "\n\n## Shape\nObservations: {}\nFeatures: {}".format(df.shape[0], df.shape[1])
    s += "\nColumns:\n{}".format(list(df.columns))

    if head:
        s += "\n\n## Head\n {}".format(df.head().to_string())

    s += "\n\n## Describe\n{}".format(df.describe().round(1).to_string())

    if values:
        s += "\n\n## Values"
        s += "\nColumn         Values        Missing  Unique Values"
        s += "\n-----------------------"
        for column in df.columns:
            missing = df[column].isna().sum()
            values = len(df) - missing
            unique = df[column].apply(
                str).unique()  # converting everything to string so we can do unique() on iterables like lists etc.
            unique_sorted = sorted(unique)
            unique_str = "({} unique values)".format(len(unique)) if len(unique) > 20 else str(unique_sorted)
            s += "\n{} {} \t\t {} \t\t {}".format(column.ljust(15), values, missing, unique_str)

    if do_print:
        print(s)
    if fpath:
        os.makedirs(os.path.dirname(fpath), exist_ok=True)
        with open(fpath, 'w') as f:
            print(s, file=f)


def analyze_instagram_dataset(input_path, output_folder, skip_if_exists=False):
    """
    Does basic summary and plotting of the values in a  dataset as output by Pipeline.Preprocessing.Preprocessor
    :param input_path: path to a csv file
    :param output_folder: path to the folder where the output will be stored
    :param skip_if_exists: skip plots that already exist on disk
    """
    if os.path.exists(output_folder) and skip_if_exists:
        print("Output already exists. Skipping. Output at {}".format(output_folder))
        return

    os.makedirs(output_folder, exist_ok=True)

    converters = {"timestamp": pd.to_datetime, "hashtags": literal_eval,
                  "caption_en": lambda x: "" if pd.isna(x) else str(x)}
    df = pd.read_csv(input_path, index_col="timestamp", converters=converters)

    save_path = os.path.join(output_folder, "{}")

    # Stats summary
    summarize(df, do_print=False, fpath=save_path.format("summary.txt"), head=False)

    # # Histograms for all columns (not very useful without specifying the bin size manually)
    # hist_fpath = os.path.join(result_path, "Summary", casestudy + "_hist.png")
    # df.hist(figsize=(20, 15))
    # plt.savefig(hist_fpath)
    # plt.plot()

    # Timeseries histogram of post count
    for step, s, label_freq in [["week", "W", 8], ["month", "M", 4], ["quarter", "Q", 1], ["year", "Y", 1]]:
        pl.plot_timeseries_histogram(df["likes"], freq=s, aggregate="count", title="Post Frequency", xlabel=step, ylabel="Number of posts",
                                     fpath=save_path.format("posts_per_{}.png".format(step)), label_every=label_freq)

    # Hashtag frequency
    freqs = pd.Series(np.concatenate(np.array(df["hashtags"]))).value_counts().rename("Occurrence")
    freqs.to_csv(save_path.format("hashtag_freqs.csv"), index=True)

    # Likes histogram
    plt.figure()
    df["likes"].hist(bins=30)
    plt.savefig(save_path.format("hist_likes.png"))

    # Comments histogram
    plt.figure()
    df["comment_count"].hist(bins=30)
    plt.savefig(save_path.format("hist_comments.png"))

    # Correlation
    df_numeric = df.select_dtypes(include=['int16', 'int32', 'int64', 'float16', 'float32', 'float64'])
    df_numeric = df_numeric[df_numeric.columns[~df_numeric.isnull().all()]]  # remove full NA columns (df.notnull() seems to not work)
    pl.plot_correlation(df_numeric, fpath=save_path.format("corr.png"), cmap="YlGnBu")

    plt.figure()
    sns.countplot(data=df["comments_disabled"])
    plt.savefig(save_path.format("comments_disabled.png"))

    print("Saved output to {}".format(output_folder))