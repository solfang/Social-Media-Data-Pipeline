import pandas as pd
import os
import numpy as np
from ast import literal_eval
from dataclasses import dataclass
import swifter  # Using swifter for faster processing: # https://stackoverflow.com/questions/45545110/make-pandas-dataframe-apply-use-all-cores
from tqdm import tqdm
import re

tqdm.pandas()  # makes .progress_apply() available


def apply_filter(df, func, *args, verbose=False) -> pd.DataFrame:
    """
    Applies a given filter function to a dataframe and prints the length of the dataframe before and after the operation if verbose=True
    """
    len_before = len(df)
    df = func(df, *args)
    if verbose:
        print("Filter:", func.__name__, *args, "| Before: {} After: {}".format(len_before, len(df)))
    return df


class CTPreprocessor:
    """
    Raw Crowdtangle API output (nested json that has been flattened once into a table) to table conversion
    Only pre-processing the data I need right now, may extend later.
    """

    def __init__(self, input_path, output_path, skip_if_exists):
        self.input_path = input_path
        self.output_path = output_path
        self.skip_if_exists = skip_if_exists

    def run(self):
        # Skip if the output file already exists (and skipping is allowed in the config)
        if self.skip_if_exists and os.path.exists(self.output_path):
            print("Output file already exists. Skipping. Output file at {}".format(self.output_path))
            return

        converters = {"date": pd.to_datetime, "statistics": literal_eval, "account": literal_eval}
        df = pd.read_csv(self.input_path, converters=converters, index_col="platformId")

        # statistics: {'actual': {'favoriteCount': 92, 'commentCount': 2}, 'expected': {'favoriteCount': 650, 'commentCount': 8}}
        df["likes"] = df["statistics"].apply(lambda x: x["actual"]["favoriteCount"])
        df["comment_count"] = df["statistics"].apply(lambda x: x["actual"]["commentCount"])
        df["likes_expected"] = df["statistics"].apply(lambda x: x["expected"]["favoriteCount"])
        df["comment_count_expected"] = df["statistics"].apply(lambda x: x["expected"]["commentCount"])

        # account: {'id': 2446551, 'name': 'B.E Architecture', 'handle': 'b.e_architecture', 'profileImage': 'https://scontent-sea1-1.cdninstagram.com/v/t51.2885-19/94168185_2719960098129819_8265620194139832320_n.jpg?stp=dst-jpg_s150x150&_nc_cat=103&ccb=1-7&_nc_sid=8ae9d6&_nc_ohc=gOMkvOtkruUAX9T8Qsy&_nc_ht=scontent-sea1-1.cdninstagram.com&oh=00_AfB2I-L_2-GAWLCNEVaWYostkRrDild41Hc3O8gTVlepMg&oe=6404DD48', 'subscriberCount': 116366, 'url': 'https://www.instagram.com/b.e_architecture/', 'platform': 'Instagram', 'platformId': '1559115180', 'verified': False}
        df["owner_id"] = df["account"].apply(lambda x: x["id"])

        # extract hashtags from the post description
        df["hashtags"] = df["description"].apply(lambda x: re.findall(r"#(\w+)", x))

        df.to_csv(self.output_path, index=True)
        print("Output table saved to {}".format(self.output_path))


class Preprocessor:
    """
    Reads a dataframe, pre-processes it and outputs a dataframe.
    """

    def __init__(self, input_path, output_path, dataset_name, remove_duplicates: bool, images_only: bool, year_filter, hashtag_filter_include,
                 hashtag_filter_exclude, max_images_per_year, lowercase_hashtags: bool, skip_if_exists=False):
        """
        :param input_path: input file path
        :param output_path: output file path (file should be a csv)
        :param dataset_name: name of the processed dataset, used for printing
        :param remove_duplicates: remove duplicate posts
        :param images_only: filter out videos
        :param year_filter: tuple of (min year, max year)
        :param hashtag_filter_include: list of hashtags to filter by (filter out posts that have none of these hashtags)
        :param hashtag_filter_exclude: list of hashtags to filter by (filter out posts that have any of these hashtags)
        :param max_images_per_year: if a given year has more posts (=images) than max_images_per_year, randomly draw max_images_per_year
        :param lowercase_hashtags: convert all hashtags to lowercase
        :param skip_if_exists: skip the pre-processing pipeline if the output file already exists
        """
        self.input_path = input_path
        self.output_path = output_path
        self.output_folder = os.path.dirname(output_path)
        os.makedirs(self.output_folder, exist_ok=True)
        self.dataset_name = dataset_name
        self.remove_duplicates = remove_duplicates
        self.images_only = images_only
        self.year_filter = year_filter
        self.hashtag_filter_include = hashtag_filter_include
        self.hashtag_filter_exclude = hashtag_filter_exclude
        self.max_images_per_year = max_images_per_year
        self.lowercase_hashtags = lowercase_hashtags
        self.skip_if_exists = skip_if_exists

    def run(self):
        """
        Applies a table conversion as given by the config
        Most of the columns are hardcoded so it only works as-is with the type of data output by the Scraper.RapidAPI.InstagramFeedScraper
        To pre-process a different data format please specify an alternative run() method
        """

        # Skip if the output file already exists (and skipping is allowed in the config)
        if self.skip_if_exists and os.path.exists(self.output_path):
            print("Output file already exists. Skipping. Output file at {}".format(self.output_path))
            return

        # Read input df
        converters = {"timestamp": pd.to_datetime, "hashtags": literal_eval}
        df = pd.read_csv(self.input_path, converters=converters, index_col="id")

        df["scrape_image"] = True

        df = self.column_stuff(df, self.dataset_name)  # highly specific to the case study datasets, am too lazy to add this properly

        # Apply pre-processing
        if self.remove_duplicates is not None:
            df = apply_filter(df, self.do_remove_duplicates, "shortcode", verbose=True)
        if self.images_only:
            df = apply_filter(df, self.filter_by_condition, "is_video", False, verbose=True)
        if len(self.year_filter) == 2:
            df = apply_filter(df, self.filter_by_year, "timestamp", *self.year_filter, verbose=True)
        if self.lowercase_hashtags:
            df = self.do_lowercase_hashtags(df, "hashtags")
        if len(self.hashtag_filter_include):
            df = apply_filter(df, self.filter_by_hashtag_includes, "hashtags", self.hashtag_filter_include, verbose=True)
        if len(self.hashtag_filter_exclude):
            df = apply_filter(df, self.filter_by_hashtag_excludes, "hashtags", self.hashtag_filter_exclude, verbose=True)

        if self.max_images_per_year != -1:
            df = self.select_n_images_per_year(df, "timestamp", self.max_images_per_year)

        df.to_csv(self.output_path, index=True)
        print("Output table saved to {}".format(self.output_path))

    def column_stuff(self, df, casestudy):
        """
        buncha stuff that didn't fit anywhere else cause it's so specific to the architecture datasets
        """
        df["image"] = df.index.astype(str) + "_" + df["shortcode"] + ".jpg"

        # add columns for the city and name of the case study
        if "_" in casestudy:
            df["city"] = casestudy.split("_")[0]
            df["building"] = casestudy.split("_")[1]
        else:
            df["city"] = None
            df["building"] = None

        # convenience handles for the post date
        df["year"] = df["timestamp"].dt.year
        df["month"] = df["timestamp"].dt.month
        df["day"] = df["timestamp"].dt.day

        df["interactions"] = df["likes"] + df["comment_count"]

        # re-order columns
        df = df.loc[:,
             ["city", "building", "year", "month", "day", "timestamp", "owner_id", "shortcode", "post_url", "type", "is_video", "interactions",
              "likes", "comment_count", "comments_disabled", "caption", "hashtags", "mentions", "image", "thumbnail_src", "scrape_image"]]
        return df

    def do_remove_duplicates(self, df, column) -> pd.DataFrame:
        """
        :param df: dataframe to filter
        :param column: column for filtering out duplicates
        :return: filtered dataframe
        """
        return df.drop_duplicates(subset=[column])

    def filter_by_condition(self, df, column, condition_value):
        return df[df[column] == condition_value]

    def filter_by_year(self, df, timestamp_col, min_year, max_year) -> pd.DataFrame:
        """
        :param df: dataframe to filter
        :param timestamp_col: name of the column of the post timestamp
        :param min_year: including
        :param max_year: excluding
        :return: filtered dataframe
        """
        return df[(df[timestamp_col].dt.year >= min_year) & (df[timestamp_col].dt.year < max_year)]

    def filter_by_hashtag_includes(self, df, hashtag_col, hashtags) -> pd.DataFrame:
        """
        Filters df so that only rows which include at least 1 hashtag from 'hashtags' are kept
        Make sure that hashtags were lowercased in a pipeline step beforehand
        :param hashtags: list of hashtags, e.g. ['#kelvingrove', '#kelvingrovegallery']
        """
        return df[df[hashtag_col].apply(lambda x: len(set(x).intersection(set(hashtags))) > 0)]

    def filter_by_hashtag_excludes(self, df, hashtag_col, hashtags) -> pd.DataFrame:
        """
        Filters df so that only rows which include don't include any hashtag from 'hashtags' are kept
        Make sure that hashtags were lowercased in a pipeline step beforehand
        :param hashtags: list of hashtags, e.g. ['#kelvingrove', '#kelvingrovegallery']
        """
        return df[df[hashtag_col].apply(lambda x: len(set(x).intersection(set(hashtags))) == 0)]

    def select_n_images_per_year(self, df, timestamp_col, n_images) -> pd.DataFrame:
        """
        Randomly selects a maximum of n images per year in the dataset to be scraped.
        E.g.
         - if n=1000 and there are 500 images from 2014, all of them get marked as to be scraped
         - if n=1000 and there are 1500 images from 2015, 1000 of them are randomly selected to be scraped
        Method works in-place by modifying the "scrape_image" column
        """
        indices_sel = np.array([])
        for year in df[timestamp_col].dt.year.unique():
            indices = df[df[timestamp_col].dt.year == year].index
            # if n>number of images present: random draw
            if len(indices) > n_images:
                indices_sel = np.concatenate([indices_sel, np.random.choice(indices, n_images, replace=False)])
            # if n<number of images present: all images are taken
            else:
                indices_sel = np.concatenate([indices_sel, indices])
        # scrape_image is set to true by default, set to false for not selected images
        df.loc[~df.index.isin(indices_sel), "scrape_image"] = False
        return df

    def do_lowercase_hashtags(self, df, hashtag_col) -> pd.DataFrame:
        df[hashtag_col] = df[hashtag_col].apply(lambda htags: [htag.lower() for htag in htags])
        return df
