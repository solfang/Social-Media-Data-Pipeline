"""
Defines implementations of the pipeline stages.
"""

import os
from abc import ABC, abstractmethod
import pandas as pd

from Scraper.RapidAPI.InstagramFeedScraper import InstagramFeedScraper
from Preprocessing.Preprocessor import Preprocessor, CTPreprocessor
from Preprocessing.Translator import Translator
from Scraper.RapidAPI.InstagramImageScraper import InstagramImageScraper
from Preprocessing.ImageLabeling.ImageLabeler import ImageLabeler
from Preprocessing.FeatureVectors.DIRAdapter import get_features
from Preprocessing.ImageAnonymization.ImageAnonymizer import ImageAnonymizer
from Exploration.ExploratoryAnalysis import analyze_instagram_dataset
import json
import warnings


class Stage(ABC):
    """
    Base class for a pipeline stage
    """

    def __init__(self, root_dir: str, dataset_name: str, params: dict):
        """"
        :param root_dir: path to the folder to store the output(s) in
        :param dataset_name: handle for the dataset (not used by all stages)
        :param params: stage-specific parameters
        """
        self.root_dir = root_dir
        self.dataset_name = dataset_name
        self.params = params

    @abstractmethod
    def run(self, input_path, output_path, skip_if_exists) -> str:
        """"
        Executes the stage: read an input, do something with and produce an output
        :param input_path: input file path
        :param skip_if_exists: skip the stage if the output already exists
        :returns: file path to the output
        """
        pass


class InstagramFeedScraperStage(Stage):

    def run(self, input_path, output_path, skip_if_exists):
        scrape_folder = os.path.join(self.root_dir, "_scrape")  # for storing the scrape data
        dfs = []  # for passing the scrape result paths onto the next stage
        # start a new scrape for each search term
        for search_term in self.params["terms"]:
            scrape_path = os.path.join(scrape_folder, "{}-{}".format(self.params["type"], search_term))
            # read the API key
            api_key_fpath = "Scraper/RapidAPI/api_key.json"
            api_key = "dummy"
            if not os.path.exists(api_key_fpath):
                data = {"API_KEY": "your-api-key"}
                with open("api_key.json", "w") as file:
                    json.dump(data, file)
                warnings.warn(
                    "WARNING: no file for the Instagrams scraper API key exists. I created one under 'Scraper/RapidAPI/api_key.json' to place your API key in (also make sure it's in gitignore)")
            else:
                with open(api_key_fpath, "r") as file:
                    data = json.load(file)
                    api_key = data["API_KEY"]
            # initialize scraper and run the scrape
            scraper = InstagramFeedScraper(scrape_path, api_key, search_term, self.params["type"], max_tries=self.params["max_tries"])
            if not scraper.is_finished():
                scraper.scrape()
            scraper.combine_scrape_results(skip_if_exists=skip_if_exists)
            posts_path = os.path.join(scrape_path, "metadata.csv")
            try:
                posts_df = pd.read_csv(posts_path)
                dfs.append(posts_df)
            except pd.errors.EmptyDataError:
                pass
            scraper.cleanup_data()

        # combine results from different search terms
        if len(dfs):
            df = pd.concat(dfs)
            df.to_csv(output_path, index=False)
        return True


class PreprocessorStage(Stage):

    def run(self, input_path, output_path, skip_if_exists):
        Preprocessor(input_path, output_path, self.dataset_name, **self.params, skip_if_exists=skip_if_exists).run()
        return True


class CTPreprocessorStage(Stage):

    def run(self, input_path, output_path, skip_if_exists):
        CTPreprocessor(input_path, output_path, skip_if_exists=skip_if_exists).run()
        return True


class ExploratoryanalysisStage(Stage):

    def run(self, input_path, output_path, skip_if_exists):
        analyze_instagram_dataset(input_path, output_path, skip_if_exists=skip_if_exists)
        return True


class TranslatorStage(Stage):

    def run(self, input_path, output_path, skip_if_exists):
        Translator(input_path, output_path, self.params["target_column"], self.params["target_language"],
                   skip_if_exists=False).run()  # don't skipp if exists cause there may be a partially translated output file
        return True


class InstagramImageScraperStage(Stage):

    def run(self, input_path, output_path, skip_if_exists):
        # store config etc. in "images"
        # the scraper will then scrape the images into "images/images". Potato logic I know but no idea where else to store the scraping config etc.
        output_path = os.path.dirname(output_path)
        df_in = pd.read_csv(input_path)
        # exclude images that shouldn't be scraped
        if "scrape_image" in df_in.columns:
            df_in = df_in[df_in["scrape_image"] == True]
        posts = [list(df_in["id"]), list(df_in["shortcode"]), list(df_in["thumbnail_src"])]
        # initialize scraper and run the scrape
        scraper = InstagramImageScraper(output_path, posts, sleep_time=0, max_attempts=5, skip_if_exists=skip_if_exists)
        if not scraper.is_finished():
            scraper.scrape()
        return True


class ImageLabelerStage(Stage):

    def run(self, input_path, output_path, skip_if_exists):
        ImageLabeler(input_path, output_path, skip_if_exists=skip_if_exists).run()
        return True


class ImageFeatureVectorStage(Stage):

    def run(self, input_path, output_path, skip_if_exists):
        image_list_file = os.path.join(os.path.dirname(input_path), "image_db.txt")
        get_features(input_path, image_list_file, output_path, self.params["gpu_id"],
                     repo_folder="Preprocessing/FeatureVectors/deep-image-retrieval",
                     skip_if_exists=skip_if_exists)
        return True


class ImageAnonymizerStage(Stage):

    def run(self, input_path, output_path, skip_if_exists):
        ImageAnonymizer(input_path, output_path, self.params["confidence"], in_place=self.params["in_place"],
                        skip_if_exists=skip_if_exists).run()  # consider setting in_place=False
        return True
