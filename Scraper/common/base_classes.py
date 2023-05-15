from abc import ABC, abstractmethod
import os
import pandas as pd

STATUS_UNFINISHED = "unfinished"
STATUS_FINISHED = "finished"


class ScrapeContentExtractor(ABC):
    """
    Combines the contents of all files in a folder into one and transforms the data into tabular format
    Input: folder with data files (e.g. .json)
    Output: Pandas dataframe
    The type of the data files as well as how the file content is transformed can be defined in _extract_one

    An extractor implementing this interface is specific to a Crowdtangle platform (instagram,facebook,reddit,twitter) and query type (/posts,/posts/search,/leaderboard,/links)

    """

    def extract_all(self, data_folder) -> pd.DataFrame:
        result_dfs = []
        for fname in os.listdir(data_folder):
            fpath = os.path.join(data_folder, fname)
            result_df = self._extract_one(fpath)  # transformation into tabular format happens here
            result_dfs.append(result_df)
        combined_df = pd.concat(result_dfs, axis=0)
        return combined_df

    # DO: override this with the actual functionality
    @abstractmethod
    def _extract_one(self, fpath) -> pd.DataFrame:
        """
        Takes one file and reads its contents into tabular format
        """
        pass


class Scraper(ABC):

    def __init__(self, scrape_folder):
        """
        :param scrape_folder: The folder to save the scrape results and config in (must be empty)
        :param result_extractor (type ScrapeContentExtractor): extractor for combining scrape results
        """
        self.scrape_folder = scrape_folder
        os.makedirs(self.scrape_folder, exist_ok=True)

        self.data_folder = os.path.join(self.scrape_folder, "data")
        os.makedirs(self.data_folder, exist_ok=True)

        self.config = None

    @abstractmethod
    def scrape(self, *args, **kwargs):
        pass

    @abstractmethod
    def combine_scrape_results(self, result_extractor: ScrapeContentExtractor, skip_if_exists=True, *args, **kwargs):
        """
        Combines the raw scrape results (e.g. json) into a pandas dataframe
        :param result_extractor (type ScrapeContentExtractor): extractor for combining scrape results
        :param skip_if_exists: skip extraction if the result files already exist on disk
        """
        pass

    @abstractmethod
    def _read_config(self):
        pass

    @abstractmethod
    def _save_config(self):
        pass

    @abstractmethod
    def get_scrape_status(self):
        pass

    def is_finished(self):
        return self.get_scrape_status() == STATUS_FINISHED
