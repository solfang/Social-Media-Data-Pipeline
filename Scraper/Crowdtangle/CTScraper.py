import os
import time
from tqdm import tqdm
import pandas as pd

import sys

sys.path.append('..')
from common.util import read_json, save_json
from common.base_classes import Scraper, ScrapeContentExtractor
from common.base_classes import STATUS_UNFINISHED, STATUS_FINISHED
from CTConfig import CTConfig
from one_query import get_hitcount
from pytangle.api import API  # https://github.com/hide-ous/pytangle


class CTScraper(Scraper):
    """
    Scrapes data from Crowdtangle for given search parameters.

    The scrape output (jsons) will be stored at [scrape_folder]/data/
    When the scrape is done, a selection of the scrape outputs will be saved in tabular format at
    [scrape_folder]/result/result.csv and [scrape_folder]/result/result_lean.csv


    Folder structure:
    - [scrape_folder]/
        - data/
            ...
        - result/
            result.csv
            result_lean.csv
        - scraping_config.json
    """

    def __init__(self, scrape_folder, api_key, query_type=None, search_params=None, chunk_days=None):
        """
        :param scrape_folder: The folder to save the scrape results and config in (must be empty)
        :param api_key: CrowdTangle API Key
        :param query_type: one of /posts, /posts/search, /leaderboard, /links (/post and /lists not supported)
        :param search_params: Crowdtangle search params as dict. Specific to the query type, see https://github.com/CrowdTangle/API/wiki
        :param chunkDays: Queries Crowdtangle in time spans of this size (e.g. chunk_days=7 -> 1 week).
                    Choose this so that one chunk  returns less than 10k results (correct behavior isn't guaranteed past 10k).
        """

        super().__init__(scrape_folder)

        # Pytangle setup
        self.api_key = api_key
        api = API(token=api_key)
        self.query_functions = {
            "/posts": api.posts,
            "/posts/search": api.search,
            "/leaderboard": api.leaderboard,
            "/links": api.links
        }

        # Load scrape config if there is an existing one, else create new one
        self.config_file = os.path.join(self.scrape_folder, "scraping_config.json")
        if os.path.exists(self.config_file):
            print("Resuming scrape from existing config...")
            self._read_config()
        else:
            print("Initializing new scrape...")
            assert query_type in ["/posts", "/posts/search", "/leaderboard", "/links"]
            self.config = CTConfig(STATUS_UNFINISHED, query_type, search_params, chunk_days, current_chunk=0, chunks=[])
            self._save_config()

    def _read_config(self):
        assert os.path.exists(self.config_file), "Expected config file found at {} but none found.".format(
            self.config_file)
        cfg = read_json(self.config_file)
        self.config = CTConfig(**cfg)

    def _save_config(self):
        save_json(self.config_file, self.config.__dict__, indent=3)

    def get_scrape_status(self):
        return self.config.scrape_status

    def _estimate_scrape_time(self, search_params, chunks, query_type):
        # Estimate total scraping time
        # time.sleep(10)  # Just to make sure we're not hitting the rate limit from a previous search
        search_params_hitcount = search_params.copy()
        current_chunk = self.config.current_chunk
        search_params_hitcount["startDate"] = chunks[current_chunk]["startDate"]
        hit_count = get_hitcount(query_type, search_params, self.api_key)
        print("Found {} posts".format(hit_count))
        print("Chunks completed: {}/{}".format(current_chunk, len(chunks)))
        chunks_left = len(chunks) - current_chunk
        # time = (#of total requests) * (10s per request) * (%of unscraped data)
        time_secs = (hit_count / search_params["count"] / 6 * 60) * (chunks_left / len(chunks))
        tt, ct = time.strftime('%H:%M:%S', time.gmtime(time_secs)), time.strftime('%H:%M:%S',
                                                                                  time.gmtime(time_secs / chunks_left))
        print("Total estimated time: {}. Estimated time per chunk: {}".format(tt, ct))
        time.sleep(10)

    def scrape(self):
        """
        Given via the config:
         - Crowdtangle search params: search_params
         - Crowdtangle query type: query_type
         - a list of [startDate, endDate] chunks
         - the index of the last unfinished chunk: current_chunk

        The search is split into chunks to not request more than 10k posts at once (limit set by Crowdtangle).
        We go through each chunk and send a request with startDate/endDate and the search params to Crowdtangle (via pytangle).
        Then collect the results for each chunk (they're split into mini-batches) and save them to disk.

        Result:
        List of .json files on disk (one for each chunk) with the response data sent by Crowdtange for the time period in the chunk
        """

        if self.get_scrape_status() == STATUS_FINISHED:
            print("This scrape is complete. To re-do it, please create a new one in a different folder.")
        else:
            search_params = self.config.search_params
            current_chunk = self.config.current_chunk
            chunks = self.config.chunks
            query_type = self.config.query_type
            query_function = self.query_functions[query_type]

            # set internal search params
            search_params["count"] = 100
            search_params["offset"] = 0

            print("\nSearch Params:")
            print(search_params, "\n")

            self._estimate_scrape_time(search_params, chunks, query_type)

            # Update search params with pytangle specific value for count
            search_params_copy = search_params.copy()
            search_params_copy["count"] = -1  # take all available posts

            with tqdm(total=len(chunks), desc="Overall Progress") as pbar:
                pbar.update(current_chunk)

                while current_chunk < len(chunks):
                    # get chunk start end end date
                    c_start, c_end = chunks[current_chunk]["startDate"], chunks[current_chunk]["endDate"]

                    # update params for current Crowdtangle request
                    search_params_copy["startDate"] = c_start
                    search_params_copy["endDate"] = c_end

                    # collect results for chunk and save them
                    chunk_posts = []
                    for post in query_function(**search_params_copy):
                        chunk_posts.append(post)
                    save_path = os.path.join(self.data_folder, "{}_{}.json".format(c_start, c_end))
                    save_json(save_path, chunk_posts)

                    # print("Posts in chunk {} - {}: {} ".format(c_start, c_end, len(chunk_posts)))

                    # update config
                    current_chunk += 1
                    self.config.current_chunk = current_chunk
                    self._save_config()

                    pbar.update(1)

            self.config.scrape_status = STATUS_FINISHED
            self._save_config()

    def combine_scrape_results(self, result_extractor, skip_if_exists=True, result_folder="result"):
        """
        Go through all scraped post data, transform it into tabular format and save it
        :param result_extractor: (type ScrapeContentExtractor): extractor for combining scrape results
        :param skip_if_exists: skip if result folder already exists
        :param result_folder: folder to save results into (as seen from executing directory)
        """

        os.makedirs(os.path.join(self.scrape_folder, result_folder), exist_ok=True)

        self.table_file = os.path.join(self.scrape_folder, result_folder, "result.csv")

        if os.path.exists(self.table_file) and skip_if_exists:
            print("Table already exists at {}. skipping".format(self.table_file))
        else:
            print("Creating meta data table from posts..")
            df = result_extractor.extract_all(self.data_folder)  # call to extractor here
            df.to_csv(self.table_file, index=False)
            print("Full table created at {}".format(self.table_file))
