import sys
import os

sys.path.append('..')
from CTScraper import CTScraper
from common.base_classes import STATUS_UNFINISHED
from CTExtractors import CTInstagramPostSearchExtractor
import api_key.API_KEY
from common.util import read_json
import time

if __name__ == "__main__":

    for year in [2014, 2015, 2016, 2017, 2018, 2019]:

        default_params = {
            "searchTerm": "#Glasgow",
            "searchField": "text_fields_only",
            "platforms": "instagram",
            "sortBy": "date",
            "startDate": "{}-01-01".format(year),
            "endDate": "{}-01-01".format(year + 1)
        }

        # Note: CT=Crowdtangle
        # Path to the folder where the scrape results and config will be stored. The folder has to be empty or not exist yet (will be created during the scrape)
        scrape_folder = "../../../data/Crowdtangle/Glasgow-{}".format(year)
        # Extractor for transforming the json response from CT into tabular format
        result_extractor = CTInstagramPostSearchExtractor()  # extractor specific to instagram and /posts/search
        # Crowdtangle API key
        # One of: /posts,/posts/search,/leaderboard,/links
        query_type = "/posts/search"
        # How many days' worth of data to request from CT in one request (=one 'chunk')
        # Should result in somewhere between 1k and 10k posts returned by crowdtangle. For more info see the readme.
        chunk_days = 366  # all, will automatically truncate to endDate-startDate
        # Path to a file with the search parameters used by Crowdtangle in json format
        search_params_file = "ct_search_paramsUNUSED.json"
        if os.path.exists(search_params_file):
            search_params = read_json(search_params_file)
            print("Reading CT search params from file")
        else:
            search_params = default_params
            print("Using CT default params")

        scraper = CTScraper(scrape_folder, API_KEY, query_type, search_params,
                            chunk_days)
        status = scraper.get_scrape_status()
        print("status:", status)
        if status == STATUS_UNFINISHED:
            scraper.scrape()
        scraper.combine_scrape_results(result_extractor, skip_if_exists=True)
