from Scraper.common.util import save_json, read_json
from Scraper.common.base_classes import Scraper, STATUS_UNFINISHED, STATUS_FINISHED
import pandas as pd
import os
import logging
import time
import requests
import json
import re


def shortcode_to_post_url(shortcode):
    return "https://www.instagram.com/p/{}/".format(shortcode)


def make_request_save_response(url, fname):
    r = requests.get(url)
    with open(fname, "wb") as f:
        f.write(r.content)


def create_log_msg(identifier, msg):
    return "{} - {}".format(identifier, msg)


def setup_logger(fpath):
    logging.basicConfig(handlers=[
        logging.FileHandler(fpath),
        logging.StreamHandler()
    ], format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
        datefmt='%H:%M:%S', level=logging.INFO)
    return logging.getLogger(__name__)


class InstagramFeedScraper(Scraper):
    """
    Scrapes Instagram data using a RapidAPI service.
    """

    def __init__(self, scrape_folder, api_key, search_term, mode, max_tries=10):
        """
        :param scrape_folder: where to save the scrape results
        :param api_key: RapidAPI key
        :param search_term: a term to search Instagram with.
        :param mode: one of: 'location', 'hashtag', 'user'
        :param max_tries: number of times the request is re-tried on failure
        """
        super().__init__(scrape_folder)

        assert mode in ["location", "hashtag", "user"]
        # remove hashtag and @ symbol
        if mode == "hashtag":
            search_term = re.sub("#", "", search_term)
        if mode == "user":
            search_term = re.sub("@", "", search_term)

            assert not search_term.startswith("#"), "Please specify the hashtag without leading #"

        # Load scrape config if there is an existing one, else create new one
        self.config_file = os.path.join(self.scrape_folder, "scraping_config.json")
        if os.path.exists(self.config_file):
            print("Resuming scrape from existing config...")
            self._read_config()
        else:
            print("Initializing new scrape...")
            self.config = {"scrape_status": STATUS_UNFINISHED,
                           "mode": mode,
                           "search_term": search_term,
                           "post_count": -1,
                           "collected_posts": 0,
                           "loop_ctr": 0,
                           "end_cursor": [""]  # list of end cursors, first one is blank
                           }
            self._save_config()

        self.max_tries = max_tries

        # API stuff
        self.url = "https://instagram-data1.p.rapidapi.com/{}/feed".format(mode)
        self.headers = {
            'x-rapidapi-host': "instagram-data1.p.rapidapi.com",
            'x-rapidapi-key': api_key
        }

        self.logger = setup_logger(os.path.join(scrape_folder, 'scraping_log.log'))

    def feed_request(self, end_cursor=""):
        """
        :param end_cursor: token to navigate pagination
        :param max_tries: number of times the request is re-tried on failure
        :return: the json response
        """
        keys = {"location": "location_id", "hashtag": "hashtag", "user": "username"}
        key = keys[self.config["mode"]]

        params = {
            key: self.config["search_term"],
            "end_cursor": end_cursor
        }
        for i in range(self.max_tries):
            try:
                response = requests.request("GET", self.url, headers=self.headers, params=params)
                response.raise_for_status()  # raises an HTTPError if an error has occurred during the request (e.g. 404)
                js = json.loads(response.content.decode('utf-8'))
                if not js["has_more"]:
                    self.logger.warning(create_log_msg(end_cursor, "Unexepcted empty response. retrying."))
                    time.sleep(10)
                    continue
                return js

            except requests.exceptions.HTTPError as errh:
                self.logger.error("Http Error:", errh)
                print(errh.response.content.decode())
            except requests.exceptions.ConnectionError as errc:
                self.logger.error("Error Connecting:", errc)
            except requests.exceptions.Timeout as errt:
                self.logger.error("Timeout Error:", errt)
            except requests.exceptions.RequestException as err:
                self.logger.error("Oops: Something Else", err)
        return None

    def scrape(self, *args, **kwargs):
        # keeps track of which page of the feed we are currently scraping
        i = self.config["loop_ctr"]
        # how many posts we have scraped so far
        collected_posts = self.config["collected_posts"]
        end_cursor = self.config["end_cursor"][-1]
        has_more = True

        # while collected_posts < self.config["post_count"] or collected_posts == 0:  # collected_posts == 0 on first loop
        while has_more:
            self.logger.info(create_log_msg(end_cursor, "Extracting feed data..."))

            js = self.feed_request(end_cursor=end_cursor)

            if js is not None:
                # extract response data
                if collected_posts == 0:  # first loop
                    self.config["post_count"] = js["count"]
                    print(
                        "Found {} posts for the search term {}:{}".format(js["count"], self.config["mode"],
                                                                          self.config["search_term"]))
                has_more = js["has_more"]
                end_cursor = js["end_cursor"]
                collected_posts += len(js["collector"])
                # save data
                fname = "{:04d}_{}.json".format(i, end_cursor)
                save_json(os.path.join(self.data_folder, fname), js)
                i += 1

                # save vars to config
                self.config["loop_ctr"] = i
                self.config["collected_posts"] = collected_posts
                self.config["end_cursor"].append(end_cursor)

                self.logger.info(create_log_msg(self.config["end_cursor"][-2], "Request successful."))

            else:
                self.logger.error(create_log_msg(end_cursor, "Request failed."))
                break

            self._save_config()
            time.sleep(1)

        self.config["scrape_status"] = STATUS_FINISHED
        self._save_config()

        print("This scrape is complete. To re-do it, please create a new one in a different folder.")

    def _read_config(self):
        self.config = read_json(self.config_file)

    def _save_config(self):
        save_json(self.config_file, self.config, indent=3)

    def get_scrape_status(self):
        print("scrape status:", self.config["scrape_status"])
        return self.config["scrape_status"]

    def combine_scrape_results(self, skip_if_exists=True, *args, **kwargs):
        """
        combines the response json data (contains multiple posts) into a table where each row is one post
        """
        result_file = os.path.join(self.scrape_folder, "metadata.csv")
        if os.path.exists(result_file) and skip_if_exists:
            print("Table already exists at {}. skipping.".format(result_file))
        else:
            print("Creating meta data table from posts..")
            rows = []
            for fpath in os.listdir(self.data_folder):
                js = read_json(os.path.join(self.data_folder, fpath))
                for post in js["collector"]:
                    row = {}
                    row["id"] = post["id"]
                    row["shortcode"] = post["shortcode"]
                    row["post_url"] = shortcode_to_post_url(post["shortcode"])
                    # location feed does not seem to return the post type
                    row["type"] = None
                    if "type" in post.keys():
                        row["type"] = post["type"]
                    row["is_video"] = post["is_video"]
                    row["likes"] = post["likes"]
                    row["comment_count"] = post["comments"]
                    row["comments_disabled"] = post["comments_disabled"]
                    row["search_mode"] = self.config["mode"]
                    row["search_term"] = self.config["search_term"]
                    # location is only available for the location feed search
                    row["caption"] = post["description"]
                    row["hashtags"] = post["hashtags"]  # re.findall(r"#(\w+)", post["description"])
                    row["display_url"] = post["display_url"]
                    row["owner_id"] = post["owner"]["id"]
                    row["timestamp"] = post["taken_at_timestamp"]
                    row["mentions"] = post["mentions"]
                    row["display_url"] = post["display_url"]
                    row["thumbnail_src"] = post["thumbnail_src"]
                    rows.append(row)
            df = pd.DataFrame(data=rows)
            if len(df) > 0:
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
                df = df.drop_duplicates(subset=["id", "shortcode"])
            df.to_csv(result_file, index=False)
            print("Output table saved to {}".format(result_file))

    def cleanup_data(self):
        """
        deletes the individual json files from the scrape (they've been combined into one table)
        Only call after combine_scrape_results has been called.
        """

        for fpath in os.listdir(self.data_folder):
            os.remove(os.path.join(self.data_folder, fpath))


if __name__ == "__main__":

    scrape_folder = "../../../data/IconicityStudies/Feed"
    tags = ["ssehydroarena"]
    mode = "hashtag"  # DO: adapt for new run, one of: location, hashtag, user
    search_terms = tags

    for search_term in search_terms:

        scrape_name = "{}-{}".format(mode, search_term)
        scrape_path = os.path.join(scrape_folder, scrape_name)
        scraper = InstagramFeedScraper(scrape_path, API_KEY, search_term, mode)
        status = scraper.get_scrape_status()
        if status == STATUS_UNFINISHED:
            scraper.scrape()
        scraper.combine_scrape_results(skip_if_exists=True)
