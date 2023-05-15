import sys

sys.path.append('..')
from Scraper.common.util import save_json, read_json
from Scraper.common.base_classes import Scraper, STATUS_UNFINISHED, STATUS_FINISHED
import pandas as pd
import os
import logging
import time
import traceback
import numpy as np
from instascrape import Post
from instascrape.exceptions.exceptions import InstagramLoginRedirectError
import requests
import random
from tqdm import tqdm
import json
if os.path.exists("api_key.py"):
    from api_key import API_KEY
else:
    print("Please create a file api_key.py with API_KEY='...' and make sure it's in gitignore")
import re
from ast import literal_eval


def get_image(url):
    try:
        r = requests.get(url)
        r.raise_for_status()  # raises an HTTPError if an error has occurred during the request (e.g. 404)
        return r.content
    except requests.exceptions.HTTPError as err:
        print(err)
        # logger.error(err)
        # logger.error(r.content.decode('utf-8'))


def create_log_msg(post_id, msg):
    return "{} - {}".format(post_id, msg)


class InstagramScraper(Scraper):

    def __init__(self, scrape_folder, scrape_name, posts, api_key, logger, sleep_time, max_attempts=10):
        """
        The scrape will go over each post in 'posts' and attempt to scrape it.
        After it has reached the bottom of the posts list, it will re-do the scrape for all failed posts.
        This process is repeated until each post is either successfully scraped or has 'max_attempts' failed attempts.
        :param scrape_folder: folder where results and scrape data will be stored to.
        :param scrape_name (str): additional info that will be added to the output data to identify scrapes (can ignore).
        :param posts: zipped list of (post ids, post shortcodes).
        :param api_key: RapidAPI 'Instagram Data' API token
        :param logger: logging module logger.
        :param sleep_time: time to wait between scrapes in seconds.
        :param max_attempts: scraper will stop trying to scrape a post if it has failed at least max_attempts times.
        """
        super().__init__(scrape_folder)
        self.scrape_name = scrape_name

        post_ids, shortcodes = list(zip(*posts))  # unzip

        assert len(post_ids) == len(set(post_ids)), "Please remove duplicate posts"

        self.image_folder = os.path.join(self.scrape_folder, "images")
        os.makedirs(self.image_folder, exist_ok=True)

        # Load scrape config if there is an existing one, else create new one
        self.config_file = os.path.join(self.scrape_folder, "scraping_config.csv")
        if os.path.exists(self.config_file):
            print("Resuming scrape from existing config...")
            self._read_config()
        else:
            print("Initializing new scrape...")
            self.config = pd.DataFrame(data={
                "post_id": post_ids,
                "data_scraped": [0] * len(post_ids),  # 0=no, 1=yes
                "image_scraped": [0] * len(post_ids),  # 0=no, 1=yes
                "data_attempts": [0] * len(post_ids),  # number of attempted scrapes
                "image_attempts": [0] * len(post_ids),  # number of attempted scrapes
                "max_attempts": [max_attempts] * len(post_ids),
                "shortcode": shortcodes,

            })
            self._save_config()

        self.url = "https://instagram-data1.p.rapidapi.com/post/info"
        self.headers = {
            'x-rapidapi-host': "instagram-data1.p.rapidapi.com",
            'x-rapidapi-key': api_key
        }

        self.logger = logger

        self.sleep_time = sleep_time

    def _download_post_image(self, js, image_folder):
        url = js["display_resources"][2]["src"]
        id = js["id"]
        shortcode = js["shortcode"]

        fname = os.path.join(image_folder, "{}_{}.jpg".format(id, shortcode))
        img = get_image(url)

        with open(fname, "wb") as f:
            f.write(img)

    def _download_post_images(self, js, image_folder):
        if js["__typename"] == "GraphImage":
            self._download_post_image(js, image_folder)

        elif js["__typename"] == "GraphSidecar":
            self._download_post_image(js, image_folder)  # front image (has shortcode of post)
            sidecar_js = js["edge_sidecar_to_children"]["edges"]
            for img_js in sidecar_js[1:]:  # sidecar images (skip front image cause it has different shortcode)
                self._download_post_image(img_js["node"], image_folder)
        else:
            print("unknown post format: '__typename'={}".format(js["__typename"]))

    def _get_post(self, shortcode, max_tries=5):
        params = {"post": "https://www.instagram.com/p/{}/".format(shortcode)}
        for i in range(max_tries):
            try:
                response = requests.request("GET", self.url, headers=self.headers, params=params)
                response.raise_for_status()  # raises an HTTPError if an error has occurred during the request (e.g. 404)
                return json.loads(response.content.decode('utf-8'))
            except requests.exceptions.HTTPError as errh:
                self.logger.error("Http error: {}".format(errh))
                self.logger.error("Error message: {}".format(errh.response.content.decode()))
                # Note: some posts will temporarily (?) be a 404 but then recover later. Just try again later.
                if response.status_code == 404:
                    break
            except requests.exceptions.ConnectionError as errc:
                self.logger.error("Connection error: {}".format(errc))
            except requests.exceptions.Timeout as errt:
                self.logger.error("Connection error: {}".format(errt))
            except requests.exceptions.RequestException as err:
                self.logger.error("Oops: Something Else: {}".format(errt))
        return None

    def scrape(self, *args, **kwargs):
        while not self.get_scrape_status(do_print=True) == STATUS_FINISHED:
            for post_id in tqdm(self._get_undone_posts(), desc="Scraping round progress"):
                scrape_success = False

                shortcode = self.config.loc[self.config["post_id"] == post_id, "shortcode"].item()

                try:
                    data_path = os.path.join(self.data_folder, "{}_{}.json".format(post_id, shortcode))
                    # skip post if it already exists on the disk
                    if os.path.exists(data_path):
                        print(data_path, "already exists")
                        self._increment_config(post_id, "data_scraped")
                        self._increment_config(post_id, "image_scraped")
                        scrape_success = True
                        continue  # will go into 'finally' block

                    # get post json data
                    self.logger.info(create_log_msg(shortcode, "Extracting data..."))
                    self._increment_config(post_id, "data_attempts")
                    post_json = self._get_post(shortcode)
                    if post_json is not None:
                        save_json(data_path, post_json, indent=3)
                        self.logger.info(create_log_msg(shortcode, "data extracted."))
                        self._increment_config(post_id, "data_scraped")

                        # get the image
                        self.logger.info(create_log_msg(shortcode, "Extracting image content..."))
                        self._increment_config(post_id, "image_attempts")
                        self._download_post_images(post_json, self.image_folder)
                        self.logger.info(create_log_msg(shortcode, "Image content extracted."))
                        self._increment_config(post_id, "image_scraped")
                        scrape_success = True
                        time.sleep(self.sleep_time)

                except Exception as e:
                    self.logger.error(create_log_msg(shortcode, e))
                    print("error on post with shortcode=", shortcode)
                    print(traceback.format_exc())
                    time.sleep(self.sleep_time)
                finally:
                    print("Scraped post {}, result: {}".format(shortcode, "success" if scrape_success else "fail"))
                    self._save_config()

            for s in tqdm(range(300),
                          desc="Short break between scrape rounds to wait for temporarily unavailable posts to come back"):
                time.sleep(1)

        print("unscraped posts:", self._get_undone_posts(shortcode=True))
        print("This scrape is complete. To re-do it, please create a new one in a different folder.")

    def _read_config(self):
        self.config = pd.read_csv(self.config_file)

    def _save_config(self):
        self.config.to_csv(self.config_file, index=False)

    def _increment_config(self, post_id, variable):
        self.config.loc[self.config["post_id"] == post_id, variable] += 1
        self._save_config()

    def _get_data_done(self, df):
        """
        returns whether all post data has been either scraped or exceeded the max number of attempts
        """
        return df[(df["data_scraped"] == 1) | (df["data_attempts"] >= df["max_attempts"])]

    def _get_image_done(self, df):
        """
        returns whether all image data has been either scraped or exceeded the max number of attempts
        """
        return df[(df["image_scraped"] == 1) | (df["image_attempts"] >= df["max_attempts"]) | (
                df["data_attempts"] >= df["max_attempts"])]

    def _get_undone_posts(self, shortcode=False):
        df_done = self._get_data_done(self.config)
        keyword = "shortcode" if shortcode else "post_id"
        return list(self.config[~self.config.index.isin(df_done.index)][keyword])

    def get_scrape_status(self, do_print=False):
        df = self.config

        data = [
            [len(df), len(df)],
            [len(self._get_data_done(df)), len(self._get_image_done(df))],
            [len(df[df["data_scraped"] == 1]), len(df[df["image_scraped"] == 1])],
            [len(df[df["data_attempts"] >= df["max_attempts"]]), len(df[df["image_attempts"] >= df["max_attempts"]])]
        ]
        res = pd.DataFrame(data=data, columns=["data", "images"], index=["total", "done", "scraped", "failed"])

        if do_print:
            print("Scrape status:")
            print(res)

        done = res.at["done", "data"] == res.at["total", "data"]
        if done:
            if do_print:
                print("unscraped posts:", self._get_undone_posts(shortcode=True))
            return STATUS_FINISHED
        else:
            return STATUS_UNFINISHED

    def combine_scrape_results(self, skip_if_exists=True, *args, **kwargs):

        """
        Quick and dirty json->table conversion. Only values of the json that are important to use are included.
        For a cleaner approach see the instascrape module (e.g. from instascrape import Post)
        """

        result_folder = output_json = os.path.join(self.scrape_folder, "results")
        os.makedirs(result_folder, exist_ok=True)
        output_json = os.path.join(result_folder, "post_raw.json")  # all post jsons concatenated
        output_csv = os.path.join(result_folder, "post_metadata.csv")  # important data as table
        output_minimal = os.path.join(result_folder, "post_minimal.csv")  # only post url, caption, comments
        output_comments = os.path.join(result_folder, "post_comments.csv")  # comment hierarchy as table
        output_images = os.path.join(result_folder, "post_images.csv")  # table that links images to posts/users

        all_jsons = os.listdir(self.data_folder)

        def shortcode_to_post_url(shortcode):
            return "https://www.instagram.com/p/{}/".format(shortcode)

        def js_to_comment_table(post_json):

            def js_to_row(post_id, post_shortcode, parent_id, comment_count, comment_js):
                c = comment_js
                return {"post_id": post_id, "post_shortcode": post_shortcode, "comment_count": comment_count,
                        "parent_comment_id": parent_id, "comment_id": c["id"], "owner": c["owner"]["username"],
                        "likes": c["edge_liked_by"]["count"], "text": c["text"]}

            rows = []
            js = post_json

            comment_count = js["edge_media_to_parent_comment"]["count"]
            for comment in js["edge_media_to_parent_comment"]["edges"]:
                c = comment["node"]
                parent_row = js_to_row(js["id"], js["shortcode"], None, comment_count, c)
                rows.append(parent_row)
                try:
                    for threaded_comment in c["edge_threaded_comments"]["edges"]:
                        c_threaded = threaded_comment["node"]
                        row = js_to_row(js["id"], js["shortcode"], parent_row["id"], comment_count, c_threaded)
                        rows.append(row)
                except KeyError:
                    pass

            return pd.DataFrame(data=rows)

        def js_to_flat_dict(post_json):
            js = post_json
            row = {}
            row["id"] = js["id"]
            row["shortcode"] = js["shortcode"]
            row["source"] = self.scrape_name
            row["post_url"] = shortcode_to_post_url(js["shortcode"])

            row["display_url"] = js["display_url"]

            if "accessibility_caption" in js.keys():
                row["accessibility_caption"] = js["accessibility_caption"]
            else:
                row["accessibility_caption"] = None

            row["is_video"] = js["is_video"]
            row["tagged_users"] = [user["node"]["user"]["username"] for user in
                                   js["edge_media_to_tagged_user"]["edges"]]

            try:
                row["caption"] = js["edge_media_to_caption"]["edges"][0]["node"]["text"]
                row["hashtags"] = re.findall(r"#(\w+)", row["caption"])
            except IndexError:
                row["caption"] = None  # note: caption can be empty ("") or not existent (None)
                row["hashtags"] = []

            row["caption_is_edited"] = js["caption_is_edited"]
            row["has_ranked_comments"] = js["has_ranked_comments"]
            row["like_and_view_counts_disabled"] = js["like_and_view_counts_disabled"]
            row["likes"] = js["edge_media_preview_like"]["count"]

            row["comment_count"] = js["edge_media_to_parent_comment"]["count"]
            comments = []
            for comment in js["edge_media_to_parent_comment"]["edges"]:
                c = comment["node"]
                comments.append(c["text"])
                try:
                    for threaded_comment in c["edge_threaded_comments"]["edges"]:
                        c_threaded = threaded_comment["node"]
                        comments.append(c_threaded["text"])
                except KeyError:
                    pass
            row["comments_flat"] = comments

            row["comments_disabled"] = js["comments_disabled"]
            row["timestamp"] = js["taken_at_timestamp"]
            row["is_affiliate"] = js["is_affiliate"]
            row["is_paid_partnership"] = js["is_paid_partnership"]
            row["location"] = None if js["location"] is None else js["location"]["name"]
            row["owner_id"] = js["owner"]["id"]
            row["owner_username"] = js["owner"]["username"]
            row["owner_followercount"] = js["owner"]["edge_followed_by"]["count"]
            row["owner_posts"] = js["owner"]["edge_owner_to_timeline_media"]["count"]
            row["is_ad"] = js["is_ad"]

            row["album_images"] = []
            if "edge_sidecar_to_children" in js.keys():
                for img in js["edge_sidecar_to_children"]["edges"]:
                    i = img["node"]
                    row["album_images"].append("{}_{}".format(i["id"], i["shortcode"]))

            return row

        def create_image_df(df_all):
            rows = []
            for idx, row in df_all.iterrows():
                imgs = ["{}_{}".format(row["id"], row["shortcode"])] + row["album_images"]
                for img in imgs:
                    img_fname = img + ".jpg"
                    if os.path.exists(os.path.join(self.image_folder, img_fname)):
                        new_row = [row["shortcode"], row["owner_username"], row["timestamp"], img_fname]
                        rows.append(new_row)
            return pd.DataFrame(data=rows, columns=["shortcode", "owner_username", "timestamp", "image"])
            # All posts as json

        if os.path.exists(output_json) and skip_if_exists:
            print(output_json, "already exists. Skipping.")
            all_posts = read_json(output_json)
        else:
            all_posts = []
            for fname in all_jsons:
                fpath = os.path.join(self.data_folder, fname)
                js = read_json(fpath)
                all_posts.append(js)
            save_json(output_json, all_posts, indent=3)
            print("Saved output to {}".format(output_json))

        # Important info as csv (lean table)
        if os.path.exists(output_csv) and skip_if_exists:
            print(output_csv, "already exists. Skipping.")
            df_all = pd.read_csv(output_csv)
            df_all["album_images"] = literal_eval(df_all["album_images"])
        else:
            rows = [js_to_flat_dict(post_json) for post_json in all_posts]
            df_all = pd.DataFrame(data=rows)
            df_all["timestamp"] = pd.to_datetime(df_all["timestamp"], unit="s")
            df_all.sort_values(by="timestamp", ascending=True, inplace=True)
            df_all.to_csv(output_csv, index=False)
            print("Saved output to {}".format(output_csv))

        # post url, caption, comments (minimal table)
        if os.path.exists(output_minimal) and skip_if_exists:
            print(output_minimal, "already exists. Skipping.")
        else:
            df_min = df_all[
                ["display_url", "post_url", "timestamp", "likes", "comment_count", "caption", "comments_flat"]]
            new_comments = df_min["comments_flat"].apply(lambda l: "\n".join(l))
            df_min.insert(len(df_min.columns), "comments", new_comments)
            del df_min["comments_flat"]
            df_min.to_csv(output_minimal, index=False)
            print("Saved output to {}".format(output_minimal))

        # comments
        if os.path.exists(output_comments) and skip_if_exists:
            print(output_comments, "already exists. Skipping.")
        else:
            dfs = [js_to_comment_table(post_json) for post_json in all_posts]
            df_comment = pd.concat(dfs, axis=0)
            df_comment.to_csv(output_comments, index=False)
            print("Saved output to {}".format(output_comments))

        # images
        if os.path.exists(output_images) and skip_if_exists:
            print(output_images, "already exists. Skipping.")
        else:
            df_images = create_image_df(df_all)
            df_images.to_csv(output_images, index=False)
            print("Saved output to {}".format(output_images))


def do_scrape(scrape_names, input_table_files, skip_if_exists,
              filter_by={"hashtags": [], "caption": [], "searchterm": []}):
    """
    Manages multiple consecutive scrapes
    :param scrape_names: list of scrape names, from which the scrape folders will be constructed
    :param input_table_files: input table for each scrape (which is the output table of the feed scrape)
    :param skip_if_exists: if a scrape has already finished we skip constructing the output tables
    :param filter_by: (not used atm) only scrape posts from the input table that meet a criterion, e.g. contain the word 'depot' in the post caption.
    """
    for i, scrape_name in enumerate(scrape_names):
        df_in = pd.read_csv(input_table_files[i])

        scrape_folder = "../../../data/Instagram-API/Posts/{}".format(scrape_name)

        os.makedirs(scrape_folder, exist_ok=True)
        logging.basicConfig(filename=os.path.join(scrape_folder, 'scraping_log.txt'),
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S', level=logging.DEBUG)
        logger = logging.getLogger(__name__)

        if filter_by["caption"]:
            df_in["caption"] = df_in["caption"].str.lower().astype(str)
            caption_filter = df_in["caption"].apply(lambda s: any([word in s for word in filter_by["caption"]]))
        else:
            caption_filter = pd.Series([True] * len(df_in))

        if filter_by["hashtags"]:
            df_in["hashtags"] = df_in["hashtags"].apply(literal_eval).apply(lambda l: [el.lower() for el in l])
            hashtag_filter = df_in["hashtags"].apply(lambda l: len(set(l) & set(filter_by["hashtags"])) > 0)
        else:
            hashtag_filter = pd.Series([True] * len(df_in))

        if filter_by["searchterm"]:
            searchterm_filter = df_in["search_term"].isin(filter_by["searchterm"])
        else:
            searchterm_filter = pd.Series([True] * len(df_in))

        # location_filter = df_in["location"].isin(location_ids)

        print("number of posts before filters:", len(df_in))
        df_in = df_in[caption_filter | hashtag_filter | searchterm_filter]
        print("number of posts after filters:", len(df_in))

        post_ids = list(df_in["id"])
        post_shortcodes = list(df_in["shortcode"])
        posts = zip(post_ids, post_shortcodes)

        scraper = InstagramScraper(scrape_folder, scrape_name, posts, API_KEY, logger, sleep_time=2, max_attempts=5)
        status = scraper.get_scrape_status()

        if status == STATUS_UNFINISHED:
            scraper.scrape()
        scraper.combine_scrape_results(skip_if_exists=skip_if_exists)


if __name__ == "__main__":
    scrape_name = "Boijmans_all" # "Boijmans_location"
    input_table = "../../../data/Instagram-API/Feed/boijmans-masterlist-hashtags_depot.csv"
    do_scrape([scrape_name], [input_table], skip_if_exists=False)
