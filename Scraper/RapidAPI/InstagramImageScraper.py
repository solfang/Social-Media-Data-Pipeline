import sys

sys.path.append('..')
from Scraper.common.util import save_json, read_json
from Scraper.common.base_classes import Scraper, STATUS_UNFINISHED, STATUS_FINISHED
import pandas as pd
import os
import logging
import time
import requests
from tqdm import tqdm


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


class InstagramImageScraper(Scraper):
    """
    Scrapes images from Instagram given the image urls
    """

    def __init__(self, scrape_folder, posts, sleep_time, max_attempts=10, skip_if_exists=True):
        """
        The scrape will go over each post in 'posts' and attempt to scrape it.
        After it has reached the bottom of the posts list, it will re-do the scrape for all failed posts.
        This process is repeated until each post is either successfully scraped or has 'max_attempts' failed attempts.
        :param scrape_folder: folder where results and scrape data will be stored to.
        :param posts: Iterable that contains 3 lists: post ids, post shortcodes, image urls
        :param sleep_time: time to wait between scrapes in seconds.
        :param max_attempts: scraper will stop trying to scrape a post if it has failed at least max_attempts times.
        :param skip_if_exists: skip images that already exist in the output folder
        """
        super().__init__(scrape_folder)
        self.skip_if_exists = skip_if_exists

        post_ids, shortcodes, image_urls = posts

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
                "image_scraped": [0] * len(post_ids),  # 0=no, 1=yes
                "image_attempts": [0] * len(post_ids),  # number of attempted scrapes
                "max_attempts": [max_attempts] * len(post_ids),
                "shortcode": shortcodes,
                "image_url": image_urls
            })
            self._save_config()

        logging.basicConfig(filename=os.path.join(scrape_folder, 'scraping_log.txt'),
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S', level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)

        self.sleep_time = sleep_time

    def get_image(self, image_url):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
            r = requests.get(image_url, headers=headers)
            r.raise_for_status()  # raises an HTTPError if an error has occurred during the request (e.g. 404)
            return r.content
        except requests.exceptions.HTTPError as errh:
            self.logger.error("Http error: {}".format(errh))
            self.logger.error("Error message: {}".format(errh.response.content.decode()))
        except requests.exceptions.ConnectionError as errc:
            self.logger.error("Connection error: {}".format(errc))
        except requests.exceptions.Timeout as errt:
            self.logger.error("Connection error: {}".format(errt))
        except requests.exceptions.RequestException as errr:
            self.logger.error("Oops: Something Else: {}".format(errr))
        return None

    def scrape(self, *args, **kwargs):
        while not self.get_scrape_status(do_print=True) == STATUS_FINISHED:
            for post_id in tqdm(self._get_undone_posts(), desc="Scraping round progress"):
                scrape_success = False

                # get post info
                shortcode = self.config.loc[self.config["post_id"] == post_id, "shortcode"].item()
                image_url = self.config.loc[self.config["post_id"] == post_id, "image_url"].item()

                # skip post if it already exists on the disk
                fpath = os.path.join(self.image_folder, "{}_{}.jpg".format(post_id, shortcode))
                if os.path.exists(fpath) and self.skip_if_exists:
                    # print(fpath, "already exists")
                    self._increment_config(post_id, "image_scraped")
                    scrape_success = True
                    continue  # will go into 'finally' block

                # attempt to scrape the image
                self.logger.info(create_log_msg(shortcode, "Extracting image content..."))
                self._increment_config(post_id, "image_attempts")
                ####
                ## Alternate version of the image url that doesn't expire. I have not tested this a lot so if it fails use the image url stored in thes crape data (i.e. comment the below line out)
                image_url = "https://www.instagram.com/p/{}/media/?size=l".format(shortcode)
                ####
                img = get_image(image_url)
                if img is not None:
                    with open(fpath, "wb") as f:
                        f.write(img)
                        self._increment_config(post_id, "image_scraped")
                        scrape_success = True
                else:
                    print("error on post with shortcode=", shortcode)
                time.sleep(self.sleep_time)
                print("Scraped post {}, result: {}".format(shortcode, "success" if scrape_success else "fail"))
                self._save_config()

            # for s in tqdm(range(300),
            #               desc="Short break between scrape rounds to wait for temporarily unavailable posts to come back"):
            #     time.sleep(1)

        print("Images saved to {}".format(self.image_folder))
        print("unscraped posts:", self._get_undone_posts(shortcode=True))
        print("This scrape is complete. To re-do it, please create a new one in a different folder.")

    def _read_config(self):
        self.config = pd.read_csv(self.config_file)

    def _save_config(self):
        self.config.to_csv(self.config_file, index=False)

    def _increment_config(self, post_id, variable):
        self.config.loc[self.config["post_id"] == post_id, variable] += 1
        self._save_config()

    def _get_image_done(self, df):
        """
        returns whether all image data has been either scraped or exceeded the max number of attempts
        """
        return df[(df["image_scraped"] == 1) | (df["image_attempts"] >= df["max_attempts"])]

    def _get_undone_posts(self, shortcode=False):
        df_done = self._get_image_done(self.config)
        keyword = "shortcode" if shortcode else "post_id"
        return list(self.config[~self.config.index.isin(df_done.index)][keyword])

    def get_scrape_status(self, do_print=False):
        """
        rows: posts
        columns: total (all), done (scraped or failed), scraped (successfully scraped), failed (scraping failed more times than max_attempts allows))
        used to view the scrape progress
        """

        df = self.config

        data = [
            [len(df)],
            [len(self._get_image_done(df))],
            [len(df[df["image_scraped"] == 1])],
            [len(df[df["image_attempts"] >= df["max_attempts"]])]
        ]
        res = pd.DataFrame(data=data, columns=["images"], index=["total", "done", "scraped", "failed"])

        if do_print:
            print("Scrape status:")
            print(res)

        done = res.at["done", "images"] == res.at["total", "images"]
        if done:
            if do_print:
                print("unscraped posts:", self._get_undone_posts(shortcode=True))
            status = STATUS_FINISHED
        else:
            status = STATUS_UNFINISHED
        print("scrape status:", status)
        return status

    def combine_scrape_results(self, skip_if_exists=True, *args, **kwargs):
        # not needed for images
        pass


def do_scrape(output_folder, scrape_names, input_table_files):
    """
    Manages multiple consecutive scrapes
    :param: output_folder: path to store the scraped data at (sub-folder for each scrape will be created automatically)
    :param scrape_names: list of scrape names, from which the scrape folders will be constructed
    :param input_table_files: input table for each scrape (which is the output table of the feed scrape)
    """
    for i, scrape_name in enumerate(scrape_names):
        # contains posts to be scraped
        df_in = pd.read_csv(input_table_files[i])

        scrape_folder = os.path.join(output_folder, scrape_name)

        # filter df_in to only include images that should be scraped
        if "scrape_image" in df_in.columns:
            df_in = df_in[df_in["scrape_image"] == True]

        post_ids = list(df_in["id"])
        post_shortcodes = list(df_in["shortcode"])
        image_urls = list(df_in["thumbnail_src"])
        posts = zip(post_ids, post_shortcodes, image_urls)

        scraper = InstagramImageScraper(scrape_folder, scrape_name, posts, sleep_time=0, max_attempts=5)
        status = scraper.get_scrape_status()

        if status == STATUS_UNFINISHED:
            scraper.scrape()


if __name__ == "__main__":
    casestudy = "Dublin_GCT"

    scrape_name = casestudy
    input_table = "../../../data/IconicityStudies/Feed_preprocessed/{}/{}_metadata.csv".format(casestudy, casestudy)
    output_folder = "../../../data/IconicityStudies/Images"
    do_scrape(output_folder, [scrape_name], [input_table])
