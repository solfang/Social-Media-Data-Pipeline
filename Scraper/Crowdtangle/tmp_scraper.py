# On the fly scraper for scraping Crowdtangle posts and images. Only works with queries that have les than 10k hits (beyond that Crowdtangle may behave weirldy)
# uses pytangle

import sys

sys.path.append('..')
from pytangle.api import API  # https://github.com/hide-ous/pytangle
from api_key import API_KEY
import os
from common.util import read_json, save_json
import requests
from tqdm import tqdm
import pandas as pd


def get_image(image_url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        r = requests.get(image_url, headers=headers)
        r.raise_for_status()  # raises an HTTPError if an error has occurred during the request (e.g. 404)
        return r.content
    except requests.exceptions.HTTPError as errh:
        print("Http error: {}".format(errh))
        print("Error message: {}".format(errh.response.content.decode()))
    except requests.exceptions.ConnectionError as errc:
        print("Connection error: {}".format(errc))
    except requests.exceptions.Timeout as errt:
        print("Connection error: {}".format(errt))
    except requests.exceptions.RequestException as errr:
        print("Oops: Something Else: {}".format(errr))
    return None


def scrape(params_list, scrape_folder):
    data_folder = os.path.join(scrape_folder, "data")
    image_folder = os.path.join(scrape_folder, "images/images")
    os.makedirs(scrape_folder, exist_ok=True)
    os.makedirs(data_folder, exist_ok=True)
    os.makedirs(image_folder, exist_ok=True)

    for params in params_list:
        api = API(token=API_KEY)

        for post in tqdm(api.search(**params), desc="{} - {}".format(params["startDate"], params["endDate"])):

            if post["type"] == "photo":
                # save text content and metadata
                data_fpath = os.path.join(data_folder, "{}.json".format(post["platformId"]))
                if not os.path.exists(data_fpath):
                    save_json(data_fpath, post)

                image_fpath = os.path.join(image_folder, "{}.jpg".format(post["platformId"]))
                if not os.path.exists(image_fpath):
                    # save the image
                    post_url = post["postUrl"]  # https://www.instagram.com/p/B55CQVNn_xI/
                    # this url gives a link to the first image of the post. Importantly, the link does not expire after a certain amount of time
                    image_url = post_url + "media/?size=l"  # https://www.instagram.com/p/B55CQVNn_xI/media/?size=l
                    img = get_image(image_url)
                    if img is not None:
                        with open(image_fpath, "wb") as f:
                            f.write(img)

    # save posts as json
    jsons = [read_json(os.path.join(data_folder, fname)) for fname in os.listdir(data_folder)]
    df = pd.DataFrame(jsons)
    df.to_csv(os.path.join(scrape_folder, "posts.csv"), index=False)


if __name__ == "__main__":

    intervals = [
    ]

    # iconicarchitecture
    # name = "Crowdtangle_architecture_iconic"
    # term = "#architecture AND (#icon OR #iconic)"
    # scrape_folder = "../../../../data/CT_datasets/{}".format(name)
    # params_list = []
    # for year in range(2018, 2019 + 1):
    #     params = {
    #         "searchTerm": "{}".format(term),
    #         "searchField": "text_fields_only",
    #         "platforms": "instagram",
    #         "sortBy": "date",
    #         "startDate": "{}-01-01T00:00:00".format(year),
    #         "endDate": "{}-12-31T23:59:59".format(year),
    #         "count": -1  # re-sample according to #iconicarchitecture frequency
    #     }
    #     params_list.append(params)
    #
    # scrape(params_list, scrape_folder)

    name = "Crowdtangle_architecture"
    term = "#architecture AND NOT (#icon OR #iconic)"
    scrape_folder = "../../../../data/CT_datasets/{}".format(name)

    # number of posts of #iconicarchitecture to re-sample by
    # distr_iconicarchitecture = {
    #     2014: 87,
    #     2015: 59,
    #     2016: 57,
    #     2017: 71,
    #     2018: 178,
    #     2019: 304,
    #     2020: 314,
    #     2021: 157,
    #     2022: 420,
    #     2023: 15
    # }
    distr_iconicbuildings = {
        2014: 26,
        2015: 32,
        2016: 81,
        2017: 131,
        2018: 178,
        2019: 392,
        2020: 210,
        2021: 154,
        2022: 191,
        2023: 44
    }

    # one_query is weird here, counted from architecture_iconic df by year
    distr = {
        2013: 311,
        2014: 582,
        2015: 1105,
        2016: 1837,
        2017: 3069,
        2018: 2929,
        2019: 2306
    }

    params_list = []
    # architecture - sample posts from each first day of the month at 00:00, 8:09, 16:00 UTC
    # for year in range(2014, 2019 + 1):
    #     for month in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]:
    #         for hour in ["00", "08", "16"]:
    #             params = {
    #                 "searchTerm": "{}".format(term),
    #                 "searchField": "text_fields_only",
    #                 "platforms": "instagram",
    #                 "sortBy": "date",
    #                 "startDate": "{}-{}-01T{}:00:00".format(year, month, hour),
    #                 "endDate": "{}-{}-01T23:59:59".format(year, month),
    #                 "count": 1 * int(distr[year] / 12)  # re-sample according to #iconicarchitecture frequency
    #             }
    #             params_list.append(params)
    for year in range(2014, 2019 + 1):
        for month in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]:
            params = {
                "searchTerm": "{}".format(term),
                "searchField": "text_fields_only",
                "platforms": "instagram",
                "sortBy": "date",
                "startDate": "{}-{}-01T00:00:00".format(year, month),
                "endDate": "{}-{}-28T23:59:59".format(year, month),
                "count": 1 * int(distr[year] / 12)  # re-sample according to #iconicarchitecture frequency
            }
            params_list.append(params)

    scrape(params_list, scrape_folder)
