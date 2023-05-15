import requests
import json
import sys
import os

sys.path.append('..')
from common.util import read_json, save_json
from api_key import API_KEY
import time

endpoints = {
    "/posts": "https://api.crowdtangle.com/posts",
    "/posts/search": "https://api.crowdtangle.com/posts/search",
    "/leaderboard": "https://api.crowdtangle.com/leaderboard",
    "/links": "https://api.crowdtangle.com/links",
    "/post": "https://api.crowdtangle.com/post/"

}


def ct_request(url, params, api_key, debug=False):
    """
    Thin wrapper for crowdtangle requests
    params: documentation: https://github.com/CrowdTangle/API/wiki
    api_key: Crowdtangle API key
    :Returns: If successful, the query result. Else returns None. If debug=True returns the queried url
    """
    ret = None
    if debug:
        ret = url
    else:
        try:
            params_copy = params.copy()
            params_copy["token"] = api_key
            r = requests.get(url, params=params_copy)
            r.raise_for_status()  # raises an HTTPError if an error has occurred during the request (e.g. 404)
            text = json.loads(r.content.decode('utf-8'))
            ret = text["result"]
        except requests.exceptions.HTTPError as err:
            print(r.content.decode('utf-8'))
            SystemExit(err)
    return ret


def get_hitcount(query_type, params, api_key):
    url = endpoints[query_type]
    params_copy = params.copy()
    params_copy["count"] = 0
    res = ct_request(url, params_copy, api_key)
    hit_count = res["hitCount"] if "hitCount" in res else 0
    return hit_count


def get_post(post_id, api_key):
    url = endpoints["/post"]
    url += post_id
    res = ct_request(url, {}, api_key)
    return res


if __name__ == "__main__":
    """
    Executes one query only based on the search params given via .json
    In case you just want to perform one single search without pagination and all that jazz
    or want to simply know the hitCount for a given query (make sure to set count=0 in the search params) 
    (hitCount is not supported by pytangle)
    """

    for year in range(2014,2019+1):

        default_params = {
            "searchTerm": "#architecture AND (#icon OR #iconic)",
            "searchField": "text_fields_only",
            "platforms": "instagram",
            "sortBy": "date",
            "startDate": "{}-01-01".format(year),
            "endDate": "{}-12-31".format(year)
        }

        query_type = "/posts/search"
        search_params_file = "ct_search_paramsUNUSED.json"

        if os.path.exists(search_params_file):
            search_params = read_json(search_params_file)
            # print("Reading search params from file")
        else:
            search_params = default_params
            # print("Using default params")

        search_params["count"] = 100
        search_params["offset"] = 0

        # print("Search Params:")
        # print(search_params, "\n")

        supported_queries = ["/posts", "/posts/search", "/leaderboard", "/links"]
        assert query_type in supported_queries


        res = ct_request(endpoints[query_type], search_params, API_KEY, debug=False)

        print(year,":", res["hitCount"], ",")
        time.sleep(10)

    # print("year,month,num_posts")
    # for year in range(2013, 2019 + 1):
    #     for month in range(1, 12 + 1):
    #         search_params["startDate"] = "{}-{:02d}-01".format(year, month)
    #         end_year = year + 1 if month == 12 else year
    #         end_month = 1 if month == 12 else month+1
    #         search_params["endDate"] = "{}-{:02d}-01".format(end_year, end_month)
    #         res = get_hitcount(query_type, search_params, API_KEY)
    #
    #         print("{},{:02d},{}".format(year, month, res))
    #         time.sleep(10)
    #
    #         # print("hit count:", res)
