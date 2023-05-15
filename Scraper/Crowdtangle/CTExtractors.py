import sys
sys.path.append('..')
from common.base_classes import ScrapeContentExtractor
from collections import defaultdict
import pandas as pd
from common.util import read_json


class CTInstagramPostSearchExtractor(ScrapeContentExtractor):
    """
    For:
    platform: instagram
    query type: /posts/search (probably also works for /posts)
    """

    def __init__(self):
        super().__init__()

    def _extract_one(self, fpath) -> pd.DataFrame:
        """
        Takes one file and reads its contents into tabular format
        """
        rows = []
        posts = read_json(fpath)

        for post in posts:
            row = {}
            p = defaultdict(lambda: pd.NA)
            p.update(post)
            row["account_name"] = p["account"]["name"]
            row["account_handle"] = p["account"]["handle"]
            row["account_subscriberCount"] = p["account"]["subscriberCount"]  # current subs
            row["account_platformId"] = p["account"]["platformId"]
            row["date"] = p["date"]
            row["description"] = p["description"]
            row["id"] = p["id"]
            row["platform"] = p["platform"]
            row["platformId"] = p["platformId"]
            row["postUrl"] = p["postUrl"]
            row["score"] = p["score"]
            row["statistics_actual_favoriteCount"] = p["statistics"]["actual"]["favoriteCount"]
            row["statistics_actual_commentCount"] = p["statistics"]["actual"]["commentCount"]
            row["statistics_expected_favoriteCount"] = p["statistics"]["expected"]["favoriteCount"]
            row["statistics_expected_commentCount"] = p["statistics"]["expected"]["commentCount"]
            row["subscriberCount"] = p["subscriberCount"]  # subs at time of posting
            row["type"] = p["type"]
            rows.append(row)

        df = pd.DataFrame(data=rows)
        return df


class CTFacebookPostSearchExtractor(ScrapeContentExtractor):
    pass
