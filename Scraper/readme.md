# Scraper

## Crowdtangle
The Crowdtangle scraper is no longer used and just here for documentation reasons.

## Instagram-API

Scrapes instagram posts in two steps:
1. Finds posts to scrape (-> a 'feed') by some criterion: supports search by 
 - location ID
 - hashtag
 - username
2. Scrape the posts and save them in tabular format

The scraper uses the RapidAPI instagram scraper 'Instagram Data' (https://rapidapi.com/logicbuilder/api/instagram-data1/).
Using the API requires a subscription.
RapidAPI provides a playground to try out different types of queries.

This goes for both scrapers:

The scraper can be paused at any time. Pausing simply means stopping the program. On program start, the scraper will detect an unfinished scrape and resume the scrape.
Once a scrape is finished, the scraper will not scrape again. To re-do a scrape, point it at an empty scraper folder.

## 1) Feed Scraper

**Overview**

Input: 
- scrape_folder: where to save the scrape results
- search term
- search type (user/location/hashtag)

Output:
- [scrape_folder]/data: raw reponses by the RapidAPI scraper
 -> The posts will already have some metadata like shortcode, timestmap, likes, comment count etc.
- [scrape_folder]/results/result.csv: csv file with all posts combined. Each line represents one post. 

**How to use**

1. Provide the RapidAPI 'Instagram Data' API token in `api_key.py`
2. Run the main method in `InstagramFeedScraper.py`

## 2) Post Scraper
**Overview**

Input:
- csv file from the feed scrape. Has to contain the columns [id, shortcode]

Output: 
- [scrape_folder]/data: raw reponses by the RapidAPI scraper. One file is one post.
- [scrape_folder]/results/post_raw.json: all posts concatenated into one json
- [scrape_folder]/results/post_metadata.csv: all posts with partially flattened metadata to fit into csv format
- [scrape_folder]/results/post_minimal.csv: csv of all posts with small subset of columns (not used atm)
- [scrape_folder]/results/post_comments.csv: csv with all comments (one comment is one line)
- [scrape_folder]/results/post_images.csv: csv with all image files (one line is one image). Note that one post can contain multiple images.

Example responses can be found in examples/
An overview of the possible top-level keys in the responses can be found at JsonInfo/PostJsonStructure.txt

**How to use**

1. Provide the RapidAPI 'Instagram Data' API token in `api_key.py`
2. Run the main method in `InstagramPostScraper.py`

