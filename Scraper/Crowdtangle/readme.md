# Crowdtangle Scraper

Scrapes data from the crowdtangle API given some search parameters.

**Honestly, if you want to scrape Crowtdangle data an _no images_, just use the search in the web interface and download the data manually as csv.**
**Though you could also first download the text data manually from Crowdtangle and then download the images via code, see https://stackoverflow.com/questions/52185082/url-signature-expired-error-when-viewing-instagram-images-from-instagram-api**
**So overall the Crowdtangle scraper is kinda useless.**

Internally, the scraper handles:
- pausing and resuming the scrape
- splitting up requests to Crowdtangle into chunks of <10k posts
- storing the scrape results on disk (as json)
- stitching the scrape result jsons together and transforming them into tabular format
- preparing a table for the instagram content scrape 
The scraper itself uses Pytangle (https://github.com/hide-ous/pytangle).
Pytangle handles:
- delay between requests to Crowdtangle (10s)
- re-trying the requests on failure

**How to use the scraper**
1. Provide your Crowdtangle API token in `config.py`
2. Provide the search paramters for Crowdtangle in `ct_search_params.json`
Example search parameters are given in `ct_search_params.json`. 
More info about the search parameters can be found in the wiki: https://github.com/CrowdTangle/API/wiki
3. Run the scraper via `run_scraper.py`

**Chunking**
Crowdtangle returns a maximum of 10k posts per request. Also, the maximum time frame one can query CT with is 1 year.
Technically, these (up to 10k) posts are returned by CT in batches of up to 100, each requries its own request. This is handled by Pytangle. Pytangle returns the posts 1 by 1 in an iterator.
At the same time, we want to choose a chunk size that returns >>100 posts, since otherwise this would create a lot of overhead cause by CT returning only up to 100 posts per request. For example, to get 110 posts we'd need 2 requests and this would slow down the scrape.
Therefore we set up a query period via `ct_search_params.json` and break up this query period into chunks of time that return more than 1k but less than 10k posts.
To find the approprate time period for each chunk:
- Set up the search parameters for CT in `postsearch_params.json`. startDate and endDate should encompass the whole time period you're interested in (up to 1 year).
- Use the `one_query.py->get_hitcount()` function to estimate the number of responses
- Divide the total responses by 5k (leaves some leeway to 10k since the number of respones can differ for the time intervals) to get the number of chunks. Divide the total time period by the number of chunks to get the chunk size.
- Enter this number as the chunkSize in the scraper arguments.
Example:
For "searchTerm": "Hamburg", "platforms": "instagram", "startDate": "2019-01-01", "endDate": "2020-01-01" (the whole year of 2019)
We get a hit count of ~250k. `250k/5k= 50 chunks` -> `365 days/50chunks=~7.3 days per chunk".
-> we set chunk_days to 7.

**Example responses**
Example responses for a /posts/search query can be found in sample_responses/

**Sandbox for trying out different queries**
The current scraper is set up to handle /posts/search CT queries.
`one-query.py` provides some code to try out different kinds of queries.
