import logging
from neurons.apify.actors import run_actor, run_actor_async, ActorConfig
from datetime import datetime, timezone, timedelta
import asyncio

# Setting up logger for debugging and information purposes
logger = logging.getLogger(__name__)

class TwitterScraperV2:
    """
    A class designed to query tweets based using the microworlds/twitter-scraper actor on the Apify platform.

    Attributes:
        actor_config (ActorConfig): Configuration settings specific to the Apify actor.
    """

    def __init__(self):
        """
        Initialize the MicroworldsTwitterScraper.
        """

        self.actor_config = ActorConfig("61RPP7dywgiy0JPD0")
        self.actor_config.memory_mbytes = 512
        self.actor_config.timeout_secs = 45

        self.keywords_past = []



    
    def execute(self, search_queries: list = ["bittensor"], limit_number: int = 15, validator_key: str = "None", validator_version: str = None, miner_uid: int = 0) -> list:
        """
        Search for tweets using search terms.

        Args:
            search_queries (list, optional): A list of search terms to be queried. Defaults to ["bittensor"].

        Returns:
            list: A list of tweets.
        """

        run_input = {
          "maxRequestRetries": 3,
          "searchMode": "live",
          "maxItems": 100,
          "minimumFavorites": 0,
          "minimumReplies": 0,
          "minimumRetweets": 0,
          "onlyImage": False,
          "onlyQuote": False,
          "onlyTwitterBlue": False,
          "onlyVerifiedUsers": False,
          "onlyVideo": False,
          "searchTerms": search_queries,
          "sort": "Latest",
          "tweetLanguage": "en"
        }

        self.searchterm = search_queries[0]

        self.keywords_past.append(search_queries)
        print(self.keywords_past)
        
        return self.map(run_actor(self.actor_config, run_input))
    
    def format_date(self, date: datetime):
        date = date.replace(tzinfo=timezone.utc)
        return date.isoformat(sep=' ', timespec='seconds')

    
    def map_item(self, item) -> dict:
        hashtags = ["#" + x["text"] for x in item.get("entities", {}).get('hashtags', [])]

        images = []

        extended_entities = item.get("extendedEntities")
        if extended_entities:
            media_urls = {m["media_key"]: m["media_url_https"] for m in extended_entities["media"] if m.get("media_url_https")}

        for media in item.get("entities", {}).get('media', []):
            media_key = media.get("media_key")
            if media.get("media_key"):
                images.append(media_urls[media_key])


        date_format = "%a %b %d %H:%M:%S %z %Y"
        parsed_date = datetime.strptime(item["createdAt"], date_format)
        return {
            'id': item['id'], 
            'url': item['url'], 
            'text': item.get('text') or item['text'], 
            'likes': item['likeCount'], 
            'images': images, 
            'username': item['author']['userName'],
            'hashtags': hashtags,
            'timestamp': self.format_date(parsed_date)
        } 

    def map(self, input: list) -> list:
        """
        Map the input data to the expected sn3 format.

        Args:
            input (list): The data to potentially map or transform.

        Returns:
            list: The mapped or transformed data.
        """
        filtered_input = []
        print("NUMBER OF ORIGINAL TWEETS " + str(len(input))) 
        for item in input:
            if (self.searchterm in item['text']):
                date_format = "%a %b %d %H:%M:%S %z %Y"
                parsed_date = datetime.strptime(item["createdAt"], date_format))
                if ((datetime.now() - parsed_date > timedelta(days=1)) and (len(filtered_input) > 40)):
                    break
                filtered_input.append(self.map_item(item))
        print("NUMBER OF VALID TWEETS " + str(len(filtered_input))) 
        return filtered_input


if __name__ == '__main__':
    # Initialize the tweet query mechanism
    query = MicroworldsTwitterScraper()

    # Execute the query for the "bitcoin" search term
    data_set = query.execute(search_queries=["bitcoin"], limit_number=10)

    urls = [tweet['url'] for tweet in data_set]
    print(f"Fetched {len(urls)} urls: {urls}")

    data_set = query.searchByUrl(urls=urls)

    verified_urls = [tweet['url'] for tweet in data_set]

    print(f"Verification returned {len(verified_urls)} tweets")
    print(f"There are {len(set(verified_urls))} unique urls")

    unverified = set(urls) - set(verified_urls)

    if len(unverified) > 0:
        print(f"Num unverified: {len(unverified)}: {unverified}, trying again with larger max_tweets")
        data_set2 = query.searchByUrl(urls=unverified, max_tweets_per_url=50)

        verified_urls2 = [tweet['url'] for tweet in data_set2]

        unverified = set(urls) - set(verified_urls) - set(verified_urls2)

        print(f"Num unverified: {len(unverified)}: {unverified}")
    else:
        print("All verified!")

    # Output the tweet data
    #for item in data_set:
    #    print(item)
