import datetime
import logging
from neurons.apify.actors import run_actor, ActorConfig

# Setting up logger for debugging and information purposes
logger = logging.getLogger(__name__)

class TrudaxRedditScraper:
    """
    A class designed to scrap reddit posts based on specific search queries using the Apify platform.

    Attributes:
        actor_config (ActorConfig): Configuration settings specific to the Apify actor.
    """

    def __init__(self):
        """
        Initialize the RedditScraperLite.
        """
        self.actor_config = ActorConfig("4YJmyaThjcRuUvQZg")
        self.timeout_secs = 45
        self.memory_mbytes = 4096 

    
    def execute(self, search_queries: list = ["bittensor"], limit_number: int = 15, validator_key: str = "None", validator_version: str = None, miner_uid: int = 0) -> list:
        """
        Execute the reddit post query process using the specified search queries.

        Args:
            search_queries (list, optional): A list of search terms to be queried. Defaults to ["bittensor"].

        Returns:
            list: A list of reddit posts.
        """

        keywords = ""
        for keyword in search_queries:
            keywords += keyword



        
        run_input = {
            "debugMode": False,
            "dev_dataset_clear": False,
            "dev_dataset_enable": False,
            "dev_transform_enable": False,
            "limit": 50,
            "mode": "posts",
            "nsfw": False,
            "query": keywords,
            "sort": "COMMENTS",
            "timing": "day",
            "types:gif": False,
            "types:image": False,
            "types:link": False,
            "types:poll": False,
            "types:text": True,
            "types:video": False
            }

        print(run_input)

        return self.map(run_actor(self.actor_config, run_input))

    def map(self, input: list) -> list:
        """
        Potentially map the input data as needed. As of now, this method serves as a placeholder and simply returns the
        input data directly.

        Args:
            input (list): The data to potentially map or transform.

        Returns:
            list: The mapped or transformed data.
        """
        print(input)
        original_format = '%Y-%m-%dT%H:%M:%S.%f%z'
        desired_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        filtered_input = [{
            'id': item['id'], 
            'url': item['url'], 
            'text': item['content'], 
            'likes': item['counter']['upvote'], 
            'dataType': 'comment',  #item['dataType'], 
            'community': item['subreddit']['name'],
            'username': item['author']['name'],
            'parent': item.get('parentId'),
            'timestamp': datetime.strptime(item['created_at'], original_format).strftime(desired_format)
        } for item in input]
        return filtered_input


if __name__ == '__main__':
    # Initialize the RedditScraperLite query mechanism with the actor configuration
    query = TrudaxRedditScraper()

    # Execute the search for the "bitcoin" search term
    data_set = query.execute(search_queries=["bitcoin"])

    # Output the data
    for item in data_set:
        print(item)
