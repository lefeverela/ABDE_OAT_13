from datetime import datetime
import logging
import traceback
from neurons.apify.actors import run_actor, ActorConfig
import neurons.score.reddit_score 
import xml.etree.ElementTree

from io import StringIO
from html.parser import HTMLParser

class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.text = StringIO()
    def handle_data(self, d):
        self.text.write(d)
    def get_data(self):
        return self.text.getvalue()




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
        
    def strip_tags(self, html):
        s = MLStripper()
        s.feed(html)
        return s.get_data()

    def remove_tags(self, text):
        return(''.join(xml.etree.ElementTree.fromstring(text).itertext()))
    
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
            "sort": "RELEVANCE",
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
        original_format = '%Y-%m-%dT%H:%M:%S.%f%z'
        desired_format = '%Y-%m-%dT%H:%M:%S.%fZ'



        
        filtered_input = []
        for item in input:
            try:
                print(item['content'])
                original_string = item['created_at']
                datetime_obj = datetime.strptime(original_string, '%Y-%m-%dT%H:%M:%S.%f%z')
                formatted_date = datetime_obj.strftime('%Y-%m-%dT%H:%M:%S')
                milliseconds = datetime_obj.strftime('%f')[:3]  
                corrected_output_with_milliseconds = f"{formatted_date}.{milliseconds}Z"
                filtered_input.append({
                    'id': item['id'], 
                    'url': item['url'], 
                    'text': item['content']['markdown'], 
                    'title': item['title'], 
                    'language': item['language'], 
                    'likes': item['counter']['upvote'], 
                    'dataType': 'comment',  #item['dataType'], 
                    'community': item['subreddit']['name'],
                    'username': item['author']['name'],
                    'parent':item['id'], 
                    'timestamp': corrected_output_with_milliseconds
                })
                
            except:
                traceback.print_exc()
                pass
        print(filtered_input)
        return filtered_input


if __name__ == '__main__':
    # Initialize the RedditScraperLite query mechanism with the actor configuration
    query = TrudaxRedditScraper()

    # Execute the search for the "bitcoin" search term
    search_key = "bitcoin"
    data_set = query.execute(search_queries=[search_key])
    scoring_metrics = neurons.score.reddit_score.calculateScore(responses = data_set, tag = search_key)
    print(scoring_metrics)

    # Output the data
    for item in data_set:
        print(item)
