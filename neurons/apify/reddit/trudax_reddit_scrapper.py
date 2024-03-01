from datetime import datetime, timedelta
import time
import logging
import traceback
from neurons.apify.actors import run_actor, ActorConfig
#import neurons.score.reddit_score 
import multiprocessing
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
        self.memory_mbytes = 32768 
        
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


        max_post = 50
        min_post = 40
        
        run_input = {
            "debugMode": False,
            "dev_dataset_clear": False,
            "dev_dataset_enable": False,
            "dev_transform_enable": False,
            "limit": max_post,
            "mode": "posts",
            "nsfw": False,
            "query": keywords,
            "sort": "NEW",
            "timing": "hour",
            "types:gif": False,
            "types:image": False,
            "types:link": False,
            "types:poll": False,
            "types:text": True,
            "types:video": False
            }

        # HOUR REQUEST
        def first_request(run_input, results_queue):
            new_results = self.map(run_actor(self.actor_config, run_input))
            results_queue.put(["FIRST", new_results])
            return ()
        
        # DAILY REQUEST
        def second_request(run_input, results_queue):
            run_input["timing"] = "day"
            new_results = self.map(run_actor(self.actor_config, run_input))
            results_queue.put(["SECOND", new_results])
            return ()
        
        # TOP REQUEST
        def third_request(run_input, results_queue):
            run_input["sort"] = "RELEVANCE"
            run_input["timing"] = "day"
            new_results = self.map(run_actor(self.actor_config, run_input))
            results_queue.put(["THIRD", new_results])
            return ()

        # Launch 3 request in parallel
        print(datetime.now())
        results_queue = multiprocessing.Manager().Queue() 
        first_process = multiprocessing.Process(target=first_request, args=[run_input, results_queue])
        second_process = multiprocessing.Process(target=second_request, args=[run_input, results_queue])
        third_process = multiprocessing.Process(target=third_request, args=[run_input, results_queue])
        first_process.start()
        second_process.start()
        third_process.start()

        # Get results of the requests
        results = {}
        start_time = datetime.now()
        while (len(results) != 3) and ((datetime.now() - start_time ) < timedelta(minutes=1)):
            if (results_queue.qsize() > 0):
                while (results_queue.qsize() > 0):
                    message = results_queue.get()
                    results[message[0]] = message[1]
            time.sleep(1)
        print(datetime.now())

        # Check results
        starting_point = ""
        starting_list = []
        if ("FIRST" in results):
            starting_point = "FIRST"
            starting_list = results["FIRST"]
        elif ("SECOND" in results):
            starting_point = "SECOND"
            starting_list = results["SECOND"]
        elif ("RELEVANCE" in results):
            starting_point = "RELEVANCE"
            starting_list = results["RELEVANCE"]

        # Get the first ids
        list_of_ids = []
        if (len(starting_list) > 0):
            list_of_ids = [result['id'] for result in starting_list]

        # Then add until we have the quotas
        if (len(list_of_ids) < min_post) and (len(starting_list) > 0):
            for result in results: 
                for message in results[result]:
                    if (message['id'] not in list_of_ids):
                        starting_list.append(message)
                        list_of_ids.append(message['id'])

        # Return results
        return (starting_list)
        

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
                #print(item['content'])
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
                pass
        #print(filtered_input)
        return filtered_input


if __name__ == '__main__':
    # Initialize the RedditScraperLite query mechanism with the actor configuration
    query = TrudaxRedditScraper()

    # Execute the search for the "bitcoin" search term
    search_key = "bitcoin"
    data_set = query.execute(search_queries=[search_key])
    scoring_metrics = neurons.score.reddit_score.calculateScore(responses = [data_set], tag = search_key)
    print(scoring_metrics)

    
