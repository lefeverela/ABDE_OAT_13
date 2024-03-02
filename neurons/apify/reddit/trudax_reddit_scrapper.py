from datetime import datetime, timedelta, timezone
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
        first_search = ""
        for keyword in search_queries:
            keywords += keyword
            first_search = keyword

        max_post_hour = 100
        max_post = 50
        min_post = 40
        
        run_input = {
            "debugMode": False,
            "dev_dataset_clear": False,
            "dev_dataset_enable": False,
            "dev_transform_enable": False,
            "limit": max_post_hour,
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
        def first_request(run_input_local, results_queue):
            new_results = self.map(run_actor(self.actor_config, run_input_local))
            results_queue.put(["FIRST", new_results])
            return ()
        
        # DAILY REQUEST
        def second_request(run_input_local, results_queue):
            run_input_local["timing"] = "day"
            new_results = self.map(run_actor(self.actor_config, run_input_local))
            results_queue.put(["SECOND", new_results])
            return ()
        
        # TOP REQUEST
        def third_request(run_input_local, results_queue):
            run_input_local["sort"] = "RELEVANCE"
            run_input_local["timing"] = "day"
            new_results = self.map(run_actor(self.actor_config, run_input_local))
            results_queue.put(["THIRD", new_results])
            return ()

        # BACKUP
        def fourth_request(run_input_local, results_queue):
            run_input_local["sort"] = "RELEVANCE"
            run_input_local["timing"] = "week"
            new_results = self.map(run_actor(self.actor_config, run_input_local))
            results_queue.put(["FOURTH", new_results])
            return ()

        # Launch 3 request in parallel
        results_queue = multiprocessing.Manager().Queue() 
        first_process = multiprocessing.Process(target=first_request, args=[run_input.copy(), results_queue])
        second_process = multiprocessing.Process(target=second_request, args=[run_input.copy(), results_queue])
        third_process = multiprocessing.Process(target=third_request, args=[run_input.copy(), results_queue])
        fourth_process = multiprocessing.Process(target=fourth_request, args=[run_input.copy(), results_queue])
        first_process.start()
        second_process.start()
        third_process.start()
        fourth_process.start()

        # Get results of the requests
        results = {}
        start_time = datetime.now()
        while (len(results) != 4) and ((datetime.now() - start_time ) < timedelta(minutes=1)):
            if (results_queue.qsize() > 0):
                while (results_queue.qsize() > 0):
                    message = results_queue.get()
                    results[message[0]] = message[1]
            time.sleep(1)

        # Check results
        #starting_point = ""
        #if ("FIRST" in results):
        #    starting_point = "FIRST"
        #elif ("SECOND" in results):
        #    starting_point = "SECOND"
        #elif ("THIRD" in results):
        #    starting_point = "THIRD"
        #elif ("FOURTH" in results):
        #    starting_point = "FOURTH"
        
        # Get the first ids
        #list_of_ids = []
        #starting_list = []
        #if (len(results[starting_point]) > 0):
        #    for result in results[starting_point]:
        #        if (first_search.lower() in str(result['text']).lower()):
        #            list_of_ids.append(result['id'])
        #            starting_list.append(result)

        # Add all the unique messages in the list
        list_of_ids = []
        starting_list = []
        for result in results: 
            for message in results[result]:
                if (message['id'] not in list_of_ids):
                    starting_list.append(message)
                    list_of_ids.append(message['id'])
                    
        # Sort the message by their age
        sorted_message = sorted(starting_list, key=lambda message: message['age_in_seconds'])
        sorted_message_relevant = []

        # Compute an estimage max average age
        max_average_age = 0
        for i in range(0, min(len(sorted_message), 20)):
            max_average_age += sorted_message[i]['age_in_seconds']
        if (max_average_age != 0):
            max_average_age = max_average_age / min(len(sorted_message), 20)
        
        # Compute the score of the list has we add messages
        max_length = 100
        relevant_count = 0
        age_sum_relevant, age_sum_all = 0, 0
        age_contribution_relevant = 0
        for i in range(0, len(sorted_message)):

            # Extract the current message we are inspecting
            message_to_check = sorted_message[i]
            nb_message_to_send = i + 1

            # Check if the message is relevant
            if (first_search.lower() in str(message_to_check['text']).lower()):
                relevant_count += 1
                age_sum_relevant +=  message_to_check['age_in_seconds']

            # Compute our length contribution
            length_contribution_relevant = (relevant_count + 1) / (max(max_length, relevant_count) + 1) * 0.3
            length_contribution_all = (nb_message_to_send + 1) / (max(max_length, nb_message_to_send) + 1) * 0.3

            # Compute age contribution
            age_sum_all += message_to_check['age_in_seconds']
            if (relevant_count > 0):
                age_contribution_relevant = (1 - (age_sum_relevant / relevant_count + 1) / (max(max_average_age, age_sum_relevant / relevant_count) + 1)) * 0.4
            age_contribution_all = (1 - (age_sum_all / nb_message_to_send + 1) / (max(max_average_age, age_sum_all / nb_message_to_send ) + 1)) * 0.4

            # Compute relevancy contribution
            if (relevant_count > 0):
                relevancy_contribution_relevant = relevant_count / relevant_count * 0.2
            else:
                relevancy_contribution_relevant = 0
            relevancy_contribution_all = relevant_count / nb_message_to_send * 0.2

            # Compute final score for all messages
            sorted_message[i]['score_messages_all'] = relevancy_contribution_all + length_contribution_all + age_contribution_all
            if (i > 0):
                sorted_message[i]['contribution_all'] = sorted_message[i]['score_messages_all'] - sorted_message[i-1]['score_messages_all']
            else:
                sorted_message[0]['contribution_all'] = sorted_message[0]['score_messages_all']

            # Compute final score if we are in a relevant message
            if (first_search.lower() in str(message_to_check['text']).lower()):
                relevant_message = message_to_check.copy()
                relevant_message['score_messages_relevant'] = relevancy_contribution_relevant + length_contribution_relevant + age_contribution_relevant
                if (len(sorted_message_relevant) > 0):
                    relevant_message['contribution_relevant'] = relevant_message['score_messages_relevant'] - sorted_message_relevant[len(sorted_message_relevant)-1]['score_messages_relevant']
                else:
                    relevant_message['contribution_relevant'] = relevant_message['score_messages_relevant']
                sorted_message_relevant.append(relevant_message)

        # Compute the max score we can get using index methodology
        max_relevant, index_relevant = 0, 0
        max_all, index_all = 0, 0
        for ab in range (5, len(sorted_message_relevant)):
            if (sorted_message_relevant[ab]['score_messages_relevant'] >= max_relevant):
                max_relevant = sorted_message_relevant[ab]['score_messages_relevant']
                index_relevant = ab
                
        for ab in range (5, len(sorted_message)):
            if (sorted_message[ab]['score_messages_all'] >= max_all):
                max_all = sorted_message[ab]['score_messages_all']
                index_all = ab
                
        # Compute new message group using the contribution factor
        contribution_relevant = []
        contribution_all = []
        contribution_relevant_count = 0
        age_sum_contribution_relevant = 0
        age_sum_contribution_all = 0
        for ab in range (0, len(sorted_message_relevant)):
            if (sorted_message_relevant[ab]['contribution_relevant'] > 0):
                contribution_relevant.append(sorted_message_relevant[ab])
                age_sum_contribution_relevant += sorted_message_relevant[ab]['age_in_seconds']
                
        for ab in range (0, len(sorted_message)):
            if (sorted_message[ab]['contribution_all'] > 0):
                contribution_all.append(sorted_message[ab])
                age_sum_contribution_all += sorted_message[ab]['age_in_seconds']
                if (first_search.lower() in str(sorted_message[ab]['text']).lower()):
                    contribution_relevant_count += 1
                    
        # Then compute the score of those 2 new groups
        # Group 1
        length_contribution = (len(contribution_relevant) + 1) / (max(max_length, len(contribution_relevant)) + 1) * 0.3
        relevancy_contribution = 0.2
        if (len(contribution_relevant) > 0):
            age_contribution = (1 - (age_sum_contribution_relevant / len(contribution_relevant) + 1) / (max(max_average_age, age_sum_contribution_relevant / len(contribution_relevant)) + 1)) * 0.4
        else:
            age_contribution = 0
        score_contribution_relevant = relevancy_contribution + length_contribution + age_contribution
        
        # Group 2
        if (len(contribution_all) > 0):
            length_contribution = (len(contribution_all) + 1) / (max(max_length, len(contribution_all)) + 1) * 0.3
            relevancy_contribution = contribution_relevant_count / len(contribution_all) * 0.2
            age_contribution = (1 - (age_sum_contribution_all / len(contribution_all) + 1) / (max(max_average_age, age_sum_contribution_all / len(contribution_all)) + 1)) * 0.4
            score_contribution_all = relevancy_contribution + length_contribution + age_contribution
        else:
            score_contribution_all = 0

        print("MOST RELEVANT INDEX " + str(index_relevant) + ", " + str(max_relevant))
        print("MOST RELEVANT INDEX ALL " + str(index_all) + ", " + str(max_all))
        print("MOST RELEVANT CONTRIBUTION RELEVANT " + str(len(contribution_relevant)) + ", " + str(score_contribution_relevant))
        print("MOST RELEVANT CONTRIBUTION ALL " + str(len(contribution_all)) + ", " + str(score_contribution_all))

        if (max(max_relevant, max_all, score_contribution_relevant, score_contribution_all) == max_relevant):
            return (sorted_message_relevant[0 : index_relevant])
        elif (max(max_relevant, max_all, score_contribution_relevant, score_contribution_all) == max_all):
            return (sorted_message[0 : index_all])
        elif (max(max_relevant, max_all, score_contribution_relevant, score_contribution_all) == score_contribution_relevant):
            return (contribution_relevant)
        elif (max(max_relevant, max_all, score_contribution_relevant, score_contribution_all) == score_contribution_all):
            return (contribution_all)
            
        
        # Then add until we have the quotas
        #if ((len(list_of_ids) < min_post) and (len(starting_list) > 0) and (len(starting_list) < max_post)):
        #    for result in results: 
        #        for message in results[result]:
        #            if ((message['id'] not in list_of_ids) and (len(starting_list) < max_post) and (first_search.lower() in str(message['text']).lower())):
        #                starting_list.append(message)
        #                list_of_ids.append(message['id'])

        # Return results
        return (sorted_message)
        

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
                age_in_seconds = (datetime.now(timezone.utc) - datetime_obj).total_seconds()
                
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
                    'timestamp': corrected_output_with_milliseconds,
                    'age_in_seconds': age_in_seconds
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

    
