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

    
    async def searchSingleUrl(self, url: str, max_tweets: int):
        run_input = {
            "maxRequestRetries": 3,
            "searchMode": "live",
            "urls": [url],
            "maxTweets": max_tweets
        }
        self.actor_config = ActorConfig("heLL6fUofdPgRXZie")
        return await run_actor_async(self.actor_config, run_input)

    
    async def distributedSearchByUrl(self, urls: list, max_tweets_per_url: int = 1):
        return await asyncio.gather(*(self.searchSingleUrl(url, max_tweets=max_tweets_per_url) for url in urls))

    
    def searchByUrl(self, urls: list, max_tweets_per_url: int = 1):
        """
        Search for tweets by url.
        """
        results = asyncio.run(self.distributedSearchByUrl(urls, max_tweets_per_url))
        print(results)
        flattened_results = [item for sublist in results for item in sublist]
        return self.map(flattened_results)

    
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

        self.first_search = search_queries[0]

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
        age_in_seconds = (datetime.now(timezone.utc) - parsed_date).total_seconds()
        return {
            'id': item['id'], 
            'url': item['url'], 
            'text': item.get('text') or item['text'], 
            'likes': item['likeCount'], 
            'title': "",
            'images': images, 
            'username': item['author']['userName'],
            'hashtags': hashtags,
            'timestamp': self.format_date(parsed_date),
            'age_in_seconds': age_in_seconds
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
        first_search = self.first_search
        print("NUMBER OF ORIGINAL TWEETS " + str(len(input))) 
        for item in input:
            filtered_input.append(self.map_item(item))

        # Sort the message by their age
        sorted_message = sorted(filtered_input, key=lambda message: message['age_in_seconds'])
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
            if (first_search.lower() in str(message_to_check['text']).lower()) or (first_search.lower() in str(message_to_check['title']).lower()):
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
            if (first_search.lower() in str(message_to_check['text']).lower()) or (first_search.lower() in str(message_to_check['title']).lower()):
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
                if (first_search.lower() in str(sorted_message[ab]['text']).lower()) or (first_search.lower() in str(sorted_message[ab]['title']).lower()):
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














        
        print("NUMBER OF VALID TWEETS " + str(len(filtered_input))) 
        return filtered_input


if __name__ == '__main__':
    # Initialize the tweet query mechanism
    query = TwitterScraperV2()

    # Execute the query for the "bitcoin" search term
    data_set = query.execute(search_queries=["bitcoin"], limit_number=10)

    urls = [tweet['url'] for tweet in data_set]
    print(f"Fetched {len(urls)} urls: {urls}")
    urls = urls[0:10]

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
