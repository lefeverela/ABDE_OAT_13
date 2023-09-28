import tweepy
# from tweepy import streaming
import os
from dotenv import load_dotenv
import time
import requests

from local_db.twitter_db import store_data
load_dotenv()

# Fetch bearer token from environment variables
bearer_token = os.getenv("BEARER_TOKEN")


def scrapTwitter(max_limit = 100, key = "bittensor"):
    """
    Function to scrape recent tweets based on a keyword.

    Args:
        max_limit (int): Maximum number of tweets to fetch. Default is 100.
        key (str): Keyword to search in tweets. Default is 'bittensor'.

    Returns:
        None
    """
    # Construct the URL for the Twitter API
    url = f"https://api.twitter.com/2/tweets/search/recent?query={key}&tweet.fields=created_at&max_results={max_limit}"
    payload = {}
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        }
    try:
        # Send a GET request to the Twitter API
        response = requests.request("GET", url, headers=headers, data=payload)
        # Parse the JSON response
        returnData = response.json()['data']
        # TODO: Add error handling for empty or error responses
        for twitterPost in returnData:
            # Store each tweet into the database
            store_data(twitterPost)
    except Exception as e:
        print(e)

def continuous_scrape(interval=16):
    """
    Function to continuously scrape tweets at a specified interval.

    Args:
        interval (int): Time interval (in seconds) between each scrape. Default is 16 seconds.

    Returns:
        None
    """
    while True:
        try:
            # Scrape tweets
            scrapTwitter()
            print("Scraping done. Waiting for the next round...")
            # Wait for the specified interval before the next round
            time.sleep(interval)
        except Exception as e:
            print(f"Error occurred: {e}")
            # TODO: Add error handling for continuous failures
            time.sleep(16) # Wait for 30s before trying again

if __name__ == "__main__":
    # Start the continuous scraping when the script is run directly
    continuous_scrape()





