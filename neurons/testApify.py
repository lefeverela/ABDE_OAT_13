
from neurons.queries import get_query, QueryType, QueryProvider
import random

def random_line(a_file="keywords.txt"):
    if not os.path.exists(a_file):
        print(f"Keyword file not found at location: {a_file}")
        quit()
    lines = open(a_file).read().splitlines()
    return random.choice(lines)

twitter_query = get_query(QueryType.TWITTER, QueryProvider.TWITTER_SCRAPERV2)
search_key = [random_line()]
tweets = twitter_query.execute(search_key, 15, "NOTIMPORTANT", None, 3)
print(type(tweets))
