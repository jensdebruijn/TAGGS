import os
import sys
from operator import itemgetter
from methods import dates

from db.elastic import Elastic
from db.postgresql import PostgreSQL

GEONAMES_USERNAME = ""
if not GEONAMES_USERNAME:
    print("Please fill out GEONAMES_USERNAME in geotag/config.py")
    # sys.exit()

# Folder to save GeoNames data
GEONAMES_DIR = os.path.join('input', 'GeoNames')
# Refresh the GeoNames data on new preprocessing run
REFRESH_GEONAMES_TABLES = False
# Max lenght of n-grams to use for toponym recognition
MAX_NGRAM_LENGTH = 3
# Minimum lenght of one n-gram
MINIMUM_GRAM_LENGTH = 4
# Two locations are considered 'near' if below:
NEAR_DISTANCE = 200000  # m
# When multiple entities are mentioned in the same tweet, discard them if further apart than:
MAX_DISTANCE_ENTITIES_IN_SAME_TWEET = 200000  # m
# A tweet bbox and entity are considered a match if closer than:
MAX_DISTANCE_BBOX_CENTER = 200000  # m
# A tweet coodrindate and entity are considered a match if closer than:
MAX_DISTANCE_CITY_COORDINATE = 200000  # m

# Scores given for metadata matches (relative importance)
SCORE_TYPES = {
    'coordinates match': 2,
    'user home': 1,
    'bbox': 2,
    # 'time zone': .5,
    'family': 3,
    'utc_offset': .5
}

# Name of the PostgreSQL database (lowercase)
POSTGRESQL_DB = 'taggs'
# Name of the toponym resolution table
TOPONYM_RESOLUTION_TABLE = 'toponym_resolution_table'
# Refresh time of the realtime geotagging module
REAL_TIME_TAGGER_REFRESH_TIME = 300  # sec
# Name of the Elasticsearch index with tweets
TWEETS_INDEX = 'taggs'
# Name of the Elasticsearch index with toponyms
TOPONYM_INDEX = 'toponyms'

# Update tweets in the database with their locations (flag for testing purposes)
UPDATE = False

# Connect to databases
es_tweets = Elastic()
es_toponyms = es_tweets
pg_Geotag = PostgreSQL(POSTGRESQL_DB)
pg = PostgreSQL(POSTGRESQL_DB)


# The functions below are meant to connect to your database.
class TweetAnalyzerCustom:
    # ID = ID of the tweet as str
    # tweet = {
    #     'date': '%a %b %d %H:%M:%S +0000 %Y',
    #     'user': {
    #                     'id': user ID,
    #                     'location': user location,
    #                     'time zone': user time zone,
    #     },
    #     'text': text in utf-8 - retweeted_status if retweet, otherwise text
    #     'retweet': Boolean: True or False,
    #     'lang': tweet language - must be available,
    #     'coordinates': tweets coordinates if coordinates are available and coordinates are not 0, 0.
    #     'bbox': tweet bbox as tuple if bbox is available: (West, South, East, North)
    # }
    def parse_tweet(self, tweet):
        ID = tweet['_id']
        tweet = tweet['_source']
        tweet['date'] = dates.isoformat_2_date(tweet['date'])
        return ID, tweet


class GeotagCustom:
    """Custom class for Geotag algorithm"""
    def locations_to_commit(self, fully_resolved, update=UPDATE, index=TWEETS_INDEX):
        """Run through each tweet (ID) and its resolved locations and commit that to the database.
        The function first checks with the cache if an update is neccesary"""
        for ID, locations in fully_resolved.items():
            locations = sorted(locations, key=itemgetter('toponym'))
            # Check if locations key already exists in the tweets dictionary.
            # If so, these are the locations in the database. And the code
            # in the else-block is ran to see if one or more of the locations
            # should be updated.
            # If the locations key does not exist, the db_locations are None,
            # and the new_locations are the currently assigned locations.
            try:
                db_locations = self.tweets[ID]['locations']
            except KeyError:
                db_locations = None
                new_locations = locations
            else:
                new_locations = []
                for db_loc in db_locations:
                    try:
                        new_locations.append(next(
                            loc for loc in locations
                            if loc['toponym'] == db_loc['toponym']
                            and loc['avg_score'] > db_loc['avg_score']
                        ))
                    except StopIteration:
                        new_locations.append(db_loc)

                for loc in locations:
                    try:
                        next(
                            db_loc for db_loc in db_locations
                            if db_loc['toponym'] == loc['toponym']
                        )
                    except StopIteration:
                        new_locations.append(loc)
            finally:
                if db_locations != new_locations:
                    self.tweets[ID]['locations'] = new_locations
                    if update:
                        body = {
                            'doc': {'locations': new_locations},
                            '_index': index,
                            '_type': 'tweet',
                            '_id': ID,
                            '_op_type': 'update'
                        }
                        yield body

    def commit(self, tweets):
        """Commit tweets to the database"""
        es_tweets.bulk_operation(tweets)

    def analyze_tweets(self, query):
        """Function that analyzes all tweets using analyze_tweet, it is possible to change the number
        of cores used for this function"""
        tweets = es_tweets.scroll_through(index=TWEETS_INDEX, body=query, size=1000, source=True)

        loc_tweets = dict(
            [
                (item[0], item[1]) for item in [
                    self.tweet_analyzer.analyze_tweet(tweet)
                    for tweet in tweets
                ] if item is not None
            ]
        )

        return loc_tweets
