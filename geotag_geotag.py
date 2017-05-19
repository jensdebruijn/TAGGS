import itertools
import datetime
import subprocess
import sys
import pandas as pd
from pprint import pprint
from operator import itemgetter
from collections import defaultdict as dd

from geotag_analyze import TweetAnalyzer
from methods import spatial, geo, clean
from methods.tweets import LastTweetsDict

from geotag_config import (
    TOPONYM_RESOLUTION_TABLE,
    SCORE_TYPES,
    GeotagCustom,
    es_tweets,
    pg_Geotag,
)

if sys.version_info < (3, 6):
    print("This application requires python 3.6+")
    sys.exit(1)


class Geotag(GeotagCustom):
    def __init__(self, min_score, min_population, n_words):
        """Get out tweet_analyzer, save the minimum score neccesary for tweets
        and if the event Geotag module is turned on, initalize the class
        for that (spinup)"""
        self.tweet_analyzer = TweetAnalyzer(min_population, n_words)
        self.min_score = min_score

        GeotagCustom.__init__(self)

    def _create_toponym_resolution_table(self):
        """Create the table that can be used for realtime tagging of tweets"""
        pg_Geotag.cur.execute("select exists(select * from information_schema.tables where table_name='toponym_resolution_table')")
        if not pg_Geotag.cur.fetchone()[0]:
            pg_Geotag.cur.execute("CREATE TABLE toponym_resolution_table ( \
                toponym VARCHAR(200), \
                geonameid BIGINT \
            )")
            pg_Geotag.conn.commit()

    def build_spinup(self, spinup_start, start):
        """Get tweets from just before the start, analyze them and load them
        into cache."""
        self.tweets = self.analyze_tweets(
            es_tweets.build_date_query(spinup_start, start, sort=True)
        )

    def eliminate_duplicates(self, tweets):
        """Eliminate near duplicate tweets. First the text of tweets is simply
        compared, then if the number of tweets is still greater than one, the
        consine similarity is used to eliminate tweets lower than a cosine
        similarity of {default}"""
        df = [(tweet['id'], tweet['text'], tweet['date']) for tweet in tweets]
        df = pd.DataFrame(tweets, columns=['id', 'text', 'date']).set_index('id')
        idx = df.groupby(['text'])['date'].transform(min) == df['date']
        df = df[idx]

        if len(df) > 1:
            df = clean.eliminate_near_duplicate_tweets(df)

        ids = set(df.index)
        return [tweet for tweet in tweets if tweet['id'] in ids]

    def delete_data(self, timestep_start):
        """Delete all data older than the start of the start of the timestep"""
        to_delete = set()
        for ID, tweet in self.tweets.items():
            if tweet['date'] < timestep_start:
                to_delete.add(ID)

        for ID in to_delete:
            del self.tweets[ID]

    def get_one_per_user(self, docs):
        """Get one document (tweet) per user"""
        scores_by_user = {}
        for doc in docs:
            user = doc['user']['id']
            if user in scores_by_user:
                scores_by_user[user].append(doc)
            else:
                scores_by_user[user] = [doc]
        docs_one_per_user = []
        for user, docs in scores_by_user.items():
            if len(docs) > 1:
                docs_one_per_user.append(
                    sorted(docs, key=itemgetter('date'), reverse=True)[0]
                )
            else:
                docs_one_per_user.append(docs[0])
        return docs_one_per_user

    def resolve_toponyms(self, toponyms, timestep_end):
        """This function resolves the toponyms to a location and yields for each
        toponym the tweet ids and the resolved toponym"""
        for toponym, geonameids in toponyms.items():

            toponym_scores = []
            # Loop through all potential toponyms"""
            for geonameid, info in geonameids.items():
                tweets = info['tweets']

                geonameid_scores = {
                    'geonameid': geonameid,
                    'type': info['type'],
                    'population': info['population'],
                    'country_geonameid': info['country_geonameid'],
                    'adm1_geonameid': info['adm1_geonameid'],
                    'coordinates': info['coordinates']
                }

                geonameid_avg_score = 0
                one_tweet_per_user = self.get_one_per_user(tweets)
                # Calculate for each score type the score
                for score_type, score_weight in SCORE_TYPES.items():
                    if score_type == 'family':
                        # Only if one of the scores for family is non-zero we need to compute the scores without duplicates.
                        # This is useful, because this operation takes especially long
                        if sum(tweet['scores']['family'] for tweet in tweets) > 0:
                            # Eliminate all duplicates. If non is given: cosine-similarity > 0.8
                            # Only consider the ones that have a family member anyway
                            tweets_w_family = [tweet for tweet in tweets if tweet['scores']['family'] is True]
                            if len(tweets_w_family) > 1:
                                tweets_wo_duplicates = self.eliminate_duplicates(tweets)
                            else:
                                tweets_wo_duplicates = tweets_w_family
                            if tweets_wo_duplicates:
                                # Convert numpy.int to int
                                geonameid_type_score = int(sum(
                                    tweet['scores']['family'] for tweet in tweets_wo_duplicates
                                    if ('general' in info['language'] or tweet['language'] in info['language'])
                                ))
                                geonameid_avg_type_score = score_weight * geonameid_type_score / len(tweets_wo_duplicates)
                        else:
                            geonameid_type_score = 0
                            geonameid_avg_type_score = 0

                    else:
                        # For all other types only consider one tweet per user
                        # Convert numpy.int to int
                        geonameid_type_score = int(sum(
                            tweet['scores'][score_type] for tweet in one_tweet_per_user
                            if ('general' in info['language'] or tweet['language'] in info['language'])
                        ))
                        geonameid_avg_type_score = score_weight * geonameid_type_score / len(one_tweet_per_user)

                    geonameid_scores[score_type] = {'type_score': geonameid_type_score, 'avg_type_score': geonameid_avg_type_score}
                    geonameid_avg_score += geonameid_avg_type_score

                geonameid_scores.update({
                    'avg_score': round(geonameid_avg_score, 3),
                    'language': info['language']
                })

                toponym_scores.append(geonameid_scores)

            # Once all scores for the topnym are collected, filter by minimum score, unless the type is country or continent
            toponym_scores = [score for score in toponym_scores if score['avg_score'] >= self.min_score or score['type'] in ['country', 'continent']]
            if toponym_scores:
                # Pick the location with the highest score as the resolved location
                resolved_location = max(toponym_scores, key=itemgetter('avg_score'))
                # If all locations have a score of 0, take the one with the highest population
                # nuber.
                if resolved_location['avg_score'] == 0:
                    resolved_location = max(toponym_scores, key=itemgetter('population'))

                # Buf if any of the resolved locations is a country or continent
                # those have preference (big_admin_area), unless one of the other
                # locations has more than 10% (0.1) of the highest score
                if any(score['type'] in ['country', 'continent'] for score in toponym_scores):
                    big_admin_areas = [score for score in toponym_scores if score['type'] in ['country', 'continent']]
                    if len(big_admin_areas) > 1:
                        continue
                    big_admin_area = big_admin_areas[0]
                    total_score = sum(score['avg_score'] for score in toponym_scores)
                    if total_score:
                        if not any(score['avg_score'] / total_score > 0.1 for score in toponym_scores):
                            resolved_location = big_admin_area
                    else:
                        resolved_location = big_admin_area
                # If the language of the tweet matches tha language of the resolved toponym, yield those ids
                ids = [
                    score['id'] for score in tweets
                    if ('general' in resolved_location['language'] or
                        'abbr' in resolved_location['language'] or
                        score['language'] in resolved_location['language'])
                ]
                yield toponym, ids, resolved_location

    def tweets_to_toponyms(self):
        """This function loops through all tweets and its ids and creates a dictionary
        with the toponyms as keys. Essentially just "reshuffling" information"""
        toponyms = {}
        for ID, tweet in self.tweets.items():
            date = tweet['date']
            for toponym, locations in tweet['toponyms'].items():
                if toponym not in toponyms:
                    toponyms[toponym] = {}
                for geonameid, loc in locations.items():
                    if geonameid not in toponyms[toponym]:
                        toponyms[toponym][geonameid] = {
                            'tweets': [],
                            'type': loc['type'],
                            'language': loc['language'],
                            'population': loc['population'],
                            'country_geonameid': loc['country_geonameid'],
                            'adm1_geonameid': loc['adm1_geonameid']
                        }
                        if 'coordinates' in loc:
                            toponyms[toponym][geonameid]['coordinates'] = loc['coordinates']
                    loc_tweet = {
                        'scores': {
                            key: value
                            for key, value in loc.items()
                            if key in SCORE_TYPES.keys()
                        },
                        'id': ID,
                        'text': tweet['text'],
                        'date': tweet['date'],
                        'user': tweet['user'],
                        'language': tweet['language']
                    }
                    toponyms[toponym][geonameid]['tweets'].append(loc_tweet)
        return toponyms

    def export_toponym_resolution_table(self, toponym_resolution_dict):
        """This function exports the toponym resolution dictionary to a database,
        so it can be used by the realtime tagger"""
        pg_Geotag.cur.execute("TRUNCATE TABLE {}".format(TOPONYM_RESOLUTION_TABLE))
        query = "INSERT INTO {trt} (toponym, geonameid) VALUES {values}".format(trt=TOPONYM_RESOLUTION_TABLE, values='{}')
        mogr = "(%s, %s)"
        values = ((toponym, geonameid) for toponym, geonameid in toponym_resolution_dict.items())
        pg_Geotag.commit_chunk(query, mogr, values)

    def analyze_timestep(self, timestep_start, timestep_end, query_start, realtime, timestep=False):
        """This function drives the analysis of a timestep and thus drives most other function"""
        print("analyzing: {}".format(timestep_end))
        # First delete data that is older than the timestep start
        self.delete_data(timestep_start)
        # Load new tweets into the cache
        self.tweets.update(
            self.analyze_tweets(
                es_tweets.build_date_query(query_start, timestep_end, sort=True)
            )
        )

        # Get the toponym dict (toponym as key and tweets and locations as values)
        toponyms = self.tweets_to_toponyms()

        # If the script also has to run in real-time, define a toponym resolution dict.
        # This dictionary stores the toponyms already found.
        if realtime:
            toponym_resolution_dict = {}

        # If a tweet has multiple locations, we perfom some extra checks. So defince a set
        # which will be used to store tweet ids with multiple locations. And a seen_ids to keep
        # track of the ids we already have. (i.e. if a tweet is already in the seen_id, it is added
        # to the duplicate ids)
        duplicate_ids = set()
        seen_ids = set()
        resolved_toponyms = {}
        # Loop through the resolved toponyms and use the seend logic (its a generator). Tweet are added
        # to the resolved toponyms. However, not yet to the fully resolved toponyms, because we still want
        # to take an extra look at the duplicates
        for toponym, ids, location in self.resolve_toponyms(toponyms, timestep_end):
            for tweet_id in ids:
                if tweet_id in seen_ids:
                    duplicate_ids.add(tweet_id)
                # Add to seen_ids. Does not raise an error if already in there.
                seen_ids.add(tweet_id)

            resolved_toponyms[toponym] = {
                'ids': ids,
                'location': location
            }

            if realtime:
                toponym_resolution_dict[toponym] = location

        if realtime:
            self.export_toponym_resolution_table(toponym_resolution_dict)

        duplicate_id_dict = {
            tweet_id: []
            for tweet_id in duplicate_ids
        }

        fully_resolved = {}

        def add_to_fully_resolved(tweet_id, toponym, location):
            location.update({'toponym': toponym})
            if tweet_id in fully_resolved:
                fully_resolved[tweet_id].append(location)
            else:
                fully_resolved[tweet_id] = [location]

        for toponym, info in resolved_toponyms.items():
            location = info['location']
            for tweet_id in info['ids']:
                if tweet_id in duplicate_ids:
                    # If the tweet has multiple locatons, we will take an extra look
                    duplicate_id_dict[tweet_id].append({**location, **{'toponym': toponym}})
                else:
                    # If an tweet has only one location, add it to fully resolved. This can be
                    # directly commited to the database
                    add_to_fully_resolved(tweet_id, toponym, location)

        # For the different locations of a tweet check if they are family. If they are related
        # keep all. If one of the locations has a substantially higher score than the other
        # discard the location with the lower score
        for tweet_id, locations in duplicate_id_dict.items():
            valid_locations = set()
            for loc1, loc2 in itertools.combinations(locations, 2):
                if self.tweet_analyzer.is_family(loc1, loc2):
                    valid_locations.add(loc1['geonameid'])
                    valid_locations.add(loc2['geonameid'])

            if not valid_locations:
                for loc1, loc2 in itertools.combinations(locations, 2):
                    locs = sorted([loc1, loc2], key=itemgetter('avg_score'), reverse=True)
                    if locs[0]['avg_score'] > .2 and locs[0]['avg_score'] > locs[1]['avg_score'] * 5:
                        valid_locations.add(locs[0]['geonameid'])

            locations = [
                loc for loc in locations
                if loc['geonameid'] in valid_locations
            ]

            for location in locations:
                toponym = location.pop('toponym')
                add_to_fully_resolved(tweet_id, toponym, location)

        # And finally commit everything to the database
        self.commit(self.locations_to_commit(fully_resolved))

    def history(self, start, timestep_length, analysis_length, end=False, realtime=False):
        """This function is the driver behind the whole historic part of the script. It
        first loads the tweets for spinup into the cache and then loops through all
        days of the analysis. If realtime is set to True, the script then starts the realtime
        funtion that does that tagging in realtime while tweets are added to the database"""
        spinup_start = start - analysis_length + timestep_length
        self.build_spinup(spinup_start, start)

        timestep = 1
        timestep_end = start + timestep * timestep_length

        while not (timestep_end > datetime.datetime.utcnow() or (end and timestep_end > end)):

            timestep_start = timestep_end - analysis_length
            query_start = timestep_end - timestep_length
            self.analyze_timestep(timestep_start, timestep_end, query_start, realtime=False, timestep=timestep)

            timestep += 1
            timestep_end = start + timestep * timestep_length

        if realtime and not end:
            last_timestep_end = timestep_end - timestep_length
            self.realtime(analysis_length, last_timestep_end)

    def realtime(self, analysis_length, last_timestep_end=False):
        """This is the realtime geotagger"""
        self._create_toponym_resolution_table()
        while True:
            timestep_end = datetime.datetime.utcnow()

            if not last_timestep_end:
                last_timestep_end = timestep_end - analysis_length

            self.analyze_timestep(timestep_end - analysis_length, timestep_end, last_timestep_end, realtime=True)
            last_timestep_end = timestep_end
