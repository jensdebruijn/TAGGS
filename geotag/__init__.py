import datetime
import sys
import pandas as pd
from operator import itemgetter

from geotag.analyze import TweetAnalyzer
from methods import clean

from geotag.config import (
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
    def __init__(self, threshold, min_population_capitalized, min_population_non_capitalized, n_words, analysis_length):
        """Get out tweet_analyzer, save the minimum score neccesary for tweets
        and if the event Geotag module is turned on, initalize the class
        for that (spinup)"""
        self.tweet_analyzer = TweetAnalyzer(min_population_capitalized, min_population_non_capitalized, n_words)
        self.threshold = threshold
        self.analysis_length = analysis_length

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
            es_tweets.build_date_query(spinup_start, start)
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
            # Loop through all potential toponyms
            for geonameid, info in geonameids.items():
                tweets = info['tweets']

                geonameid_scores = {
                    'tweet_ids': [tweet['id'] for tweet in tweets],
                    'geonameid': geonameid,
                    'type': info['type'],
                    'population': info['population'],
                    'country_geonameid': info['country_geonameid'],
                    'adm1_geonameid': info['adm1_geonameid'],
                    'coordinates': info['coordinates'],
                    'language': info['language'],
                    'abbreviations': info['abbreviations']
                }

                geonameid_avg_score = 0
                one_tweet_per_user = self.get_one_per_user(tweets)
                # Calculate for each score type the score
                for score_type in SCORE_TYPES.keys():
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
                                geonameid_avg_type_score = geonameid_type_score / len(tweets_wo_duplicates)
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
                        geonameid_avg_type_score = geonameid_type_score / len(one_tweet_per_user)

                    geonameid_scores[score_type] = {'type_score': geonameid_type_score, 'avg_type_score': geonameid_avg_type_score}
                    geonameid_avg_score += geonameid_avg_type_score

                geonameid_scores.update({
                    'avg_score': round(geonameid_avg_score, 3)
                })

                toponym_scores.append(geonameid_scores)

            # Once all scores for the topnym are collected, filter by minimum score, unless the type is country or continent
            toponym_scores = [score for score in toponym_scores if score['avg_score'] >= self.threshold or score['type'] in ['country', 'continent']]
            if toponym_scores:
                toponym_scores = sorted(
                    sorted(
                        toponym_scores,
                        key=itemgetter('population'),
                        reverse=True
                    ),
                    key=itemgetter('avg_score'),
                    reverse=True
                )
                # Pick the location with the highest score as the resolved location
                resolved_location = toponym_scores[0]
                # If all locations have a score of 0, take the one with the highest population
                # nuber.
                if resolved_location['avg_score'] == 0:
                    resolved_location = max(toponym_scores, key=itemgetter('population'))

                def find_similar_in_country(resolved_location, toponym_scores):
                    if resolved_location['type'] == 'adm1':
                        return resolved_location
                    else:
                        for toponym_score in toponym_scores:
                            if (
                                toponym_score['type'] == 'adm1' and toponym_score['country_geonameid'] == resolved_location['country_geonameid']
                            ):
                                return toponym_score
                        else:
                            return resolved_location

                if any(score['type'] in ['country', 'continent'] for score in toponym_scores):
                    resolved_location = sorted([score for score in toponym_scores if score['type'] in ['country', 'continent']], key=itemgetter('population'), reverse=True)[0]
                else:
                    resolved_location = find_similar_in_country(resolved_location, toponym_scores)

                resolved_location['toponym'] = toponym

                # If the language of the tweet matches tha language of the resolved toponym, yield those ids
                ids = [
                    tweet_id for tweet_id in resolved_location['tweet_ids']
                    if ('general' in resolved_location['language'] or
                        (
                            'abbr' in resolved_location['language'] and
                            self.tweets[tweet_id]['original_ngrams'][toponym] in resolved_location['abbreviations']
                        ) or
                        self.tweets[tweet_id]['language'] in resolved_location['language'])
                ]
                del resolved_location['tweet_ids']
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
                            'adm1_geonameid': loc['adm1_geonameid'],
                            'abbreviations': loc['abbreviations']
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
        print("analyzing: {}".format(timestep_end))
        # First delete data that is older than the timestep start
        self.delete_data(timestep_start)
        # Load new tweets into the cache
        self.tweets.update(
            self.analyze_tweets(
                es_tweets.build_date_query(query_start, timestep_end)
            )
        )

        # Get the toponym dict (toponym as key and tweets and locations as values)
        toponyms = self.tweets_to_toponyms()

        # If a tweet has multiple locations, we perfom some extra checks. So defince a set
        # which will be used to store tweet ids with multiple locations. And a seen_ids to keep
        # track of the ids we already have. (i.e. if a tweet is already in the seen_id, it is added
        # to the duplicate ids)
        resolved_locations = {}
        # Loop through the resolved toponyms and use the seend logic (its a generator). Tweet are added
        # to the resolved toponyms. However, not yet to the fully resolved toponyms, because we still want
        # to take an extra look at the duplicates
        for toponym, ids, location in self.resolve_toponyms(toponyms, timestep_end):
            location.update({'toponym': toponym})
            for tweet_id in ids:
                if tweet_id in resolved_locations:
                    resolved_locations[tweet_id].append(location)
                else:
                    resolved_locations[tweet_id] = [location]

        fully_resolved = {}
        for tweet_id, locations in resolved_locations.items():
            tweet = self.tweets[tweet_id]
            ngrams_capitalized = set(
                resolved_location['toponym']
                for resolved_location in locations if tweet['original_ngrams'][resolved_location['toponym']][0].isupper()
            )

            toponyms_to_remove = set()
            if ngrams_capitalized and len(ngrams_capitalized) != len(locations):
                for location in locations:
                    if location['toponym'] not in ngrams_capitalized and location['toponym'] not in tweet['subsetted_ngrams']:
                        toponyms_to_remove.add(location['toponym'])
                    else:
                        pass

            locations = [loc for loc in locations if loc['toponym'] not in toponyms_to_remove]

            if locations:
                fully_resolved[tweet_id] = locations

        # And finally commit everything to the database
        self.commit(self.locations_to_commit(fully_resolved))

    def history(self, start, timestep_length, end=False, realtime=False):
        """This function is the driver behind the whole historic part of the script. It
        first loads the tweets for spinup into the cache and then loops through all
        days of the analysis. If realtime is set to True, the script then starts the realtime
        funtion that does that tagging in realtime while tweets are added to the database"""
        spinup_start = start - self.analysis_length + timestep_length
        print("building spinup")
        self.build_spinup(spinup_start, start)

        timestep = 1
        timestep_end = start + timestep * timestep_length

        while not (timestep_end > datetime.datetime.utcnow() or (end and timestep_end > end)):

            timestep_start = timestep_end - self.analysis_length
            query_start = timestep_end - timestep_length
            self.analyze_timestep(timestep_start, timestep_end, query_start, realtime=False, timestep=timestep)

            timestep += 1
            timestep_end = start + timestep * timestep_length

        if realtime and not end:
            last_timestep_end = timestep_end - timestep_length
            self.realtime(last_timestep_end)

    def realtime(self, last_timestep_end=False):
        """This is the realtime geotagger"""
        self._create_toponym_resolution_table()
        while True:
            timestep_end = datetime.datetime.utcnow()

            if not last_timestep_end:
                last_timestep_end = timestep_end - self.analysis_length

            self.analyze_timestep(timestep_end - self.analysis_length, timestep_end, last_timestep_end, realtime=True)
            last_timestep_end = timestep_end
