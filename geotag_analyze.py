import sys
import re
import elasticsearch.exceptions
from bs4 import BeautifulSoup
import requests
import itertools
import collections
from collections import defaultdict as dd
import pandas as pd
from pprint import pprint

from methods import sanitize, spatial, geo

from geotag_config import (
    TWEETS_INDEX,
    TOPONYM_INDEX,
    MAX_NGRAM_LENGTH,
    MINIMUM_GRAM_LENGTH,
    NEAR_DISTANCE,
    MAX_DISTANCE_CITY_COORDINATE,
    MAX_DISTANCE_BBOX_CENTER,
    USER_ENTITY_DISTANCE,
    TweetAnalyzerCustom,
    es_tweets,
    es_toponyms,
    pg_Geotag,
    pg_spatial
)


class Base:
    def __init__(self, n_words):
        """In this function we load a lot of data that is used for analysis
        of a tweet"""
        # Get the tokens for analysis that we will limit the analysis to. If the
        # tweet text does not contain one of the tokens in this dictionary it is
        # discarded
        self.tags = self._get_tags()

        # The size order of the administrative levels. Can be used for sorting
        # locations by its size.
        self.size_order = {
            "continent": 0,
            "country": 1,
            "adm1": 2,
            "town": 3
        }

        # List of timezones per content
        self.timezones_per_continent = self._get_timezones_per_continent()
        # A dictionary to convert Twitter time zones to official time zones
        self.tz_map = self._load_tz_map()

        # The geometries of countries and continents
        countries, continents = self._load_adm_areas()
        # Get a dictonary of the geometry and the bounding box of each country and continetn
        self.paths = self._parse_paths(countries, continents)
        # A set of all the country geonameids
        self.country_geonameids = set(countries.keys())
        # A set of all amd1 geonameids
        self.adm1_map_geonameids = self._load_adm1_geonameids()
        # Dictionary to get the continent(s) a country is in
        self.country_2_continent = self._load_country_2_continent()

        # All codes representing towns and places
        self.town_codes = set(['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLC', 'PPLCH', 'PPLF', 'PPLG', 'PPLH', 'PPLL', 'PPLQ', 'PPLR', 'PPLS', 'PPLW', 'PPLX', 'STLMT'])

        # Dictionary with list of all timezones for each country
        self.time_zones_per_country = self._load_time_zones_per_country()
        # Dictonary with most common words for each language (max 1000)
        self.most_common_words = self._get_most_common_words_web(n_words)
        # Alternative names for each country
        self.country_alternative_names = self._get_alternative_names_countries()

    def get_ngrams_space_separable(self, clean_text):
        tokens = sanitize.tokenize(clean_text, remove_punctuation=True)
        ngrams = sanitize.gramify(tokens, 1, 3)
        return sanitize.discard_ngrams_with_digits(ngrams)

    def get_ngrams(self, clean_text, lang):
        return self.get_ngrams_space_separable(clean_text)

    def _load_time_zones_per_country(self):
        """Returns a dictonary with a set of timezones for each country"""
        pg_spatial.cur.execute("""
            SELECT countries.geonameid, time_zones_per_country.name
            FROM time_zones_per_country
            INNER JOIN countries
            ON time_zones_per_country.ISO2=countries.ISO2
        """)
        timezones = dd(set)
        for geonameid, time_zone_loc_name in pg_spatial.cur.fetchall():
            timezones[geonameid].add(time_zone_loc_name)
        return dict(timezones)

    def _get_timezones_per_continent(self):
        """Return a dictonary with a set of timezones for each continent"""
        df = pd.read_excel('input/tables/timezones_per_continent.xlsx')
        return {
            column: set(df[column])
            for column in df
        }

    def _load_adm1_geonameids(self):
        """Return the set of amd1 geonameids"""
        pg_spatial.cur.execute("""SELECT geonameid FROM adm1""")
        return set(geonameid for geonameid, in pg_spatial.cur.fetchall())

    def _load_adm_areas(self):
        """Return two dictionaries (countries and continents) with the
        path of each administrative area"""
        countries = {}

        pg_spatial.cur.execute("SELECT geonameid, ST_AsText(geometry) FROM countries")
        for geonameid, wkt in pg_spatial.cur.fetchall():
            if wkt:
                geom = geo.wkt_to_geom(wkt)
                path = geo.PolygonPath(geom)
                countries[geonameid] = path

        continents = {}
        pg_spatial.cur.execute("SELECT geonameid, ST_AsText(geometry) FROM continents")
        for geonameid, wkt in pg_spatial.cur.fetchall():
            if wkt:
                geom = geo.wkt_to_geom(wkt)
                path = geo.PolygonPath(geom)
                continents[geonameid] = path

        return countries, continents

    def _parse_paths(self, *args):
        """Return a dictonary with the paths and bounding boxes for all input"""
        paths = {}
        for adm_level in args:
            for geonameid, path in adm_level.items():
                bbox = path.get_extents()
                paths[geonameid] = {
                    'path': path,
                    'bbox': bbox
                }
        return paths

    def _load_tz_map(self):
        """Return a dictonary that translates a
        Twitter timezone to a official time zone"""
        pg_spatial.cur.execute("SELECT twitter_name, tz_name FROM time_zone_map")
        return {
            twitter_name: tz_name
            for twitter_name, tz_name
            in pg_spatial.cur.fetchall()
        }

    def _load_country_2_continent(self):
        """Return a dictionary with the continent(s) a country is on"""
        pg_spatial.cur.execute("SELECT geonameid, continents FROM countries")
        return {
            country: [int(c) for c in continent.split(',')]
            for country, continent in pg_spatial.cur.fetchall()
        }

    def _get_tags(self):
        """Return a dictonary with the tags used for analysis for each language
        The used tags can be changed in input/tags.txt"""
        with open('input/tags.txt', 'rb') as f:
            tags = dd(set)
            for line in f.readlines():
                tag, language = line.decode().strip().replace(' ', '').split(',')
                tags[language].add(tag)
        return dict(tags)

    def _get_alternative_names_countries(self):
        """Return a list of the alternative names for countries"""
        names = set()
        pg_spatial.cur.execute("""SELECT geonameid FROM countries""")
        for geonameid, in pg_spatial.cur.fetchall():
            pg_spatial.cur.execute(f"""SELECT name FROM geonames WHERE geonameid = {geonameid}""")
            res = pg_spatial.cur.fetchone()
            if res is None:
                continue
            name, = res
            names.add(name)
            pg_spatial.cur.execute(f"""SELECT alternate_name FROM alternate_names WHERE geonameid = {geonameid}""")
            for name, in pg_spatial.cur.fetchall():
                names.add(name)
        return names

    def _get_all_ngrams(self, language):
        """Generator that yields all ngrams from database for a certain language"""
        print("Processing {}".format(language))
        body = {
            'query': {
                'constant_score': {
                    'filter': {
                        'term': {
                            'lang': language
                        }
                    }
                }
            }
        }
        n_tweets = es_tweets.n_hits(index=TWEETS_INDEX, doc_type='tweet', body=body)
        tweets = es_tweets.scroll_through(index=TWEETS_INDEX, body=body, size=1000, source=True)
        for i, tweet in enumerate(tweets):
            if i % 10000 == 0:
                print("Finding most_common_names: {}%".format(round((i+1) / n_tweets * 100, 1)), end="\r")
            if tweet['retweet']:
                continue
            # Tokenize tweet
            clean_text = sanitize.clean_text(tweet['text'])
            tokens = sanitize.tokenize(clean_text, tweet['lang'])
            # Create ngrams from tweet up to a length of 3
            ngrams = sanitize.gramify(tokens, MAX_NGRAM_LENGTH)
            ngrams = (ngram for ngram in ngrams if not any(char.isdigit() for char in ngram))
            for n_gram in ngrams:
                if len(n_gram) >= MINIMUM_GRAM_LENGTH:
                    yield n_gram
        print()

    def _get_subset_most_common_words(self, n):
        """Queries the most common words for each language from the database, then Returns
        a dictonary with the most n common words"""
        return {
            language: set(word[0] for word in words.most_common(n))
            for language, words in self._get_most_common_words_db().items()
        }

    def _get_most_common_words_db(self):
        """Returns a counter for each language in tags"""
        return {
            language: collections.Counter(self._get_all_ngrams(language))
            for language
            in self.tags.keys()
        }

    def _get_most_common_words_web(self, n=1000):
        """Read n (max = 1000) most common words from the database"""
        if n > 1000:
            print("Can only download 1000 most common using this website - setting n to 1000")
            n = 1000

        d = {}
        pg_spatial.cur.execute("""SELECT DISTINCT language FROM most_common_words""")
        for language, in pg_spatial.cur.fetchall():
            d[language] = set()
            pg_spatial.cur.execute(f"""SELECT word FROM most_common_words WHERE language = '{language}' ORDER BY n ASC LIMIT {n}""")
            for word, in pg_spatial.cur.fetchall():
                d[language].add(word)

        return d


class TweetAnalyzer(TweetAnalyzerCustom, Base):
    """This class is meant to take a tweet analyze it and return data regarding the
    potential locations and matches with metadata. The class takes as one of its childs
    a TweetAnalyzerCustom class that with a custom function that converts the tweet
    to the proper format. See geotag_config.py"""
    def __init__(self, min_population, n_words):
        """Set some initial values and call the __init__ of the its parent classes"""
        self.min_population = min_population
        Base.__init__(self, n_words)

    def find_user_location(self, tweet):
        """Parses the location field of the user. The user field is split at a comma if present. If a comma is present,
        it is assumed that the part before the comma is the city and the second part the country. If no comma is present
        we assume that the user field specifies the country. The function returns False if not location is found, and a tuple
        otherwise. Either ('country', geonameid) or ('place', [..., ...])"""
        u_location = tweet['user']['location']
        if not u_location:
            return False
        u_location = u_location.split(',')
        u_location = [loc.strip().lower() for loc in u_location]
        if len(u_location) == 2:
            try:
                towns = es_toponyms.get(index=TOPONYM_INDEX, doc_type='unique_name', id=u_location[0])['_source']['locations']
            except (elasticsearch.exceptions.NotFoundError, ValueError):
                return False
            towns = [town for town in towns if town['feature_code'] in self.town_codes]
            if not towns:
                return False
            try:
                countries = es_toponyms.get(index=TOPONYM_INDEX, doc_type='unique_name', id=u_location[1])['_source']['locations']
            except (elasticsearch.exceptions.NotFoundError, ValueError):
                return False
            countries = set(country['geonameid'] for country in countries if country['feature_code'] == 'PCLI')
            possible_u_locations = [town for town in towns if town['country_geonameid'] in countries]
            if not possible_u_locations:
                return False
            else:
                return 'place', possible_u_locations

        elif len(u_location) == 1:
            u_location = u_location[0]
            if u_location:
                try:
                    countries = es_toponyms.get(index=TOPONYM_INDEX, doc_type='unique_name', id=u_location)['_source']['locations']
                except elasticsearch.exceptions.NotFoundError:
                    pass
                else:
                    countries = [loc for loc in countries if loc['feature_code'] == 'PCLI']
                    if len(countries) == 1:
                        return 'country', countries[0]['geonameid']
            return False
        return False

    def area_contains(self, geonameid, coordinate):
        """Returns true if given geonameid contains the coordinate, else False"""
        try:
            area = self.paths[geonameid]
        except KeyError:
            return False
        return area['bbox'].contains(*coordinate) and area['path'].contains_point(coordinate)

    def is_family(self, loc1, loc2, siblings=True):
        """Checks if 2 locations are "family". This can be geographical parent-child, or siblings.
        The sibblings behavior can be turned of by setting sibblings to False"""
        # If loc1 and loc2 are of the same type, they cannot be child-parent
        if loc1['type'] == loc2['type']:
            if siblings is False:
                return False
            elif loc1['type'] == 'town':  # Thus loc2['type'] is town as well (see above code)
                return self.is_near(loc1, loc2)
            elif loc1['type'] == 'adm1':  # Both are in the same country
                return loc1['country_geonameid'] == loc2['country_geonameid']
            else:
                return True  # Both are either countries or continents
        else:
            # sort from big to small
            locs = sorted(
                [loc1, loc2],
                key=lambda val: self.size_order[val['type']]
            )
            if locs[0]['type'] == 'continent':
                if locs[1]['type'] == 'country':
                    # If the geonameid of the contintent is in the geonameid(s) of the continents of a country
                    return locs[0]['geonameid'] in self.country_2_continent[locs[1]['country_geonameid']]
                else:  # The other one is either adm1 or town. Too small to relate to a continent. Therefore, False
                    return False
            else:
                # If both admin areas are in the same country. It doesn't matter if town, adm1 or adm2 etc.
                return locs[0]['country_geonameid'] == locs[1]['country_geonameid']

    def match_coordinates(self, location, tweet):
        """Returns true if a tweet is sent out from within the given locations, else False"""
        if location['type'] == 'continent':
            return self.area_contains(location['geonameid'], tweet['coordinates'])
        if location['type'] == 'town':
            return spatial.distance_coords(tweet['coordinates'], location['coordinates']) < MAX_DISTANCE_CITY_COORDINATE
        else:
            return self.area_contains(location['country_geonameid'], tweet['coordinates'])

    def match_bbox(self, location, tweet):
        """Returns true if a tweet is sent out from within the given locations, else False"""
        bbox = tweet['bbox']
        bbox_center = (bbox[0] + bbox[2]) / 2, (bbox[1] + bbox[3]) / 2
        if location['type'] == 'continent':
            return self.area_contains(location['geonameid'], bbox_center)
        elif location['type'] == 'town':
            return spatial.distance_coords(bbox_center, location['coordinates']) < MAX_DISTANCE_BBOX_CENTER
        else:
            return self.area_contains(location['country_geonameid'], bbox_center)

    def match_timezones(self, location, tweet):
        """Checks if a tweets timezone corresponds with a locations time zone"""
        time_zone_user = tweet['user']['time zone']
        if time_zone_user:
            try:
                time_zone_user_olsen = self.tz_map[time_zone_user]
            except KeyError:
                time_zone_user_olsen = time_zone_user
            if location['type'] == 'continent':
                try:
                    return time_zone_user_olsen in self.timezones_per_continent[location['geonameid']]
                except KeyError:
                    return False
            if location['type'] == 'country':
                try:
                    country_timezones = self.time_zones_per_country[location['geonameid']]
                except KeyError:
                    print(f"Could not find {location['geonameid']} in country timezones")
                    return False
                else:
                    return time_zone_user_olsen in country_timezones
            else:
                return location['time_zone'] == time_zone_user_olsen
        else:
            return False

    def match_user_locations(self, location, user_locations):
        """Returns true if a user location mathes given location"""
        u_loc_type, u_loc_s = user_locations
        if u_loc_type == 'country':
            if location['type'] == 'town':
                return u_loc_s == location['country_geonameid']
            elif location['type'] == 'continent':
                return location['geonameid'] in self.country_2_continent[u_loc_s]
            else:
                return location['country_geonameid'] == u_loc_s
        else:
            if location['type'] == 'town':
                for u_loc in u_loc_s:
                    distance = spatial.distance_coords(u_loc['coordinates'], location['coordinates'])
                    if distance < USER_ENTITY_DISTANCE:
                        return True
                else:
                    return False
            else:
                for u_loc in u_loc_s:
                    if location['type'] == 'continent':
                        geonameid = location['geonameid']
                    else:
                        geonameid = location['country_geonameid']
                    if self.area_contains(geonameid, u_loc['coordinates']):
                        return True
                else:
                    return False
        return False

    def get_location_type(self, loc):
        """Returns the type (town, country, amd1 or continent) of a location, based on its characteristics in the database"""
        if loc['feature_code'] in set(['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLC', 'PPLG', 'PPLR', 'PPLS', 'PPLX', 'STLMT']):
            loc['type'] = 'town'
            # Convert coordinates to tuple: more efficient for later analysis
            loc['coordinates'] = (loc['coordinates'][0], loc['coordinates'][1])
            return loc
        if loc['feature_code'] in ['ADM1', 'ADM1H', 'ADM2', 'ADM2H'] or loc['geonameid'] in self.adm1_map_geonameids:
            loc['type'] = 'adm1'
            return loc
        if loc['feature_code'] == 'PCLI':
            loc['type'] = 'country'
            return loc
        if loc['feature_code'] == 'CONT':
            loc['type'] = 'continent'
            return loc
        return None

    def is_near(self, loc1, loc2):
        """Returns true if two locations are nearby"""
        if loc1['type'] == 'town' and loc2['type'] == 'town':
            return spatial.distance_coords(loc1['coordinates'], loc2['coordinates']) < NEAR_DISTANCE
        else:
            return False

    def strip_tags(self, ngram, tags):
        for tag in tags:
            if tag in ngram:
                ngram = ngram.replace(tag, "").strip().replace("  ", " ")
        return ngram

    def analyze_tweet(self, tweet, index=None):
        """This function takes as input a tweet and returns the tweet metadata that
        is important further down the line and the potential locations of a tweet"""
        ID, tweet = self.parse_tweet(tweet)
        clean_text = sanitize.clean_text(tweet['text'])
        ngrams = self.get_ngrams(clean_text, tweet['lang'])

        # Get all the tags for analysis in a language. Sort them by lenght.
        # This is important because all keywords a tweet is found by are removed
        # before futher analysis.
        try:
            tags = sorted(self.tags[tweet['lang']], reverse=True)
        except KeyError:
            return None
        # Remove all tags from the tokens
        ngrams = [
            self.strip_tags(ngram, tags) for ngram in ngrams
        ]

        # Create set from all ngrams that are longer than the MINIMUM_GRAM_LENGTH
        # unless part of the list of alternative names for countries
        ngrams = set(
            ngram for ngram in ngrams if (
                ngram in self.country_alternative_names or (
                    len(ngram) >= MINIMUM_GRAM_LENGTH and
                    ngram not in self.most_common_words[tweet['lang']]
                )
            )
        )

        if not ngrams:
            return None

        # Search for ngrams in gazetteer
        documents = es_toponyms.mget(
            index=TOPONYM_INDEX,
            doc_type='unique_name',
            body={'ids': list(ngrams)}
        )['docs']

        # select only found documents
        documents = [doc for doc in documents if doc['found'] is True]
        if not documents:
            return None

        # Build set of toponyms that are part of other toponyms (e.g. Remove York if New York is also in the set)
        topynym_in_toponym = set()
        found_ngrams = set([doc['_id'] for doc in documents])
        for ngram in found_ngrams:
            for other_ngram in found_ngrams:
                if ngram != other_ngram:
                    if ngram in other_ngram.split(' '):
                        topynym_in_toponym.add(ngram)
                        break

        user_locations = None
        tweet_toponyms = {}

        # Loop through all documents
        for doc in documents:
            toponym = doc['_id']

            # Do not consider if toponym is part of other toponym
            if toponym in topynym_in_toponym or toponym in tags:
                continue

            # Discard all locations with a population lower than self.min_population
            doc_locations = [
                loc for loc in doc['_source']['locations']
                if loc['population'] >= self.min_population
            ]
            # Get the locaton types of all locations. Discard if none is found
            doc_locations = list(filter(None, map(self.get_location_type, doc_locations)))
            if not doc_locations:
                continue
            # If multiple locations bear the same name and are family, only keep
            # the one with the highest number of translations in the geonames
            # database. This is a proxy for the importance of the locations
            if len(doc_locations) >= 2:
                to_discard = set()
                for loc1, loc2 in itertools.combinations(doc_locations, 2):
                    if self.is_family(loc1, loc2, siblings=False):
                        sorted_locs = sorted(
                            sorted(
                                [loc1, loc2],
                                key=lambda val: self.size_order[val['type']]
                            ),
                            key=lambda val: len(val['iso-language']),
                            reverse=True
                        )
                        to_discard.add(sorted_locs[1]['geonameid'])

                if to_discard:
                    doc_locations = [
                        loc for loc in doc_locations
                        if loc['geonameid'] not in to_discard
                    ]

            # match tweet coordinates
            if 'coordinates' in tweet and tweet['coordinates']:
                for loc in doc_locations:
                    loc['coordinates match'] = self.match_coordinates(loc, tweet)
            else:
                for loc in doc_locations:
                    loc['coordinates match'] = False

            # Match tweet time zine
            if 'time zone' in tweet['user']:
                for loc in doc_locations:
                    loc['time zone'] = self.match_timezones(loc, tweet)
            else:
                for loc in doc_locations:
                    loc['time zone'] = False

            # Match tweet user location
            if 'location' in tweet['user']:
                if user_locations is None:
                    # Retuns False if none is found, otherwise list of locations
                    user_locations = self.find_user_location(tweet)
                if user_locations:
                    for loc in doc_locations:
                        loc['user home'] = self.match_user_locations(loc, user_locations)
                else:
                    for loc in doc_locations:
                        loc['user home'] = False
            else:
                for loc in doc_locations:
                    loc['user home'] = False

            # Match tweet bounding box
            # Do not consider a bounding box if a coordinate is already present.
            if 'bbox' in tweet and tweet['bbox'] and 'coordinates' not in tweet:
                for loc in doc_locations:
                    loc['bbox'] = self.match_bbox(loc, tweet)
            else:
                for loc in doc_locations:
                    loc['bbox'] = False

            for loc in doc_locations:
                loc['family'] = False

            # If other locaions are already added to the tweet_toponyms we can
            # check for family. If family is true, set both to True
            if tweet_toponyms:
                for tweet_toponym in tweet_toponyms.values():
                    for geonameid, loc1 in tweet_toponym.items():
                        for loc2 in doc_locations:
                            # Geonameid is not saved as a key-value-pair. Thus we need to add the geonameid to the dictonary before passing loc1 and loc2 to self.is_family()
                            if self.is_family(loc1, loc2):
                                loc1['family'] = True
                                loc2['family'] = True

            # Collect all neccesary information for each location and add it to the dictonary
            locs_information = {}
            for loc in doc_locations:
                geonameid = loc['geonameid']
                locs_information[geonameid] = {
                    'bbox': loc['bbox'],
                    'time zone': loc['time zone'],
                    'coordinates match': loc['coordinates match'],
                    'user home': loc['user home'],
                    'family': loc['family'],
                    'type': loc['type'],
                    'country_geonameid': loc['country_geonameid'],
                    'adm1_geonameid': loc['adm1_geonameid'],
                    'language': loc['iso-language'],
                    'geonameid': loc['geonameid'],
                    'population': loc['population'],
                    'coordinates': loc['coordinates']
                }

            tweet_toponyms[toponym] = locs_information

        if not tweet_toponyms:
            return None
        # collect all neccesary information and return
        d = {
            'toponyms': tweet_toponyms,
            'user': {'id': tweet['user']['id']},
            'date': tweet['date'],
            'text': clean_text,
            'language': tweet['lang']
        }
        if index:
            d['index'] = index

        return ID, d


if __name__ == '__main__':
    test = TweetAnalyzer(5000, 1000)
    text = 'RT @JoeyClipstar: Bow_Woooow_Signs RT to #BadBoyRecords  - The Breakfast Club http://t.co/3w58p6Sbx2 RT http://t.co/LbQU2brfpf !!!!??'
    text = 'RT @JoeyClipstar 釋內文之英文單字均可再點入查詢'
    text = '東京と広島とオランダ'
    clean_text = sanitize.clean_text(text)
    print(test.get_ngrams(clean_text, 'ja'))
