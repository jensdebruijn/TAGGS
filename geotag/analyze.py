from pytz import all_timezones, timezone
from re import compile
import elasticsearch.exceptions
from itertools import combinations
from datetime import timedelta, datetime
from operator import itemgetter
from collections import defaultdict as dd
from collections import OrderedDict
import pandas as pd

from methods import sanitize, spatial, geo

from geotag.config import (
    TWEETS_INDEX,
    TOPONYM_INDEX,
    MAX_NGRAM_LENGTH,
    MINIMUM_GRAM_LENGTH,
    NEAR_DISTANCE,
    MAX_DISTANCE_CITY_COORDINATE,
    MAX_DISTANCE_BBOX_CENTER,
    SCORE_TYPES,
    TweetAnalyzerCustom,
    es_tweets,
    es_toponyms,
    pg
)

first_word_recognizer = compile('(?:^|(?:[.!?:]\s))(\w+)')


class LastUserLocationDict(OrderedDict):
    def __init__(self, size, *args, **kwargs):
        self.size = 10000
        self.pop = False
        OrderedDict.__init__(self, *args, **kwargs)

    def getandmove(self, key):
        self.move_to_end(key)
        return OrderedDict.__getitem__(self, key)

    def __setitem__(self, key, value):
        OrderedDict.__setitem__(self, key, value)
        if not self.pop and len(self) > self.size:
            self.pop = True
        if self.pop:
            self.popitem(last=False)


class Offset2TimeZones(dict):
    def __missing__(self, key):
        self[key] = []

    def find_timezones(self, offset, dt, timezone=timezone, all_timezones_set=set(all_timezones)):
        maximum_date = dt + timedelta(days=365)
        td = timedelta(days=3)
        tzs = {
            tz.zone for tz in map(timezone, all_timezones_set)
            if dt.astimezone(tz).utcoffset() == offset
        }

        while True:
            dt += td

            future_tzs = {
                tz.zone for tz in map(timezone, all_timezones_set)
                if dt.astimezone(tz).utcoffset() == offset
            }
            if future_tzs != tzs:
                dt = dt-td
                break
            if dt > maximum_date:
                return tzs, dt-td

        td = timedelta(hours=1)
        while True:
            dt += td

            future_tzs = {
                tz.zone for tz in map(timezone, all_timezones_set)
                if dt.astimezone(tz).utcoffset() == offset
            }
            if dt > maximum_date or future_tzs != tzs:
                return tzs, dt - td

    def get_tz(self, offset, dt):
        offset = timedelta(seconds=offset)
        if not self[offset]:
            tzs, end = self.find_timezones(offset, dt)
            self[offset].append({
                "tzs": tzs,
                "start": datetime(1970, 1, 1),
                "end": end
            })
            return tzs
        else:
            for tz in self[offset][::-1]:
                if tz["end"] > dt >= tz["start"]:
                    return tz["tzs"]
            else:
                tzs, end = self.find_timezones(offset, dt)
                self[offset].append({
                    "tzs": tzs,
                    "start": self[offset][-1]["end"],
                    "end": end
                })
                return tzs


class Base:
    def __init__(self, n_words):
        """In this function we load a lot of data that is used for analysis
        of a tweet"""
        # Get the tokens for analysis that we will limit the analysis to. If the
        # tweet text does not contain one of the tokens in this dictionary it is
        # discarded
        self.tags = self._get_tags()
        self.toponym_capitalization = self._get_language_info()

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
        self.town_codes = set(['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLC', 'PPLCH', 'PPLF', 'PPLG', 'PPLH', 'PPLL', 'PPLQ', 'PPLR', 'PPLS', 'PPLW', 'STLMT'])

        # Dictionary with list of all timezones for each country
        self.time_zones_per_country = self._load_time_zones_per_country()
        # Alternative names for each country
        country_alternative_names = self._get_alternative_names_countries()
        self.country_alternative_names_set = set(country_alternative_names)

        adm1_alternative_names = self._get_alternative_names_adm1()

        self.adm_names = country_alternative_names

        for name, geonameids in adm1_alternative_names.items():
            if name not in self.adm_names:
                self.adm_names[name] = geonameids
            else:
                self.adm_names[name].update(geonameids)

        # Dictonary with most common words for each language (max 10000)
        self.most_common_words = self._get_most_common_words(n_words)

        self.offset2timezones = Offset2TimeZones()

    def get_ngrams_space_separable(self, clean_text):
        tokens = sanitize.tokenize(clean_text, remove_punctuation=True)
        ngrams = sanitize.gramify(tokens, 1, 3)
        return sanitize.discard_ngrams_with_digits(ngrams)

    def get_ngrams(self, clean_text, lang):
        return self.get_ngrams_space_separable(clean_text)

    def _get_language_info(self):
        df = pd.read_excel('input/tables/languages.xlsx')
        toponym_capitalization = df.set_index('language_code')['toponym_captitalization'].replace('Yes', True).to_dict()
        return toponym_capitalization

    def _load_time_zones_per_country(self):
        """Returns a dictonary with a set of timezones for each country"""
        pg.cur.execute("""
            SELECT countries.geonameid, time_zones_per_country.name
            FROM time_zones_per_country
            INNER JOIN countries
            ON time_zones_per_country.ISO2=countries.ISO2
        """)
        timezones = dd(set)
        for geonameid, time_zone_loc_name in pg.cur.fetchall():
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
        pg.cur.execute("""SELECT geonameid FROM adm1""")
        return set(geonameid for geonameid, in pg.cur.fetchall())

    def _load_adm_areas(self):
        """Return two dictionaries (countries and continents) with the
        path of each administrative area"""
        countries = {}

        pg.cur.execute("SELECT geonameid, ST_AsText(geom) FROM countries")
        for geonameid, wkt in pg.cur.fetchall():
            if wkt:
                geom = geo.wkt_to_geom(wkt)
                path = geo.PolygonPath(geom)
                countries[geonameid] = path

        continents = {}
        pg.cur.execute("SELECT geonameid, ST_AsText(geom) FROM continents")
        for geonameid, wkt in pg.cur.fetchall():
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
        pg.cur.execute("SELECT twitter_name, tz_name FROM time_zone_map")
        return {
            twitter_name: tz_name
            for twitter_name, tz_name
            in pg.cur.fetchall()
        }

    def _load_country_2_continent(self):
        """Return a dictionary with the continent(s) a country is on"""
        pg.cur.execute("SELECT geonameid, continents FROM countries")
        return {
            country: [int(c) for c in continent.split(',')]
            for country, continent in pg.cur.fetchall()
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
        names = dd(dict)
        pg.cur.execute("""SELECT geonameid FROM countries""")
        for geonameid, in pg.cur.fetchall():
            pg.cur.execute(f"""SELECT name, full_name, population, country_geonameid, adm1_geonameid FROM geonames WHERE geonameid = {geonameid}""")
            res = pg.cur.fetchone()
            if res is None:
                continue
            name, full_name, population, country_geonameid, adm1_geonameid = res
            if name not in names:
                names[name] = {}

            geonameid_info = {
                'type': 'country',
                'abbreviations': [],
                "toponym": name,
                "geonameid": geonameid,
                "population": population,
                "country_geonameid": country_geonameid,
                "adm1_geonameid": adm1_geonameid
            }
            names[name][geonameid] = geonameid_info

            pg.cur.execute(f"""SELECT alternate_name, isolanguage, full_name FROM alternate_names WHERE geonameid = {geonameid}""")
            for name, isolanguage, full_name in pg.cur.fetchall():
                if name not in names:
                    names[name] = {}
                if geonameid not in names[name]:
                    names[name][geonameid] = geonameid_info
                if isolanguage == 'abbr':
                    names[name][geonameid]['abbreviations'].append(full_name)
        return names

    def _get_alternative_names_adm1(self):
        """Return a list of the alternative names for adm1"""
        names = dd(set)
        pg.cur.execute("""
            SELECT geonameid
            FROM geonames
            WHERE feature_code IN ('ADM1', 'ADM1H', 'ADM2', 'ADM2H')
                OR geonames.geonameid IN (
                    SELECT adm1.geonameid FROM adm1
                )
        """)
        for geonameid, in pg.cur.fetchall():
            pg.cur.execute(f"""SELECT name, full_name, population, country_geonameid, adm1_geonameid FROM geonames WHERE geonameid = {geonameid}""")
            res = pg.cur.fetchone()
            if res is None:
                continue
            name, full_name, population, country_geonameid, adm1_geonameid = res
            if name not in names:
                names[name] = {}

            geonameid_info = {
                'type': 'adm1',
                'abbreviations': [],
                "toponym": name,
                "geonameid": geonameid,
                "population": population,
                "country_geonameid": country_geonameid,
                "adm1_geonameid": adm1_geonameid
            }
            names[name][geonameid] = geonameid_info

            pg.cur.execute(f"""SELECT alternate_name, isolanguage, full_name FROM alternate_names WHERE geonameid = {geonameid}""")
            for name, isolanguage, full_name in pg.cur.fetchall():
                if name not in names:
                    names[name] = {}
                if geonameid not in names[name]:
                    names[name][geonameid] = geonameid_info
                if isolanguage == 'abbr':
                    names[name][geonameid]['abbreviations'].append(full_name)
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

    def _get_most_common_words(self, n):
        """Read n (max = 10000) most common words from the database"""
        if n > 10000:
            print("Can only download 1000 most common using this website - setting n to 1000")
            n = 10000

        pg.cur.execute("""SELECT name FROM geonames WHERE population > 100000""")
        places_high_population = set(name for name, in pg.cur.fetchall())

        d = {}
        pg.cur.execute("""SELECT DISTINCT language FROM most_common_words""")
        for language, in pg.cur.fetchall():
            d[language] = set()
            pg.cur.execute(f"""SELECT word FROM most_common_words WHERE language = '{language}' ORDER BY n ASC LIMIT {n}""")
            for word, in pg.cur.fetchall():
                word = word.lower()
                if word not in places_high_population:
                    d[language].add(word)

        return d


class TweetAnalyzer(TweetAnalyzerCustom, Base):
    """This class is meant to take a tweet analyze it and return data regarding the
    potential locations and matches with metadata. The class takes as one of its childs
    a TweetAnalyzerCustom class that with a custom function that converts the tweet
    to the proper format. See geotag_config.py"""
    def __init__(self, min_population_capitalized, min_population_non_capitalized, n_words):
        """Set some initial values and call the __init__ of the its parent classes"""
        self.min_population_capitalized = min_population_capitalized
        self.min_population_non_capitalized = min_population_non_capitalized
        Base.__init__(self, n_words)

        self.lastuserlocationdict = LastUserLocationDict(10000)

    def extract_user_locations_child(self, child, original_name, parent_name, parent_info):
        try:
            locations = es_toponyms.get(index=TOPONYM_INDEX, doc_type='unique_name', id=child)['_source']['locations']
        except (elasticsearch.exceptions.NotFoundError, ValueError):
            return [parent_info]
        else:
            locations = sorted(locations, key=itemgetter('population'), reverse=True)
            if locations[0]['population'] == 0:
                return [parent_info]
            else:
                for loc in locations:
                    loc = self.get_location_type(loc)
                    if loc and ('abbr' not in loc['iso-language'] or original_name in loc['abbreviations'] and self.is_family(loc, parent_info, child, parent_name, siblings=False)):
                        loc.update({"toponym": child})
                        return [loc]
                else:
                    return [parent_info]

    def find_user_location_town(self, name, original_name):
        try:
            locations = es_toponyms.get(index=TOPONYM_INDEX, doc_type='unique_name', id=name)['_source']['locations']
        except (elasticsearch.exceptions.NotFoundError, ValueError):
            return []
        else:
            locations = sorted(locations, key=itemgetter('population'), reverse=True)
            if locations[0]['population'] < 10000:
                return []
            else:
                for loc in locations:
                    if 'abbr' not in loc['iso-language'] or original_name in loc['abbreviations']:
                        loc = self.get_location_type(loc)
                        if loc:
                            loc.update({"toponym": name})
                            return [loc]
                else:
                    return []

    def find_user_location(self, u_location):
        """Parses the location field of the user. The user field is split at a comma if present. If a comma is present,
        it is assumed that the part before the comma is the city and the second part the country. If no comma is present
        we assume that the user field specifies the country. The function returns False if not location is found, and a tuple
        otherwise. Either ('country', geonameid) or ('place', [..., ...])"""
        if not u_location:
            return []

        if '/' in u_location:
            return [
                loc for split in u_location.split('/') for loc in self.find_user_location(split)
            ]

        if ' and ' in u_location:
            return [
                loc for split in u_location.split(' and ') for loc in self.find_user_location(split)
            ]

        if '&' in u_location:
            return [
                loc for split in u_location.split('&') for loc in self.find_user_location(split)
            ]

        u_location = u_location.strip().replace('.', '')
        u_location_lower = u_location.lower()

        u_location_splitted_comma = u_location_lower.split(',')
        if len(u_location_splitted_comma) == 1:
            u_location_lower_splitted_space = u_location_lower.split(' ')
            for i in range(1, len(u_location_lower_splitted_space)+1):
                name = ' '.join(u_location_lower_splitted_space[-i:])
                try:
                    parent_geonameids = self.adm_names[name]
                except KeyError:
                    continue
                else:
                    original_name = ' '.join(u_location.split(' ')[-len(name.split(' ')):])
                    parent_geonameids = {
                        geonameid: geonameid_info
                        for geonameid, geonameid_info
                        in parent_geonameids.items()
                        if not geonameid_info['abbreviations'] or original_name in geonameid_info['abbreviations']
                    }
                    if parent_geonameids:
                        break
            else:
                return self.find_user_location_town(u_location_lower, u_location)
            child = u_location_lower[:-len(name)].strip()
            if child:
                original_name_i = u_location_lower.index(child)
                original_name = u_location[original_name_i:original_name_i+len(child)]
                locations = []
                for parent_geonameid, parent_info in parent_geonameids.items():
                    locations.extend(self.extract_user_locations_child(child, name, original_name, parent_info))
                return locations
            else:
                return parent_geonameids.values()
        elif len(u_location_splitted_comma) == 2:
            child, parent = u_location_splitted_comma
            child, parent = child.strip(), parent.strip()
            try:
                parent_geonameids = self.adm_names[parent]
            except KeyError:
                return self.find_user_location_town(parent, u_location.split(',')[-1].strip())
            original_parent_name = u_location.split(',')[-1].strip()
            parent_geonameids = {
                geonameid: geonameid_info
                for geonameid, geonameid_info
                in parent_geonameids.items()
                if not geonameid_info['abbreviations'] or original_parent_name in geonameid_info['abbreviations']
            }
            if not parent_geonameids:
                return self.find_user_location_town(parent, original_parent_name)
            locations = []
            for parent_geonameid, parent_info in parent_geonameids.items():
                locations.extend(self.extract_user_locations_child(child, parent, original_parent_name, parent_info))
            return locations
        elif len(u_location_splitted_comma) == 3:
            u_location_original_splitted = u_location.split(',')
            return self.find_user_location(' '.join([u_location_original_splitted[0] + u_location_original_splitted[-2]]))
        else:
            return []

    def find_time_zones_tweet(self, tweet):
        return self.offset2timezones.get_tz(tweet['user']['utc_offset'], tweet['date'])

    def area_contains(self, geonameid, coordinate):
        """Returns true if given geonameid contains the coordinate, else False"""
        try:
            area = self.paths[geonameid]
        except KeyError:
            return False
        return area['bbox'].contains(*coordinate) and area['path'].contains_point(coordinate)

    def is_family(self, loc1, loc2, toponym1, toponym2, siblings=True, consider_toponym_length=True, consider_population=False):
        """Checks if 2 locations are "family". This can be geographical parent-child, or siblings.
        The siblings behavior can be turned of by setting siblings to False"""
        # If loc1 and loc2 are of the same type, they cannot be child-parent
        if loc1['type'] == loc2['type']:
            if not siblings:
                return False
            elif loc1['type'] == 'town':  # Thus loc2['type'] is town as well (see code above)
                if consider_population:
                    min_population = 5000
                else:
                    min_population = 1
                if loc1['population'] >= min_population and loc2['population'] >= min_population:
                    return self.is_near(loc1, loc2)
                else:
                    return False
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
                if locs[0]['type'] == 'country':
                    return locs[0]['country_geonameid'] == locs[1]['country_geonameid']
                elif consider_toponym_length:
                    if len(toponym1) >= 7 and len(toponym2) >= 7:
                        return locs[0]['geonameid'] == locs[1]['adm1_geonameid']
                    else:
                        return False
                else:
                    return locs[0]['geonameid'] == locs[1]['adm1_geonameid'] or locs[0]['adm1_geonameid'] == locs[1]['adm1_geonameid']

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

    def match_offset(self, location, timezones):
        if location['type'] == 'continent':
            try:
                continent_timezones = self.timezones_per_continent[location['geonameid']]
                return len(timezones & continent_timezones) > 0
            except KeyError:
                return False
        elif location['type'] == 'country':
            try:
                country_timezones = self.time_zones_per_country[location['geonameid']]
                return len(timezones & country_timezones) > 0
            except KeyError:
                print(f"Could not find {location['geonameid']} in country timezones")
                return False
        else:
            return location['time_zone'] in timezones

    def match_user_locations(self, location, toponym, user_locations):
        """Returns true if a user location mathes given location"""
        user_location = sorted(user_locations, key=itemgetter('population'), reverse=True)[0]
        if self.is_family(location, user_location, toponym, user_location['toponym']):
            if user_location['type'] == 'country' and location['type'] != 'country':
                return min(location['population'] / user_location['population'], 1)
            else:
                return 1
        else:
            return 0

    def get_location_type(self, loc):
        """Returns the type (town, country, amd1 or continent) of a location, based on its characteristics in the database"""
        if loc['feature_code'] in set(['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLC', 'PPLG', 'PPLR', 'PPLS', 'STLMT']):
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

    def strip_tags(self, ngrams, tags):
        new_ngrams = []
        subsetted_ngrams = set()
        for ngram in ngrams:
            subsetted = False
            for tag in tags:
                while True:
                    try:
                        i = ngram.lower().index(tag)
                    except ValueError:
                        break
                    else:
                        new_ngram = (ngram[:i] + ngram[i + len(tag):]).strip().replace('  ', ' ')
                        new_ngrams.append(new_ngram)
                        subsetted_ngrams.add(new_ngram)
                        subsetted = True
                        break
                if subsetted:
                    break
            else:
                new_ngrams.append(ngram)
        return new_ngrams, subsetted_ngrams

    def find_first_letter_original_ngram(self, text, ngram):
        return text[text.lower().index(ngram)]

    def get_ngrams_space_separable(self, clean_text):
        tokens = sanitize.tokenize(clean_text, remove_punctuation=True)
        ngrams = sanitize.gramify(tokens, 1, 3)
        return sanitize.discard_ngrams_with_digits(ngrams)

    def analyze_tweet(self, tweet, index=None):
        """This function takes as input a tweet and returns the tweet metadata that
        is important further down the line and the potential locations of a tweet"""
        tweet_id, tweet = self.parse_tweet(tweet)
        clean_text = sanitize.clean_text(tweet['text'], lower=False)
        ngrams = self.get_ngrams_space_separable(clean_text)

        # Get all the tags for analysis in a language. Sort them by lenght.
        # This is important because all keywords a tweet is found by are removed
        # before futher analysis.
        try:
            tags = sorted(self.tags[tweet['lang']], reverse=True)
        except KeyError:
            return None

        # Remove all tags from the tokens
        ngrams, subsetted_ngrams = self.strip_tags(ngrams, tags)

        ngrams = [ngram for ngram in ngrams if ngram]

        lower_case_ngrams = []
        ngrams_orgininal = {}
        for ngram in ngrams:
            ngram_lower = ngram.lower()

            if ngram_lower not in ngrams_orgininal:
                ngrams_orgininal[ngram_lower] = ngram
            elif not ngrams_orgininal[ngram_lower].istitle():
                ngrams_orgininal[ngram_lower] = ngram
            lower_case_ngrams.append(ngram_lower)

        # Create set from all ngrams that are longer than the MINIMUM_GRAM_LENGTH
        # unless part of the list of alternative names for countries
        lower_case_ngrams = set(
            ngram for ngram in lower_case_ngrams if (
                ngram in self.country_alternative_names_set or (
                    len(ngram) >= MINIMUM_GRAM_LENGTH and
                    ngram not in self.most_common_words[tweet['lang']]
                )
            )
        )

        if not lower_case_ngrams:
            return None

        # Search for ngrams in gazetteer
        documents = es_toponyms.mget(
            index=TOPONYM_INDEX,
            doc_type='unique_name',
            body={'ids': list(lower_case_ngrams)}
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
                    if ' ' + ngram + ' ' in ' ' + other_ngram + ' ':
                        if ngrams_orgininal[ngram][0].isupper():
                            other_ngram_parts = ngrams_orgininal[other_ngram].split(' ')
                            if all(other_ngram_part[0].isupper() for other_ngram_part in other_ngram_parts):
                                topynym_in_toponym.add(ngram)
                            else:
                                topynym_in_toponym.add(other_ngram)
                        else:
                            topynym_in_toponym.add(ngram)

        user_locations = None
        timezones = None
        tweet_toponyms = {}
        toponym_capitalization = self.toponym_capitalization[tweet['lang']]
        if toponym_capitalization:
            first_word_sentences = first_word_recognizer.findall(clean_text)
            first_word_sentences = set(word.lower() for word in first_word_sentences)

        # Loop through all documents
        for doc in documents:
            toponym = doc['_id']

            # Do not consider if toponym is part of other toponym
            if toponym in topynym_in_toponym or toponym in tags:
                continue

            # Discard all locations with a population lower than self.min_population
            if toponym_capitalization and toponym not in first_word_sentences and ngrams_orgininal[toponym].istitle():
                doc_locations = [
                    loc for loc in doc['_source']['locations']
                    if loc['population'] >= self.min_population_capitalized
                ]
            else:
                doc_locations = [
                    loc for loc in doc['_source']['locations']
                    if loc['population'] >= self.min_population_non_capitalized
                ]

            # Get the locaton types of all locations. Discard if none is found
            doc_locations = list(filter(None, map(self.get_location_type, doc_locations)))
            if not doc_locations:
                continue
            # If multiple locations bear the same name and are family, only keep
            # the one with the highest number of translations in the geonames
            # database. This is a proxy for the importance of the locations
            if len(doc_locations) > 1:
                to_discard = set()
                for loc1, loc2 in combinations(doc_locations, 2):
                    if self.is_family(loc1, loc2, toponym, toponym, siblings=False, consider_toponym_length=False):
                        sorted_locs = sorted(
                            sorted(
                                [loc1, loc2],
                                key=lambda val: self.size_order[val['type']]
                            ),
                            key=lambda val: val['translations'],
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
                    if self.match_coordinates(loc, tweet):
                        loc['coordinates match'] = SCORE_TYPES['coordinates match']
                    else:
                        loc['coordinates match'] = 0
            else:
                for loc in doc_locations:
                    loc['coordinates match'] = 0

            # Match tweet time zine
            # if 'time zone' in tweet['user']:
            #     for loc in doc_locations:
            #         loc['time zone'] = self.match_timezones(loc, tweet)
            # else:
            #     for loc in doc_locations:
            #         loc['time zone'] = 0

            # match tweet utc offset
            if 'utc_offset' in tweet['user'] and tweet['user']['utc_offset'] is not None:
                if timezones is None:
                    timezones = self.find_time_zones_tweet(tweet)
                if timezones:
                    for loc in doc_locations:
                        if self.match_offset(loc, timezones):
                            loc['utc_offset'] = SCORE_TYPES['utc_offset']
                        else:
                            loc['utc_offset'] = 0
                else:
                    for loc in doc_locations:
                        loc['utc_offset'] = 0
            else:
                for loc in doc_locations:
                    loc['utc_offset'] = 0

            # Match tweet user location
            if 'location' in tweet['user']:
                if user_locations is None:
                    user_locations_str = tweet['user']['location']
                    if user_locations_str:
                        try:
                            user_locations = self.lastuserlocationdict.getandmove(user_locations_str)
                        except KeyError:
                            user_locations = self.find_user_location(user_locations_str)
                            self.lastuserlocationdict[user_locations_str] = user_locations
                    else:
                        user_locations = False

                if user_locations:
                    for loc in doc_locations:
                        if self.match_user_locations(loc, toponym, user_locations):
                            loc['user home'] = SCORE_TYPES['user home']
                        else:
                            loc['user home'] = 0
                else:
                    for loc in doc_locations:
                        loc['user home'] = 0
            else:
                for loc in doc_locations:
                    loc['user home'] = 0

            # Match tweet bounding box
            # Do not consider a bounding box if a coordinate is already present.
            if 'bbox' in tweet and tweet['bbox'] and 'coordinates' not in tweet:
                for loc in doc_locations:
                    if self.match_bbox(loc, tweet):
                        loc['bbox'] = SCORE_TYPES['bbox']
                    else:
                        loc['bbox'] = 0
            else:
                for loc in doc_locations:
                    loc['bbox'] = 0

            for loc in doc_locations:
                loc['family'] = 0

            # If other locaions are already added to the tweet_toponyms we can
            # check for family. If family is true, set both to True
            if tweet_toponyms:
                for tweet_toponym, geonameids in tweet_toponyms.items():
                    for geonameid, loc1 in geonameids.items():
                        for loc2 in doc_locations:
                            # Geonameid is not saved as a key-value-pair. Thus we need to add the geonameid to the dictonary before passing loc1 and loc2 to self.is_family()
                            if self.is_family(loc1, loc2, toponym, tweet_toponym, siblings=False, consider_population=True):
                                loc1['family'] = SCORE_TYPES['family']
                                loc2['family'] = SCORE_TYPES['family']

            # Collect all neccesary information for each location and add it to the dictonary
            locs_information = {}
            for loc in doc_locations:
                geonameid = loc['geonameid']
                locs_information[geonameid] = {
                    'bbox': loc['bbox'],
                    # 'time zone': loc['time zone'],
                    'utc_offset': loc['utc_offset'],
                    'coordinates match': loc['coordinates match'],
                    'user home': loc['user home'],
                    'family': loc['family'],
                    'type': loc['type'],
                    'country_geonameid': loc['country_geonameid'],
                    'adm1_geonameid': loc['adm1_geonameid'],
                    'language': loc['iso-language'],
                    'geonameid': loc['geonameid'],
                    'population': loc['population'],
                    'coordinates': loc['coordinates'],
                    'abbreviations': loc['abbreviations']
                }

            tweet_toponyms[toponym] = locs_information

        if not tweet_toponyms:
            return None
        # collect all neccesary information and return
        d = {
            'original_ngrams': {toponym: ngrams_orgininal[toponym] for toponym in tweet_toponyms},
            'subsetted_ngrams': subsetted_ngrams,
            'toponyms': tweet_toponyms,
            'user': {'id': tweet['user']['id']},
            'date': tweet['date'],
            'text': clean_text,
            'language': tweet['lang']
        }
        if index:
            d['index'] = index

        # if tweet_id == 681164080812560400 or tweet_id == '681164080812560400':
        #     print(d)

        return tweet_id, d


if __name__ == '__main__':
    test = TweetAnalyzer(5000, 1000)
    text = 'RT @JoeyClipstar: Bow_Woooow_Signs RT to #BadBoyRecords  - The Breakfast Club http://t.co/3w58p6Sbx2 RT http://t.co/LbQU2brfpf !!!!??'
    text = 'RT @JoeyClipstar 釋內文之英文單字均可再點入查詢'
    text = '東京と広島とオランダ'
    clean_text = sanitize.clean_text(text)
    print(test.get_ngrams(clean_text, 'ja'))
