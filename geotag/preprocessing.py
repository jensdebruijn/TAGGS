import time
import xml.etree.ElementTree as ET
import requests
import os
import csv
import pandas as pd
from requests.packages.urllib3.exceptions import MaxRetryError

from db.postgresql import PostgreSQL
from IO import files
from methods import shapefiles, function

from config import (
    TOPONYM_INDEX,
    GEONAMES_USERNAME,
    REFRESH_GEONAMES_TABLES,
    GEONAMES_DIR,
    POSTGRESQL_DB,
    es_toponyms
)


pd.options.mode.chained_assignment = None
SRID = 4326  # WGS84


class Preprocess(PostgreSQL):
    def __init__(self):
        # Connect to PostgreSQL
        PostgreSQL.__init__(self, POSTGRESQL_DB)
        PostgreSQL.initialize_postgis(self)

    def index_unique_names(self):
        """This function gets all unique names from the geonames and alternative names table, collects
        all some data from these databases and indexes all data to elasticsearch ready for querying"""

        def get_toponyms(names):
            # Check if the index exists. If it does not exist, the database is emtpy and we need to
            # index all names. Otherwise we first query the database to find all names
            # already indexed and only index the names not yet indexed.
            if es_toponyms.indices.exists(index=TOPONYM_INDEX):
                names_no_exist = []
                for chunk in function.chunker(names, 10000):
                    documents = es_toponyms.mget(
                        index=TOPONYM_INDEX,
                        doc_type='unique_name',
                        body={'ids': chunk}
                        )['docs']
                    names_no_exist.extend([doc['_id'] for doc in documents if doc['found'] is False])
            else:
                names_no_exist = names
            if names_no_exist:
                n_names = len(names_no_exist)
                for i, name in enumerate(names_no_exist, start=1):

                    if i % 100 == 0:
                        print("Indexing unique names ({}/{})".format(i, n_names), end="\r")
                    if es_toponyms.exists(index='toponyms', doc_type='unique_name', id=name):
                        continue

                    self.cur.execute("""
                        SELECT geonameid
                        FROM geonames
                        WHERE name='{}'
                    """.format(name.replace("'", "''")))
                    geonameids = [geonameid for geonameid, in self.cur.fetchall()]

                    abbreviations = {
                        geonameid: []
                        for geonameid in geonameids
                    }

                    languages = {
                        geonameid: ['general']
                        for geonameid in geonameids
                    }

                    self.cur.execute("""
                        SELECT geonameid, isolanguage, full_name
                        FROM alternate_names
                        WHERE alternate_name='{}'
                    """.format(name.replace("'", "''")))

                    for geonameid, isolanguage, full_name in self.cur.fetchall():
                        if not isolanguage:
                            continue

                        if geonameid not in languages:
                            languages[geonameid] = [isolanguage]
                        else:
                            languages[geonameid].append(isolanguage)
                        if geonameid not in abbreviations:
                            abbreviations[geonameid] = []
                        if isolanguage == 'abbr':
                            abbreviations[geonameid].append(full_name)

                    if languages:

                        ids = '(' + ', '.join([str(geonameid) for geonameid in languages.keys()]) + ')'

                        self.cur.execute(f"""SELECT
                                geonameid,
                                ST_X(location),
                                ST_Y(location),
                                population,
                                feature_code,
                                feature_class,
                                country_geonameid,
                                adm1_geonameid,
                                time_zone,
                                (
                                    SELECT COUNT(*)
                                    FROM alternate_names
                                    WHERE geonames.geonameid = alternate_names.geonameid
                                ) AS translations
                            FROM geonames
                            WHERE geonameid IN {ids}""")

                        locations = [
                                {
                                     'geonameid': geonameid,
                                     'iso-language': languages[geonameid],
                                     'coordinates': (longitude, latitude),
                                     'time_zone': time_zone,
                                     'population': population,
                                     'country_geonameid': country_geonameid,
                                     'adm1_geonameid': adm1_geonameid,
                                     'feature_code': feature_code,
                                     'feature_class': feature_class,
                                     'translations': translations,
                                     'abbreviations': abbreviations[geonameid]
                                }
                                for geonameid, longitude, latitude, population, feature_code, feature_class, country_geonameid, adm1_geonameid, time_zone, translations in self.cur.fetchall()
                                if languages[geonameid] is not None
                        ]

                        if locations:
                            body = {
                                'locations': locations,
                                '_index': TOPONYM_INDEX,
                                '_type': 'unique_name',
                                '_id': name,
                                '_op_type': 'index'
                            }
                            yield body

            # Print the final number ones more without \r to that it is not overwritten by the next print.
            print(f"Indexing unique names ({n_names}/{n_names})")

        # # Retrieve all distinct names from the geonames and alternative names table
        self.cur.execute("""
            SELECT DISTINCT name
            FROM
            (
                SELECT name FROM geonames
                UNION ALL
                SELECT alternate_name FROM alternate_names
            ) AS x""")

        toponyms_to_index = get_toponyms([name for name, in self.cur.fetchall()])
        es_toponyms.bulk_operation(toponyms_to_index)

    def get_geonames(self, file, ext):
        """This function downloads data from the geonames website and unzips if
        neccesary. For more info see: http://download.geonames.org/export/dump/readme.txt"""
        try:
            os.makedirs(GEONAMES_DIR)
        except OSError:
            pass
        file_path_wo_ext = os.path.join(GEONAMES_DIR, file)
        if not os.path.exists(file_path_wo_ext + '.txt') or REFRESH_GEONAMES_TABLES:
            print('Downloading {}'.format(file))
            url = 'http://download.geonames.org/export/dump/{}'.format(file + '.' + ext)
            print(f"\t{url}")
            file_path = file_path_wo_ext + '.' + ext
            response = files.download_http(url, file_path)
            if response is False:
                print('Error')
            if ext == 'zip':
                files.unzipper(file_path, GEONAMES_DIR)
                os.remove(file_path)

    def parse_table(self, file_path, column_names, columns_out, dtypes, skiprows=0):
        """This function parses a geonames table and retuns it as a pandas dataframe"""
        file_path = os.path.join(GEONAMES_DIR, file_path)
        df = pd.read_csv(file_path, sep='\t', header=None, names=column_names, dtype=dtypes, skiprows=skiprows, engine='c', keep_default_na=False, na_values=["", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "N/A", "NULL", "NaN", "nan"], quoting=csv.QUOTE_NONE, usecols=columns_out)
        os.remove(file_path)
        return df[columns_out]

    def create_continent_table(self):
        """This function reads the continents shapefile and commits it to PostgreSQL"""
        self.cur.execute("select exists(select * from information_schema.tables where table_name='continents')")
        if not self.cur.fetchone()[0]:
            print("Creating continents table")
            self.cur.execute(f"""CREATE TABLE continents (
                geonameid INTEGER PRIMARY KEY,
                geom GEOMETRY(Geometry, {SRID})
            )""")

            areas = shapefiles.shapefile_to_wkt('input/maps/continents/continent.shp', 'geonameid')
            areas = {
                id: shape
                for id, shape in areas.items()
            }
            arg_str = ','.join(self.cur.mogrify("(%s, ST_GeometryFromText(%s, {SRID}))".format(SRID=SRID), value).decode() for value in areas.items())
            self.cur.execute("INSERT INTO continents (geonameid, geom) VALUES " + arg_str)
            self.conn.commit()

    def create_country_table(self):
        """This function creates the country table, parsing most data from the geonames table. In additon
        it finds the country outlines"""
        self.cur.execute("select exists(select * from information_schema.tables where table_name='countries')")
        if not self.cur.fetchone()[0]:
            print("Creating country table")
            self.cur.execute(f"""CREATE TABLE countries ( \
                geonameid INTEGER PRIMARY KEY, \
                ISO2 VARCHAR(2), \
                ISO3 VARCHAR(3), \
                continents VARCHAR(23), \
                languages VARCHAR(300), \
                name VARCHAR(200), \
                geom GEOMETRY(MULTIPOLYGON, {SRID}) \
            )""")

            features = self.get_geoname_table('countryInfo', 'txt', ['ISO2', 'ISO3', 'ISO-Numeric', 'FIPS', 'Country', 'Capital', 'Area', 'Population', 'Continent', 'tld', 'self.currencyCode', 'self.currencyName', 'Phone', 'Postal Code Format', 'Postal Code Regex', 'Languages', 'geonameid', 'neighbours', 'EquivalentFipsCode'], ['geonameid', 'ISO2', 'ISO3', 'Continent', 'Languages', 'Country'], skiprows=51)
            features['Country'] = features.Country.str.lower()

            continent_code2geonameid = {
                'EU': '6255148',
                'AS': '6255147',
                'AF': '6255146',
                'NA': '6255149',
                'OC': '6255151',
                'SA': '6255150',
                'AN': '6255152'
            }

            special_continent_cases = {
                '2017370': '6255148,6255147',
                '1643084': '6255147,6255151',
                '1522867': '6255147,6255148',
                '587116': '6255147,6255148',
                '614540': '6255147,6255148',
                '298795': '6255147,6255148',
                '357994': '6255147,6255146',
                '3703430': '6255149,6255150',
            }

            def convert_to_geonameid(row):
                if row['geonameid'] in special_continent_cases:
                    return special_continent_cases[row['geonameid']]
                else:
                    return continent_code2geonameid[row['Continent']]

            features['Continent'] = features.apply(lambda row: convert_to_geonameid(row), axis=1)

            outlines = shapefiles.shapefile_to_wkt('input/maps/ne_10m_admin_0_countries/ne_10m_admin_0_countries.shp', 'ISO_A2', force_multipolygon=True)

            features["outlines"] = features["ISO2"].map(outlines)

            arg_str = ','.join(self.cur.mogrify(f"(%s, %s, %s, %s, %s, %s, ST_GeometryFromText(%s, {SRID}))", value).decode() for i, value in features.iterrows())
            self.cur.execute("INSERT INTO countries (geonameid, ISO2, ISO3, continents, languages, name, geom) VALUES " + arg_str)

            self.conn.commit()

    def create_adm1_table(self):
        self.cur.execute("select exists(select * from information_schema.tables where table_name='adm1')")
        if not self.cur.fetchone()[0]:
            print("Creating admin1 table")
            self.cur.execute(f"""CREATE TABLE adm1 ( \
                geonameid INTEGER PRIMARY KEY, \
                geom GEOMETRY(Geometry, {SRID}) \
            )""")

            merged_on_gn_id_shp = 'output/shapefile/adm1_merged_on_gn_id.shp'
            if not os.path.exists(merged_on_gn_id_shp):
                shapefiles.merge_shapefile_by_id(
                    'input/maps/ne_10m_admin_1_states_provinces/ne_10m_admin_1_states_provinces.shp',
                    merged_on_gn_id_shp,
                    'adm1',
                    'gn_id'
                )

            areas = shapefiles.shapefile_to_wkt(merged_on_gn_id_shp, 'gn_id')
            areas = {
                id: shape
                for id, shape in areas.items()
                if id >= 1
            }

            arg_str = ','.join(self.cur.mogrify("(%s, ST_GeometryFromText(%s, {SRID}))".format(SRID=SRID), value).decode() for value in areas.items())
            self.cur.execute("INSERT INTO adm1 (geonameid, geom) VALUES " + arg_str)
            self.conn.commit()

            self.cur.execute("""
                CREATE INDEX
                IF NOT EXISTS adm1_geometry_idx
                ON adm1
                USING GIST (geom)""")
            self.conn.commit()

    def find_administrative_parents(self):
        self.cur.execute("""SELECT EXISTS (
              SELECT 1 FROM information_schema.columns
              WHERE table_name='geonames'
              AND column_name='adm1_geonameid'
        )""")
        if not self.cur.fetchone()[0]:
            print("Finding administrative parents")
            self.cur.execute("""
                ALTER TABLE geonames
                ADD COLUMN adm1_geonameid INT
            """)
            print("Matching geonames with adm1 - this will take a long time")
            self.cur.execute("""
                UPDATE geonames
                SET adm1_geonameid = adm1.geonameid
                FROM adm1
                WHERE (ST_Within(geonames.location, adm1.geom) AND feature_code in ('PPL','PPLA','PPLA2','PPLA3','PPLA4','PPLC','PPLG','PPLR','PPLS','PPLX','STLMT'))
            """)
            self.conn.commit()
            self.cur.execute("""
                UPDATE geonames
                SET adm1_geonameid = adm2.adm1_parent
                FROM adm2
                WHERE adm2.adm2 = geonames.geonameid
            """)
            self.conn.commit()
            self.cur.execute("""
                CREATE INDEX
                IF NOT EXISTS geonames_adm1_geonameids
                ON geonames (adm1_geonameid)
            """)
            self.conn.commit()

    def find_time_zones(self):
        self.cur.execute("select exists ( \
              SELECT 1 FROM information_schema.columns \
              where table_name='geonames' \
              and column_name='time_zone' \
        )")
        if not self.cur.fetchone()[0]:
            print("Matching geonames with time_zones")
            self.cur.execute("""ALTER TABLE geonames ADD COLUMN time_zone VARCHAR(40)""")

            self.cur.execute("""
                UPDATE geonames
                SET time_zone = time_zones.name
                FROM time_zones
                WHERE ST_Within(geonames.location, time_zones.geom)
            """)
            self.conn.commit()

            self.cur.execute("""
                CREATE INDEX
                IF NOT EXISTS geonames_names
                ON geonames (name)
            """)
            self.conn.commit()

    def get_childs(self, geonameid):
        url = f"http://api.geonames.org/children?geonameId={geonameid}&username={GEONAMES_USERNAME}"
        while True:
            try:
                root = ET.fromstring(requests.get(url).content)
                break
            except MaxRetryError:
                time.sleep(100)
        return list(geoname.text for geoname in root.iter('geonameId'))

    def get_children(self, geonameids):
        child_parents = set()
        n = len(geonameids)
        for i, geonameid in enumerate(geonameids, start=1):
            print(f'{i}/{n}', end='\r')
            for child in self.get_childs(geonameid):
                child_parents.add((child, geonameid))
        print(f'{i}/{n}')
        return list(child_parents)

    def get_adm1_children(self):
        self.cur.execute("""SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='adm2')""")
        if not self.cur.fetchone()[0]:
            print("Getting adm1 children. This might take a while")
            self.cur.execute("SELECT geonameid FROM adm1")
            geonameids = list(geonameid for geonameid, in self.cur.fetchall())
            child_parents = self.get_children(geonameids)

            self.cur.execute("""
                CREATE TABLE adm2 (
                    adm2 INT,
                    adm1_parent INT
                )""")

            query = """INSERT INTO adm2 (adm2, adm1_parent) VALUES {}"""
            mogr = f"(%s, %s)"
            self.commit_chunk(query, mogr, child_parents)

    def get_geoname_table(self, file_path, ext, columns_in, columns_out, skiprows=0):
        self.get_geonames(file_path, ext)
        dtypes = {
            'geonameid': object,
            'name': object,
            'asciiname': object,
            'alternatenames': object,
            'latitude': object,
            'longitude': object,
            'feature_class': object,
            'feature_code': object,
            'country code': object,
            'cc2': object,
            'admin1_code': object,
            'admin2_code': object,
            'admin3_code': object,
            'admin4_code': object,
            'population': float,
            'elevation': float,
            'dem': float,
            'time_zone': object,
        }
        return self.parse_table(f'{file_path}.txt', column_names=columns_in, columns_out=columns_out, dtypes=dtypes, skiprows=skiprows)

    def create_geonames_table(self):
        # self.cur.execute('DROP TABLE IF EXISTS geonames')
        self.cur.execute("""SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='geonames')""")
        if not self.cur.fetchone()[0]:
            print("Creating geonames table")
            self.cur.execute(f"""
                CREATE TABLE geonames (
                geonameid INT PRIMARY KEY,
                name VARCHAR(200),
                full_name VARCHAR(200),
                feature_class CHAR(1),
                feature_code VARCHAR(10),
                population BIGINT,
                location GEOMETRY(Point, {SRID}),
                country_geonameid INT,
                admin1_geonameid INT)
            """)
            columns_in = ['geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude', 'feature_class', 'feature_code', 'country', 'cc2', 'admin1_code', 'admin2_code', 'admin3_code', 'admin4_code', 'population', 'elevation', 'dem', 'time_zone', 'modification_date']
            columns_out = ['geonameid', 'name', 'feature_class', 'feature_code', 'country', 'population', 'longitude', 'latitude']

            self.get_geonames('allCountries', 'zip')
            dtypes = {
                'geonameid': int,
                'name': object,
                'asciiname': object,
                'alternatenames': object,
                'latitude': object,
                'longitude': object,
                'feature_class': object,
                'feature_code': object,
                'country': object,
                'cc2': object,
                'admin1_code': object,
                'admin2_code': object,
                'admin3_code': object,
                'admin4_code': object,
                'population': float,
                'elevation': float,
                'dem': float,
                'time_zone': object,
            }
            features = self.parse_table('allCountries.txt', column_names=columns_in, columns_out=columns_out, dtypes=dtypes)

            town_codes = set(['PPL', 'PPLA', 'PPLA2', 'PPLA3', 'PPLA4', 'PPLC', 'PPLCH', 'PPLF', 'PPLG', 'PPLH', 'PPLL', 'PPLQ', 'PPLR', 'PPLS', 'PPLW', 'PPLX', 'STLMT'])
            adm_codes = set(['PCLI', 'ADM1', 'ADM2', 'ADM1H', 'ADM2H'])
            other_codes = set(['CONT'])

            select_feature_codes = town_codes | adm_codes | other_codes
            self.cur.execute("""SELECT geonameid FROM adm1""")
            adm1_codes = set(geonameid for geonameid, in self.cur.fetchall())

            features = features[
                (features['feature_code'].isin(select_feature_codes)) | features['geonameid'].isin(adm1_codes)
            ]

            features['full_name'] = features['name']
            features['name'] = features.name.str.lower()
            features['longitude'] = pd.to_numeric(features['longitude'])
            features['latitude'] = pd.to_numeric(features['latitude'])

            self.cur.execute("""
                SELECT ISO2, geonameid FROM countries
            """)
            ISO2_2_geonameid = {
                ISO2: geonameid
                for ISO2, geonameid in self.cur.fetchall()
            }

            # Map country ISO-code to geonameid
            features['country'] = features['country'].map(ISO2_2_geonameid)

            query = """
                INSERT INTO geonames (geonameid, name, feature_class, feature_code, country_geonameid, population, location, full_name)
                VALUES {}
            """
            mogr = f"(%s, %s, %s, %s, %s, %s, ST_GeomFromText('POINT(%s %s)', {SRID}), %s)"
            values = (value for i, value in features.iterrows())
            self.commit_chunk(query, mogr, values)

    def create_alternate_names_table(self):
        # Only execute if table does not exist
        self.cur.execute("select exists(select * from information_schema.tables where table_name='alternate_names')")
        if not self.cur.fetchone()[0]:
            print("Creating alternate_names table")
            # Create table
            self.cur.execute("""CREATE TABLE alternate_names (
                alternateNameId INTEGER PRIMARY KEY,
                geonameid INTEGER,
                isolanguage VARCHAR(7),
                alternate_name VARCHAR(400),
                full_name VARCHAR(400)
            )""")

            self.get_geonames('alternateNames', 'zip')
            columns_in = ['alternateNameId', 'geonameid', 'isolanguage', 'alternate_name', 'isPreferredName', 'isShortName', 'isColloquial', 'isHistoric']
            columns_out = ['alternateNameId', 'geonameid', 'isolanguage', 'alternate_name']
            dtypes = {
                'alternateNames': object,
                'geonameid': int,
                'isolanguage': object,
                'alternate name': object,
                'isPreferredName': object,
                'isShortName': object,
                'isColloquial': object,
                'isHistoric': object,
            }
            alternate_names = self.parse_table('alternateNames.txt', column_names=columns_in, columns_out=columns_out, dtypes=dtypes)
            alternate_names = alternate_names[~alternate_names['isolanguage'].isin(['link', 'post'])]

            # Use only rows with geonameids in geonames table. Others we don't need
            self.cur.execute("SELECT geonameid FROM geonames")
            geonameids = set(id for id, in self.cur.fetchall())
            alternate_names = alternate_names[alternate_names['geonameid'].isin(geonameids)]

            # Set name to lowercase
            alternate_names['full_name'] = alternate_names['alternate_name']
            alternate_names['alternate_name'] = alternate_names['alternate_name'].str.lower()

            # Commit to database
            query = "INSERT INTO alternate_names (alternateNameId, geonameid, isolanguage, alternate_name, full_name) VALUES {}"
            mogr = "(%s, %s, %s, %s, %s)"
            values = (value for i, value in alternate_names.iterrows())
            self.commit_chunk(query, mogr, values)

            # Create index for faster query. We need that later
            self.cur.execute("""CREATE INDEX
                IF NOT EXISTS alternate_names_names
                ON alternate_names (alternate_name)""")
            self.cur.execute("""CREATE INDEX
                IF NOT EXISTS alternative_names_geonameids
                ON alternate_names (geonameid)""")
            self.conn.commit()

    def create_time_zone_table(self):
        # self.cur.execute("DROP TABLE IF EXISTS time_zones")
        self.cur.execute("select exists(select * from information_schema.tables where table_name='time_zones')")
        if not self.cur.fetchone()[0]:
            print("Creating time_zones table")
            self.cur.execute("CREATE TABLE time_zones ( \
                name VARCHAR(40) PRIMARY KEY,  \
                geom GEOMETRY(Geometry, {SRID}) \
            )".format(SRID=SRID))

            shapefile = 'input/time_zones/time_zones.shp'
            timezones = shapefiles.shapefile_to_wkt('input/time_zones/time_zones.shp', 'TZID')
            values = ((name, wkt) for name, wkt in timezones.items())
            query = "INSERT INTO time_zones (name, geom) VALUES {}"
            mogr = "(%s, ST_GeometryFromText(%s, {SRID}))".format(SRID=SRID)
            self.commit_chunk(query, mogr, values, 100)
            self.cur.execute("CREATE INDEX \
                IF NOT EXISTS tz_geometry_idx \
                ON time_zones \
                USING GIST (geom)")
            self.conn.commit()

    def create_time_zone_map(self):
        # self.cur.execute("DROP TABLE IF EXISTS time_zones")
        self.cur.execute("select exists(select * from information_schema.tables where table_name='time_zone_map')")
        if not self.cur.fetchone()[0]:
            print("Creating time_zones map")
            self.cur.execute("CREATE TABLE time_zone_map ( \
                twitter_name VARCHAR(40) PRIMARY KEY,  \
                tz_name VARCHAR(40)  \
            )")
            with open('input/time_zones/tz_names.csv', 'r') as f:
                reader = csv.reader(f)
                tz_map = [(row[0], row[1]) for row in reader]

            query = "INSERT INTO time_zone_map (twitter_name, tz_name) VALUES {}"
            mogr = "(%s, %s)"
            self.commit_chunk(query, mogr, tz_map)
            self.cur.execute("CREATE INDEX \
                IF NOT EXISTS tz_geometry_idx \
                ON time_zones \
                USING GIST (geom)")
            self.conn.commit()

    def create_timezones_per_country_table(self):
        self.cur.execute("select exists(select * from information_schema.tables where table_name='time_zones_per_country')")
        if not self.cur.fetchone()[0]:
            print("Creating time_zones_per_country table")
            self.cur.execute("CREATE TABLE time_zones_per_country ( \
                ISO2 VARCHAR(2), \
                name VARCHAR(200) \
            )")

            timezones_table = 'input/tables/time_zones.txt'
            df = pd.read_table(timezones_table, sep='\t', comment='#', usecols=range(3), names=['ISO2', 'coordinates_tz', 'name'], keep_default_na=False, na_values=["", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN", "N/A", "NULL", "NaN", "nan"])
            df.drop('coordinates_tz', axis=1, inplace=True)

            query = "INSERT INTO time_zones_per_country (ISO2, name) VALUES {}"
            mogr = "(%s, %s)"
            values = (value for i, value in df.iterrows())
            self.commit_chunk(query, mogr, values, 100)

    def get_most_common_words(self):
        self.cur.execute("""SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name='most_common_words')""")
        if not self.cur.fetchone()[0]:
            self.cur.execute("""
                CREATE TABLE most_common_words (
                n SMALLINT,
                language VARCHAR(2),
                word VARCHAR(30)
            )""")

            languages = ['en', 'id', 'tl', 'fr', 'de', 'it', 'nl', 'pl', 'sr', 'pt', 'es', 'tr', 'sw']

            words = []
            for language in languages:
                with open(f'input/word_frequencies/words_{language}.txt', 'rb') as f:
                    for i, line in enumerate(f.readlines(), start=1):
                        word = line.decode().split(' ')[0].strip()
                        words.append((i, language, word))
                        if i == 10000:
                            break

            query = "INSERT INTO most_common_words (n, language, word) VALUES {}"
            mogr = "(%s, %s, %s)"
            self.commit_chunk(query, mogr, words, 100)


if __name__ == '__main__':
    p = Preprocess()
    p.create_continent_table()
    p.create_country_table()
    p.create_adm1_table()
    p.get_adm1_children()

    p.create_time_zone_table()
    p.create_time_zone_map()
    p.create_timezones_per_country_table()

    p.create_geonames_table()
    p.find_time_zones()
    p.find_administrative_parents()
    p.get_most_common_words()

    p.create_alternate_names_table()
    p.index_unique_names()
