TAGGS
============

TAGGS is a tool to geotag tweets based on the tweet content.  First, tweets are collected over a 24-hour period. Each individual tweet within this timeframe is analyzed on an individual basis by matching the text of the tweet to our gazetteer (toponym recognition). Next, each of the tweets’ candidate locations is given a score, indicating how well the candidate location matches the tweets’ additional spatial information. While previous approaches use the information of this individual tweet, we group all tweets according to the mentioned toponym, found in the toponym recognition step. Then, we compute the total score for each of the candidate locations by summing the scores of the individual tweets and use a voting process to assign the best location (toponym resolution) to all tweets in the group. Once the locations have been assigned to the tweets, the same procedure is applied to a later timeframe, which includes newly incoming tweets, while tweets older than 24 hours are not considered any longer.

Please cite as:

TAGGS: A New Approach for Geotagging Tweets for Disaster Response using Temporal Grouping

Abstract
============

The availability of timely and accurate information about an ongoing event is important assist relief organizations in enhancing disaster response. Recently, social media, and in particular Twitter, has gained traction as a novel source of information on disastrous events. Unfortunately, geographical information is rarely attached to tweets. As a solution, analysis of the tweet text, combined with an analysis of the tweet metadata, can help to increase the number of geolocated tweets. In this paper, we demonstrate a new algorithm, TAGGS (Toponym-based Algorithm for Grouped Geotagging of Social media), that reliably geotags roughly two times more tweets than previously developed algorithms and at the same time reduces the total number of errors, by using the spatial information of groups of tweets mentioning the same location. We apply this approach to 35.1 million flood-related tweets collected over 2.5 years in 12 languages. In our dataset, we find 11.6 million tweets mentioning one or more location, which can be cities/villages (6.9 million), provinces (3.3 million) or countries (2.2 million). Validation shows that about 65%-75% of the tweets are correctly located by our new model. Correspondingly, our model can form the basis for a global Geotag and monitoring system.

Requirements
============

* Python 3.6+
* Python modules as stated in requirements.txt
* GDAL
* An Elasticsearch database (tested with v5.3)
* PostgreSQL (tested with v9.6)
* PostGIS (tested with v2.3)

Installation
============

While this tool works, it is designed for scientific research, and by no means production ready and you should expect some effort to get everything running.

* A set of tweets mentioning keywords related to a specific topic should be loaded in a Elasticsearch index using the mapping provided in es_mapping_tweets.json. Alternatively you can edit the functions in geotag_config.py to connect to your custom format.
* Enter the server, port, username and password of your Elasticsearch and PostgreSQL database in config.py
* Sign up for an account at [GeoNames](https://www.geonames.org) and enter your user account in config.py
* Run geotag_preprocessing.py
* Enter run pararameters in geotag_geotag.py
* Run geotag_geotag.py

Contact
=======

Jens de Bruijn - jens.de.bruijn@vu.nl
