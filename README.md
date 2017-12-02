TAGGS
============

TAGGS is a tool to geoparse tweets based on the tweet content.  First, tweets are collected over a 24-hour period. Each individual tweet within this timeframe is analyzed on an individual basis by matching the text of the tweet to our gazetteer (toponym recognition). Next, each of the tweets’ candidate locations is given a score, indicating how well the candidate location matches the tweets’ additional spatial information. While previous approaches use the information of this individual tweet, we group all tweets according to the mentioned toponym, found in the toponym recognition step. Then, we compute the total score for each of the candidate locations by summing the scores of the individual tweets and use a voting process to assign the best location (toponym resolution) to all tweets in the group. Once the locations have been assigned to the tweets, the same procedure is applied to a later timeframe, which includes newly incoming tweets, while tweets older than 24 hours are not considered any longer.


Abstract
============

Timely and accurate information about ongoing events are crucial for relief organizations seeking to effectively respond to disasters. Recently, social media platforms, especially Twitter, have gained traction as a novel source of information on disaster events. Unfortunately, geographical information is rarely attached to tweets, which hinders the use of Twitter for geographical applications. As a solution, geoparsing algorithms extract and locate geographical locations referenced in a tweet’s text. This paper describes TAGGS, a new algorithm that enhances location disambiguation by employing both metadata and the contextual spatial information of groups of tweets referencing the same location regarding a specific disaster type. Validation demonstrated that TAGGS approximately attains a recall of 0.9 and precision of 0.85. Without lowering precision, this roughly doubles the number of correctly found administrative subdivisions and cities, towns and villages as compared to individual geoparsing. We applied TAGGS to 55.1 million flood-related tweets in 12 languages, collected over 3 years. We found 19.2 million tweets mentioning one or more flood locations, which can be towns (11.2 million), administrative subdivisions (5.1 million), or countries (4.6 million). In the future, TAGGS could form the basis for a global event detection system.

Requirements
============

* Python 3.6+
* Python modules as stated in requirements.txt
* GDAL
* An Elasticsearch database (tested with v5.3)
* PostgreSQL (tested with v9.6)
* PostGIS (tested with v2.3)

Datasets
============
* [GeoNames database](http://download.geonames.org/export/dump/readme.txt)
* [Natural Earth data](http://www.naturalearthdata.com/)
* [Twitter Data through streaming API](https://dev.twitter.com/streaming/overview)
* [tz_world](http://efele.net/maps/tz/world/)
* [ESRI Continents map](https://www.arcgis.com/home/item.html?id=a3cb207855b348a297ab85261743351ds)

Installation
============

* A set of tweets mentioning keywords related to a specific topic should be loaded in a Elasticsearch index using the mapping provided in es_mapping_tweets.json. Alternatively you can edit the functions in geotag_config.py to use to your custom format.
* Enter the server, port, username and password of your Elasticsearch and PostgreSQL database in config.py
* Sign up for an account at [GeoNames](https://www.geonames.org) and enter your user account in geotag.config.py
* Run geotag/preprocessing.py
* Enter run pararameters in run.py
* Run run.py

Contact
=======

Jens de Bruijn - j.a.debruijn@outlook.com
