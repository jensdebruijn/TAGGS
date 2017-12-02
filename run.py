import datetime
import geotag
from geotag.config import TWEETS_INDEX

if __name__ == '__main__':
    geotagger = geotag.Geotag(
        threshold=.2,
        min_population_capitalized=1,
        min_population_non_capitalized=5000,
        analysis_length=datetime.timedelta(hours=24),
        n_words=1000
    )
    start = datetime.datetime(2014, 7, 29)
    end = False
    geotagger.history(
        start, timestep_length=datetime.timedelta(hours=6), end=end, realtime=False
    )
