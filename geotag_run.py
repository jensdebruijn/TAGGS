import datetime
import geotag_geotag

if __name__ == '__main__':
    geotagger = geotag_geotag.Geotag(min_score=.2, min_population=5000, n_words=1000)
    start = datetime.datetime(2014, 7, 29)
    end = False
    geotagger.history(
        start, timestep_length=datetime.timedelta(hours=6), analysis_lengthdatetime.timedelta(hours=24), end=end, realtime=False
    )
