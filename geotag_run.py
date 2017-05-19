import datetime
import geotag_config
import geotag_geotag

if __name__ == '__main__':
    geotagger = geotag_geotag.Geotag(0.2, 5000, 1000)
    start = datetime.datetime(2014, 7, 29)
    end = False
    geotagger.history(
        start, datetime.timedelta(hours=6), datetime.timedelta(hours=24), end=end, realtime=False
    )
