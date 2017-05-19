from elasticsearch import Elasticsearch, helpers
import logging


from methods import function
from config import ELASTIC_USER, ELASTIC_PASSWORD, ELASTIC_HOST, ELASTIC_PORT


class Elastic(Elasticsearch):
    def __init__(self):
        if ELASTIC_USER:
            super().__init__(
                [{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}],
                http_auth=(ELASTIC_USER, ELASTIC_PASSWORD)
            )
        else:
            super().__init__(
                [{'host': ELASTIC_HOST, 'port': ELASTIC_PORT}]
            )
        tracer = logging.getLogger('elasticsearch')
        tracer.setLevel(logging.CRITICAL)

    def bulk_operation(self, iterator, size=1000):
        if iterator:
            bulk = helpers.streaming_bulk(self, iterator, chunk_size=size, request_timeout=60)
            while True:
                try:
                    next(bulk)
                except StopIteration:
                    break

    def loop_search(self, page, source=True):
        for hit in page['hits']['hits']:
            if source:
                yield hit
            else:
                yield hit['_source']

    def scroll_through(self, index, body, doc_type=None, size=100, scroll='1m', source=True):
        page = self.search(index=index, doc_type=doc_type, body=body, size=size, scroll=scroll)
        scroll_id = page['_scroll_id']
        scroll_size = page['hits']['total']
        for hit in self.loop_search(page, source):
            yield hit
        returned = size
        while scroll_size > returned:
            page = self.scroll(scroll_id=scroll_id, scroll=scroll)
            for hit in self.loop_search(page, source):
                returned += 1
                yield hit

    def n_hits(self, index, doc_type=None, body=None):
        return self.search(index=index, doc_type=doc_type, body=body, size=0)['hits']['total']

    def build_location_query(self):
        return {
          "query": {
            "nested": {
              "path": "locations",
              "query": {
                "bool": {
                  "must": [
                    {
                      "exists": {
                        "field": "locations"
                      }
                    }
                  ]
                }
              }
            }
          }
        }

    def build_date_query(self, start, end, locations=False, sort=False, filter_countries=False, filter_adm1=False):
        if not locations:
            query = {
                "query": {
                    "range": {
                        "date": {
                            "gte": start.isoformat(),
                            "lte": end.isoformat(),
                        }
                    }
                }
            }
        else:
            loc_query = {}
            if filter_countries and filter_adm1:
                if not isinstance(filter_adm1, list):
                    filter_adm1 = [filter_adm1]
                if not isinstance(filter_countries, list):
                    filter_adm1 = [filter_countries]
                loc_query.update({
                    "should": [
                        {
                            "terms":
                                {"locations.geonameid": filter_adm1 + filter_countries}
                        },
                        {
                            "terms":
                                {"locations.adm1_geonameid": filter_adm1}
                        }
                    ],
                    "minimum_should_match": 1
                })
            elif filter_countries:
                if isinstance(filter_countries, list):
                    term = "terms"
                else:
                    term = "term"
                loc_query.update({
                    "must": [
                        {
                            term:
                                {"locations.geonameid": filter_countries}
                        }
                    ]
                })
            elif filter_adm1:
                if isinstance(filter_adm1, list):
                    term = "terms"
                else:
                    term = "term"
                loc_query.update({
                    "should": [
                        {
                            term:
                                {"locations.geonameid": filter_adm1}
                        },
                        {
                            term:
                                {"locations.adm1_geonameid": filter_adm1}
                        }
                    ],
                    "minimum_should_match": 1
                })
            else:
                loc_query.update({
                  "must": [
                    {
                      "exists": {
                        "field": "locations"
                      }
                    }
                  ]
                })
            query = {
                "must": [
                    {
                        "range": {
                            "date": {
                                "gte": start.isoformat(),
                                "lte": end.isoformat(),
                            }
                        }
                    },
                    {
                        "nested": {
                          "path": "locations",
                          "query": {
                            "bool": loc_query
                          }
                        }
                    },
                ]
            }
            query = {
                "query": {
                    "bool": query
                }
            }
        if sort:
            query.update({
                "sort": [
                    {"date": {"order": "asc"}}
                ]})
        return query
