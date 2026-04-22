import elasticsearch
from elasticsearch import helpers

from constants.config import SECTION_ES, ES_HOST, ES_PORT, ES_SCHEME, ES_USE_AUTH, ES_USERNAME, ES_PASSWORD, \
    ES_SSL_ENABLED, ES_SSL_VERIFY, ES_SSL_CA_FILE
from helper.config import Config
from helper.logger import Logger

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 9200
DEFAULT_SCHEME = 'http'
DEFAULT_SSL_ENABLED = False
DEFAULT_SSL_VERIFY = False

# def connect_to_es(ip, port, elastic_creds, qu):
#     # Connect to the elastic cluster
#     # es = Elasticsearch(['https://%s:%s' % (ip, port)], http_auth=(elastic_creds[0], elastic_creds[1]),
#     #                    verify_certs=False, timeout=60, max_retries=10, retry_on_timeout=True)
#     es = Elasticsearch([ip], port=port, scheme="https", http_auth=(elastic_creds[0], elastic_creds[1]),
#                        verify_certs=False, timeout=60, max_retries=10, retry_on_timeout=True)
#     # put the result in the thread queue
#     qu.put(es)
#     return


def es_conn_tlimit(ip, port, elastic_creds, tlimit_s):
    host = Config.get(SECTION_ES, ES_HOST, DEFAULT_HOST)
    port = Config.getint(SECTION_ES, ES_PORT, DEFAULT_PORT)
    scheme = Config.get(SECTION_ES, ES_SCHEME, DEFAULT_SCHEME)
    use_auth = Config.getboolean(SECTION_ES, ES_USE_AUTH, default=False)
    if use_auth:
        user = Config.get(SECTION_ES, ES_USERNAME)
        password = Config.get(SECTION_ES, ES_PASSWORD)
    _ssl_enabled = Config.getboolean(SECTION_ES, ES_SSL_ENABLED, DEFAULT_SSL_ENABLED)
    _ssl_verify = DEFAULT_SSL_VERIFY
    if _ssl_enabled:
        _ssl_verify = Config.getboolean(SECTION_ES, ES_SSL_VERIFY, default=DEFAULT_SSL_VERIFY)
        if _ssl_verify:
            ca_file = Config.get(SECTION_ES, ES_SSL_CA_FILE)
            if not ca_file.endswith('.pem'):
                raise ValueError('Unsupported CA file received. Only PEM file is supported')

    es = elasticsearch.Elasticsearch([host], port=port, scheme=scheme,
                       http_auth=(user, password) if use_auth else None,
                       verify_certs=_ssl_verify if _ssl_enabled else False,
                       ca_certs=ca_file if _ssl_verify else None,
                       timeout=60, max_retries=10, retry_on_timeout=True)
    return es


def es_query(es_obj, index_name, query_body, n_size, supress_warn=False):
    # make a basic query for queries that return < 10k results
    # Initialize the scroll
    res = es_obj.search(index=index_name,
                        body=query_body,
                        size=n_size)
                        # track_total_hits=True)

    # find the total number of matches to the query

    # initialize list for results
    query_results_data = [dict(dc['_source'], **{'_id': dc['_id']}) for dc in res['hits']['hits']]

    # check if there are more than 10k matches, show warning
    if not supress_warn:
        total_n_matches = res['hits']['total']['value']
        if total_n_matches > 10000:
            Logger.log.warn('Warning! More than 10000 records found, use a scroll query instead. '
                  'Only returning 10000 records. Increase n_size up to 10000')
        # check if there are more matches than the size limit for the query
        elif total_n_matches > n_size:
            Logger.log.warn('Warning! Expected query size is less than the number of records found. '
                  'Only returning %d records' % n_size)

    return query_results_data, res


# def scroll_query(es_obj, index_name, query_body, n_size):
#     # use a scrolling helper for queries that will return > 10k results
#     # Initialize the scroll
#     page = es_obj.search(index=index_name,
#                          body=query_body,
#                          scroll='2m',
#                          search_type='query_then_fetch',
#                          size=n_size)
#     # initialize list for results
#     query_results = [dict(dc['_source'], **{'_id': dc['_id']}) for dc in page['hits']['hits']]
#     # get the scroll params
#     sid = page['_scroll_id']
#     scroll_size = page['hits']['total']['value']  # get the number of total hits for the query
#     if page['hits']['total']['relation'] != 'eq':
#         Logger.log.info('Warning! hits.total.relation value is not \'eq\', search results may be inaccurate. '
#               'hits.total.relation = %s' % page['hits']['total']['relation'])
#
#     # Start scrolling
#     while scroll_size > 0:
#         # get the data on the page
#         page = es_obj.scroll(scroll_id=sid, scroll='2m')
#
#         # return the query results
#         query_results += [dict(dc['_source'], **{'_id': dc['_id']}) for dc in page['hits']['hits']]
#
#         if page['_scroll_id'] != sid:
#             es_obj.clear_scroll(scroll_id=sid)
#
#         # Update the scroll ID
#         sid = page['_scroll_id']
#         # Get the number of results that we returned in the last scroll
#         scroll_size = len(page['hits']['hits'])
#
#     return query_results


def scroll_query(es_obj, index_name, query_body, include_id):

    # use the scan helper to intitiate the scroll query, this eliminates problems with having too many active scrolls
    # preserve_order True makes the query inefficient, however, there is a bug right now with setting it to false
    #   tracking this issue in https://github.com/elastic/elasticsearch-py/issues/931
    try:
        res = helpers.scan(es_obj, query=query_body, index=index_name, preserve_order=True, request_timeout=60)

        query_results = []  # initialize the results list
        # iterate through the scan results
        for doc in res:
            query_results.append(doc['_source'])
            if include_id:
                query_results[-1]['_id'] = doc['_id']

        return query_results
    except Exception as ex:
        Logger.log.exception("Exception occured")
        raise ex


def dump_index(es_obj, index_name, include_id_flag):
    # dump the contents of an entire index to a list in python, works on indices with any number of documents with
    # a scroll query
    query_body = {
                   "query": {
                       "match_all": {}
                   }
                 }

    return scroll_query(es_obj, index_name, query_body, include_id=include_id_flag)
