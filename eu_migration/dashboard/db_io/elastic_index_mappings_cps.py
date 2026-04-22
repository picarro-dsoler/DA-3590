

def create_cps_bdy_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "bdy_shape": {"type": "geo_shape"},
                "ExternalId": {"type": "keyword"},
                "CustomerBo": {"type": "keyword"},
                "id": {"type": "keyword"},
                "Descriptio": {"type": "keyword"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_cps_intersected_segments_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep},
            "analysis": {
                "normalizer": {
                    "user_name_normalizer": {
                        "type": "custom",
                        "char_filter": [],
                        "filter": ["lowercase", "asciifolding"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "epoch_time_start": {"type": "date", "format": "epoch_second"},
                "epoch_time_end": {"type": "date", "format": "epoch_second"},
                "gmt_string_start": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSS"},
                "gmt_string_end": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSS"},
                "survey_id": {"type": "keyword"},
                "description": {"type": "keyword"},
                "analyzer_id": {"type": "keyword"},
                "surveyor_unit_id": {"type": "text"},
                "customer_name": {"type": "keyword"},
                "survey_tag": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    }},
                "first_name": {
                    "type": "keyword",
                    "normalizer": "user_name_normalizer"
                },
                "last_name": {
                    "type": "keyword",
                    "normalizer": "user_name_normalizer"
                },
                "segment_mode": {"type": "integer"},
                "segment_shape": {"type": "geo_shape"},
                "segment_len_m": {"type": "float"},
                "segment_len_miles": {"type": "float"},
                "id": {"type": "keyword"},
                "drive_main_ratio": {"type": "float"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_cps_intersected_peaks_ind(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep},
            "analysis": {
                "normalizer": {
                    "user_name_normalizer": {
                        "type": "custom",
                        "char_filter": [],
                        "filter": ["lowercase", "asciifolding"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "epoch_time": {"type": "date", "format": "epoch_second"},
                "time_string": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSS"},
                "last_time_string": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSS"},
                "first_time_string": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSS"},
                "min_ch4": {"type": "float"},
                "max_ch4": {"type": "float"},
                "med_ch4": {"type": "float"},
                "min_amplitude": {"type": "float"},
                "max_amplitude": {"type": "float"},
                "med_amplitude": {"type": "float"},
                "position": {"type": "geo_point"},
                "position_shape": {"type": "geo_shape"},
                "analyzer_id": {"type": "keyword"},
                "survey_ids": {"type": "text"},
                "survey_tags": {"type": "text"},
                "customer_name": {"type": "keyword"},
                "first_name": {
                    "type": "keyword",
                    "normalizer": "user_name_normalizer"
                },
                "last_name": {
                    "type": "keyword",
                    "normalizer": "user_name_normalizer"
                },
                "ExternalId": {"type": "keyword"},
                "id": {"type": "keyword"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res
