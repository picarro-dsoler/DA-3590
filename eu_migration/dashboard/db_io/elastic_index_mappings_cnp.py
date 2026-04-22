

def create_cnp_eqsources(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep
            }
        },
        "mappings": {
            "properties": {
                "avg_epoch": {"type": "integer"},
                "gmt_string_event": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSS"},
                "emission_rate": {"type": "double"},
                "cumul_percent": {"type": "double"},
                "lat": {"type": "double"},
                "lon": {"type": "double"},
                "position": {"type": "geo_point"},
                "coord": {"type": "geo_shape"}
            }
        }
    }

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_cnp_eqsource_points_rbs(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep
            }
        },
        "mappings": {
            "properties": {
                "objectid": {"type": "integer"},
                "emissionrate": {"type": "double"},
                "belowgroundprobability": {"type": "double"},
                "orig_fid": {"type": "integer"},
                "lat": {"type": "double"},
                "lon": {"type": "double"},
                "position": {"type": "geo_point"},
                "coord_shape": {"type": "geo_shape"}
            }
        }
    }

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_cnp_survey_areas(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep
            }
        },
        "mappings": {
            "properties": {
                "bdy": {"type": "geo_shape"},
                "objectid": {"type": "integer"},
                "name": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    },
                },
                "sap_equip_number": {"type": "integer"},
                "cnp_scoping_summary": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    },
                },
                "geographical_reference_from_sco": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    },
                },
                "f2020_replacement_miles": {"type": "double"},
                "miles_in_boundary": {"type": "double"},
                "Shape__Area": {"type": "double"},
                "Shape__Length": {"type": "double"}
            }
        }
    }

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_cnp_survey_area_grids(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep
            }
        },
        "mappings": {
            "properties": {
                "bdy": {"type": "geo_shape"},
                "objectid": {"type": "integer"},
                "join_count": {"type": "integer"},
                "target_fid": {"type": "integer"},
                "objectid_orig": {"type": "integer"},
                "name": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    },
                },
                "sap_equip_number": {"type": "integer"},
                "mainlengthmiles": {"type": "double"},
                "Shape__Area": {"type": "double"},
                "Shape__Length": {"type": "double"}
            }
        }
    }

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_cnp_gas_mains(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep
            }
        },
        "mappings": {
            "properties": {
                "line_str": {"type": "geo_shape"},
                "objectid": {"type": "integer"},
                "material_cd": {"type": "integer"},
                "pressure_cd": {"type": "integer"},
                "size_cd": {"type": "integer"},
                "Shape__Length": {"type": "double"}
            }
        }
    }

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_cnp_mains_with_leaks(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep
            }
        },
        "mappings": {
            "properties": {
                "line_str": {"type": "geo_shape"},
                "objectid": {"type": "integer"},
                "emissionrate": {"type": "double"},
                "belowgroundprobability": {"type": "double"},
                "Shape__Length": {"type": "double"}
            }
        }
    }

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res
