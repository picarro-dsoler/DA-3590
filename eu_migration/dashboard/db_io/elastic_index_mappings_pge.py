

def create_pge_plat_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "floc": {"type": "keyword"},
                "main_length_feet": {"type": "float"},
                "num_service_taps": {"type": "integer"},
                "plat_bdy": {"type": "geo_shape"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_leak_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep},
            "analysis": {
                "normalizer": {
                    "lower_case_normalizer": {
                        "type": "custom",
                        "char_filter": [],
                        "filter": ["lowercase", "asciifolding"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "leaknumber": {"type": "keyword"},
                "division": {"type": "keyword"},
                "workcenter": {"type": "keyword"},
                "line_use": {"type": "keyword"},
                "line_use_name": {"type": "keyword"},
                "facility_type": {"type": "keyword"},
                "line_use_type_name": {"type": "keyword"},
                "surface": {"type": "keyword"},
                "grade": {"type": "float"},
                "cp": {"type": "keyword"},
                "expense": {"type": "keyword"},
                "date_reported": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "reported_by": {"type": "keyword"},
                "date_repaired": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "caused_by": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    }
                },
                "repair_type": {
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword",
                                            "normalizer": "lower_case_normalizer"
                                        }
                                    },
                                },
                "material": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    },
                },
                "source": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    },
                },
                "read_location": {
                    "type": "text",
                    "fields": {
                        "english": {
                            "type": "text",
                            "analyzer": "english"
                        },
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    },
                },
                "remarks": {
                    "type": "text",
                    "fields": {
                        "english": {
                            "type": "text",
                            "analyzer": "english"
                        },
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    },
                },
                "reported_floc": {"type": "keyword"},
                "reported_address": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    },
                },
                "lat": {"type": "double"},
                "lon": {"type": "double"},
                "position": {"type": "geo_point"},
                "geocode_accuracy_score": {"type": "float"},
                "geocode_accuracy_type": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    }
                },
                "geocode_address": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    },
                },
                "geocode_source": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    }
                },
                "floc": {"type": "keyword"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_risk_based_survey_index(es_obj, index_name, n_shards, n_rep):

    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "objectid": {"type": "long"},
                "planned_in_2020": {"type": "integer"},
                "floc": {"type": "keyword"},
                "countofservicetaps": {"type": "long"},
                "lof_met_ext_main": {"type": "double"},
                "lof_met_atmos_main": {"type": "double"},
                "lof_all_incorroper_main": {"type": "double"},
                "lof_met_weld_main": {"type": "double"},
                "lof_pls_fusion_main": {"type": "double"},
                "lof_all_pipedope_main": {"type": "double"},
                "lof_all_other_main": {"type": "double"},
                "lof_all_malfunc_main": {"type": "double"},
                "lof_all_compresscoupl_main": {"type": "double"},
                "lof_met_matfail_main": {"type": "double"},
                "lof_pls_matfail_main": {"type": "double"},
                "lof_all_constrdef_main": {"type": "double"},
                "lof_met_ext_service": {"type": "double"},
                "lof_met_atmos_service": {"type": "double"},
                "lof_all_incorroper_service": {"type": "double"},
                "lof_met_weld_service": {"type": "double"},
                "lof_pls_fusion_service": {"type": "double"},
                "lof_all_pipedope_service": {"type": "double"},
                "lof_all_other_service": {"type": "double"},
                "lof_all_compresscoupl_service": {"type": "double"},
                "lof_met_matfail_service": {"type": "double"},
                "lof_pls_matfail_service": {"type": "double"},
                "lof_all_constrdef_service": {"type": "double"},
                "lof_pls_teecap_service": {"type": "double"},
                "lof_main": {"type": "double"},
                "lof_service": {"type": "double"},
                "lof": {"type": "double"},
                "bgp": {"type": "double"},
                "model": {
                    "type": "text",
                    "fields": {
                      "keyword": {"type": "keyword"}
                    }
                  },
                "prediction": {"type": "double"},
                "residual": {"type": "double"},
                "last_survey_start_mike_jackson": {"type": "text"},
                "last_survey_start_iw29_se16n": {"type": "text"},
                "st_area_shape_": {"type": "text"},
                "st_length_shape_": {"type": "text"},
                "last_survey_start_q1_2020": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                }
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_repaired_leak_with_emissions_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep},
            "analysis": {
                "normalizer": {
                    "lower_case_normalizer": {
                        "type": "custom",
                        "char_filter": [],
                        "filter": ["lowercase", "asciifolding"]
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                "emissionrate_scfh": {"type": "double"},
                "emissionrate_mcfy": {"type": "double"},
                "emissionrank": {"type": "integer"},
                "leaknumber": {"type": "keyword"},
                "line_use_name": {"type": "keyword"},
                "surface": {"type": "keyword"},
                "grade": {"type": "integer"},
                "cp": {"type": "keyword"},
                "date_reported": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "date_repaired": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "caused_by": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    }
                },
                "repair_type": {
                                    "type": "text",
                                    "fields": {
                                        "raw": {
                                            "type": "keyword",
                                            "normalizer": "lower_case_normalizer"
                                        }
                                    },
                                },
                "material": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    },
                },
                "source": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    },
                },
                "floc": {"type": "keyword"},
                "reported_address": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    },
                },
                "lat": {"type": "double"},
                "lon": {"type": "double"},
                "position": {"type": "geo_point"},
                "repaired": {"type": "keyword"},
                "measurement": {"type": "keyword"},
                "num_service_taps": {"type": "integer"},
                "main_length_miles": {"type": "double"},
                "leaks_per_thousand_service_taps": {"type": "double"},
                "leaks_per_mile_of_main": {"type": "double"}

            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_gpsraw_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "analyzerid": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    },
                },
                "dt": {"type": "float"},
                "gpsfit": {"type": "integer"},
                "epochtime": {"type": "timestamp"},
                "lat": {"type": "double"},
                "lon": {"type": "double"},
                "position": {"type": "geo_point"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_surveytime_2018_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "analyzerid": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    },
                },
                "floc": {"type": "keyword"},
                "eq_survey_hours": {"type": "double"},
                "compliance_survey_hours": {"type": "double"},
                "num_service_taps": {"type": "integer"},
                "main_length_miles": {"type": "double"},
                "eq_survey_miles_per_hour": {"type": "double"},
                "compliance_survey_miles_per_hour": {"type": "double"},
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_lof_svc_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "lof_mf_pls_matfail": {"type": "float"},
                "division": {"type": "keyword"},
                "num_service_taps": {"type": "integer"},
                "lof_cor_met_ext_norm": {"type": "float"},
                "lof_cor_met_ext": {"type": "float"},
                "floc": {"type": "keyword"},
                "lof_mf_pls_matfail_norm": {"type": "float"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_lof_main_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "lof_mf_pls_matfail": {"type": "float"},
                "division": {"type": "keyword"},
                "lof_cor_met_ext_norm": {"type": "float"},
                "main_length_feet": {"type": "float"},
                "lof_cor_met_ext": {"type": "float"},
                "floc": {"type": "keyword"},
                "lof_mf_pls_matfail_norm": {"type": "float"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_svc_summary_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "diameter": {"type": "float"},
                "num_service_taps": {"type": "integer"},
                "sumservices": {"type": "float"},
                "division": {"type": "keyword"},
                "installyear": {
                                    "type": "date",
                                    "format": "yyyy"
                               },
                "material": {"type": "keyword"},
                "sumservices_norm": {"type": "float"},
                "cptype": {"type": "text"},
                "floc": {"type": "keyword"},
                "plastictype": {"type": "keyword"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_main_summary_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "diameter": {"type": "float"},
                "division": {"type": "keyword"},
                "main_length_feet": {"type": "float"},
                "installyear": {
                                    "type": "date",
                                    "format": "yyyy"
                               },
                "material": {"type": "keyword"},
                "sumlength": {"type": "float"},
                "length_norm": {"type": "float"},
                "cptype": {"type": "text"},
                "floc": {"type": "keyword"},
                "plastictype": {"type": "keyword"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_svctap_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                # "st_astext": {"type": "geo_shape"},
                "svc_tap_shape": {"type": "geo_shape"},
                "installedcompletiondate": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                # "leaksurveygridfuncloc": {"type": "keyword"},
                "floc": {"type": "keyword"},
                "material": {"type": "keyword"},
                "subtypecd": {"type": "keyword"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_intersected_segments_index(es_obj, index_name, n_shards, n_rep):
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
                "gmt_string_start": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSZ"},
                "gmt_string_end": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSZ"},
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
                "floc": {"type": "keyword"},
                "drive_main_ratio": {"type": "float"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_main_leak_dimp_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "OBJECTID": {"type": "integer"},
                "Join_Count": {"type": "integer"},
                "leak_pipe_": {"type": "float"},
                "TARGET_FID": {"type": "integer"},
                "leak__": {"type": "float"},
                "datefound2": {"type": "date", "format": "yyyy-MM-dd"},
                "division_b": {"type": "keyword"},
                "week_numbe": {"type": "keyword"},
                "l01_divisi": {"type": "keyword"},
                "l01_work_c": {"type": "keyword"},
                "line_use": {"type": "keyword"},
                "l01_leak_n": {"type": "keyword"},
                "l01_leak_1": {"type": "keyword"},
                "l01_line_u": {"type": "keyword"},
                "l01_leak_2": {"type": "keyword"},
                "l01_leak_3": {"type": "keyword"},
                "pre_repair": {"type": "keyword"},
                "l01_curren": {"type": "keyword"},
                "street_add": {"type": "text"},
                "l01_locati": {"type": "text"},
                "apt__": {"type": "text"},
                "city": {"type": "keyword"},
                "cp_": {"type": "keyword"},
                "f__cap_exp": {"type": "keyword"},
                "wall": {"type": "keyword"},
                "plat": {"type": "keyword"},
                "block": {"type": "keyword"},
                "dt_reporte": {"type": "date", "format": "yyyy-MM-dd"},
                "reported_b": {"type": "keyword"},
                "repaired_d": {"type": "date", "format": "yyyy-MM-dd"},
                "caused_by": {"type": "keyword"},
                "repair_typ": {"type": "keyword"},
                "material": {"type": "keyword"},
                "cpa__": {"type": "keyword"},
                "source": {"type": "keyword"},
                "l01_leak_4": {"type": "text"},
                "remarks": {"type": "text"},
                "functional": {"type": "keyword"},
                "lon": {"type": "float"},
                "lat": {"type": "float"},
                "position": {"type": "geo_point"},
                "withinsurv": {"type": "keyword"},
                "withinleak": {"type": "keyword"},
                "areaname": {"type": "keyword"},
                "analysis_y": {"type": "keyword"},
                "lof_all_ex": {"type": "float"},
                "lof_met_in": {"type": "float"},
                "lof_met_ex": {"type": "float"},
                "lof_met_at": {"type": "float"},
                "lof_all_xb": {"type": "float"},
                "lof_all_in": {"type": "float"},
                "lof_met_we": {"type": "float"},
                "lof_pls_fu": {"type": "float"},
                "lof_all_ea": {"type": "float"},
                "lof_all_qu": {"type": "float"},
                "lof_all_fl": {"type": "float"},
                "lof_all_li": {"type": "float"},
                "lof_all_ot": {"type": "float"},
                "lof_all_ro": {"type": "float"},
                "lof_all_fi": {"type": "float"},
                "lof_all__1": {"type": "float"},
                "lof_all_pr": {"type": "float"},
                "lof_all_3r": {"type": "float"},
                "lof_all_va": {"type": "float"},
                "lof_all_ve": {"type": "float"},
                "lof_all_pi": {"type": "float"},
                "lof_all__2": {"type": "float"},
                "lof_all_ma": {"type": "float"},
                "lof_all_co": {"type": "float"},
                "primaryobj": {"type": "keyword"},
                "createdby": {"type": "keyword"},
                "createddat": {"type": "keyword"},
                "eventid": {"type": "keyword"},
                "groupevent": {"type": "keyword"},
                "groupdescr": {"type": "keyword"},
                "rfapipemat": {"type": "keyword"},
                "rfapipepla": {"type": "keyword"},
                "rfismetall": {"type": "keyword"},
                "rfisplasti": {"type": "keyword"},
                "sme_janasq": {"type": "float"},
                "sme_fema_f": {"type": "float"},
                "sme_nearfa": {"type": "float"},
                "sme_landsl": {"type": "float"},
                "sme_steel_": {"type": "float"},
                "sme_cp_coa": {"type": "float"},
                "lof_met_ma": {"type": "float"},
                "lof_pls_ma": {"type": "float"},
                "lof_all_el": {"type": "float"},
                "rfapipeins": {"type": "date", "format": "yyyy-MM-dd"},
                "lof_all__3": {"type": "float"},
                "sme_excav": {"type": "float"},
                "rfasme_gdg": {"type": "keyword"},
                "rfasme_g_1": {"type": "keyword"},
                "lof_all_ts": {"type": "float"},
                "sme_tsunam": {"type": "float"},
                "rfasme_ser": {"type": "float"},
                "sme_cpa_le": {"type": "float"},
                "sme_elecma": {"type": "float"},
                "sme_faultc": {"type": "float"},
                "sme_leaked": {"type": "float"},
                "sme_leak_1": {"type": "float"},
                "sme_leak_2": {"type": "float"},
                "sme_leak_3": {"type": "float"},
                "sme_leak_4": {"type": "float"},
                "sme_leak_5": {"type": "float"},
                "sme_leak_6": {"type": "float"},
                "sme_xbore": {"type": "float"},
                "sme_leak_7": {"type": "float"},
                "sme_leak_8": {"type": "float"},
                "sme_intera": {"type": "float"},
                "sme_age_ma": {"type": "float"},
                "sme_age_fu": {"type": "float"},
                "sme_age__1": {"type": "float"},
                "rfasme_xbo": {"type": "float"},
                "main_job": {"type": "keyword"},
                "objectid_1": {"type": "keyword"},
                "areaname_1": {"type": "keyword"},
                "analysis_1": {"type": "keyword"},
                "rof_all_ex": {"type": "float"},
                "rof_met_in": {"type": "float"},
                "rof_met_ex": {"type": "float"},
                "rof_met_at": {"type": "float"},
                "rof_all_xb": {"type": "float"},
                "rof_all_in": {"type": "float"},
                "rof_met_we": {"type": "float"},
                "rof_pls_fu": {"type": "float"},
                "rof_all_ea": {"type": "float"},
                "rof_all_qu": {"type": "float"},
                "rof_all_fl": {"type": "float"},
                "rof_all_li": {"type": "float"},
                "rof_all_ot": {"type": "float"},
                "rof_all_ro": {"type": "float"},
                "rof_all_fi": {"type": "float"},
                "rof_all__1": {"type": "float"},
                "rof_all_pr": {"type": "float"},
                "rof_all_3r": {"type": "float"},
                "rof_all_va": {"type": "float"},
                "rof_all_ve": {"type": "float"},
                "rof_all__2": {"type": "float"},
                "rof_all_pi": {"type": "float"},
                "rof_all_ma": {"type": "float"},
                "rof_all_co": {"type": "float"},
                "primaryo_1": {"type": "keyword"},
                "createdby_": {"type": "keyword"},
                "createdd_1": {"type": "keyword"},
                "eventid_1": {"type": "keyword"},
                "groupeve_1": {"type": "keyword"},
                "groupdes_1": {"type": "keyword"},
                "rof_cor": {"type": "float"},
                "rof_ed": {"type": "float"},
                "rof_ef": {"type": "float"},
                "rof_io": {"type": "float"},
                "rof_mf": {"type": "float"},
                "rof_nf": {"type": "float"},
                "rof_oof": {"type": "float"},
                "rof_oth": {"type": "float"},
                "rof_tot": {"type": "float"},
                "cof_census": {"type": "float"},
                "cof_met_at": {"type": "float"},
                "cof_met_ex": {"type": "float"},
                "cof_met_in": {"type": "float"},
                "cof_all_ex": {"type": "float"},
                "cof_all_co": {"type": "float"},
                "cof_all_ma": {"type": "float"},
                "cof_all_in": {"type": "float"},
                "cof_all_xb": {"type": "float"},
                "cof_met_ma": {"type": "float"},
                "cof_met_we": {"type": "float"},
                "cof_pls_fu": {"type": "float"},
                "cof_pls_ma": {"type": "float"},
                "cof_migrat": {"type": "float"},
                "cof_all_ea": {"type": "float"},
                "cof_all_fl": {"type": "float"},
                "cof_all_li": {"type": "float"},
                "cof_all_ot": {"type": "float"},
                "cof_all_qu": {"type": "float"},
                "cof_all_ro": {"type": "float"},
                "cof_all_3r": {"type": "float"},
                "cof_all_el": {"type": "float"},
                "cof_all_fi": {"type": "float"},
                "cof_all_pr": {"type": "float"},
                "cof_all__1": {"type": "float"},
                "cof_all_va": {"type": "float"},
                "cof_all_ve": {"type": "float"},
                "cof_all__2": {"type": "float"},
                "cof_all_pi": {"type": "float"},
                "cof_pipepr": {"type": "float"},
                "cof_popden": {"type": "float"},
                "rof_all__3": {"type": "float"},
                "cof_all__3": {"type": "float"},
                "rof_met_ma": {"type": "float"},
                "rof_pls_ma": {"type": "float"},
                "rof_all_el": {"type": "float"},
                "cof_rail_c": {"type": "float"},
                "rfacof_pip": {"type": "keyword"},
                "cof_all_ts": {"type": "float"},
                "rof_all_ts": {"type": "float"},
                "rfisplas_1": {"type": "keyword"},
                "rfismeta_1": {"type": "keyword"},
                "main_job_1": {"type": "keyword"},
                "type": {"type": "keyword"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pge_svc_leak_dimp_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "OBJECTID": {"type": "integer"},
                "Join_Count": {"type": "integer"},
                "leak_pipe_": {"type": "float"},
                "TARGET_FID": {"type": "integer"},
                "leak__": {"type": "float"},
                "datefound2": {"type": "date", "format": "yyyy-MM-dd"},
                "division_b": {"type": "keyword"},
                "week_numbe": {"type": "keyword"},
                "l01_divisi": {"type": "keyword"},
                "l01_work_c": {"type": "keyword"},
                "line_use": {"type": "keyword"},
                "l01_leak_n": {"type": "keyword"},
                "l01_leak_1": {"type": "keyword"},
                "l01_line_u": {"type": "keyword"},
                "l01_leak_2": {"type": "keyword"},
                "l01_leak_3": {"type": "integer"},
                "pre_repair": {"type": "integer"},
                "l01_curren": {"type": "integer"},
                "street_add": {"type": "text"},
                "l01_locati": {"type": "text"},
                "apt__": {"type": "text"},
                "city": {"type": "keyword"},
                "cp_": {"type": "keyword"},
                "f__cap_exp": {"type": "keyword"},
                "wall": {"type": "keyword"},
                "plat": {"type": "keyword"},
                "block": {"type": "keyword"},
                "dt_reporte": {"type": "date", "format": "yyyy-MM-dd"},
                "reported_b": {"type": "keyword"},
                "repaired_d": {"type": "date", "format": "yyyy-MM-dd"},
                "caused_by": {"type": "keyword"},
                "repair_typ": {"type": "keyword"},
                "material": {"type": "keyword"},
                "cpa__": {"type": "keyword"},
                "source": {"type": "keyword"},
                "l01_leak_4": {"type": "text"},
                "remarks": {"type": "text"},
                "functional": {"type": "keyword"},
                "lon": {"type": "float"},
                "lat": {"type": "float"},
                "position": {"type": "geo_point"},
                "withinsurv": {"type": "keyword"},
                "withinleak": {"type": "keyword"},
                "areaname": {"type": "keyword"},
                "analysis_y": {"type": "keyword"},
                "lof_all_ex": {"type": "float"},
                "lof_met_in": {"type": "float"},
                "lof_met_ex": {"type": "float"},
                "lof_met_at": {"type": "float"},
                "lof_all_xb": {"type": "float"},
                "lof_all_in": {"type": "float"},
                "lof_met_we": {"type": "float"},
                "lof_pls_fu": {"type": "float"},
                "lof_all_ea": {"type": "float"},
                "lof_all_qu": {"type": "float"},
                "lof_all_fl": {"type": "float"},
                "lof_all_li": {"type": "float"},
                "lof_all_ot": {"type": "float"},
                "lof_all_ro": {"type": "float"},
                "lof_all_fi": {"type": "float"},
                "lof_all__1": {"type": "float"},
                "lof_all_pr": {"type": "float"},
                "lof_all_3r": {"type": "float"},
                "lof_all_va": {"type": "float"},
                "lof_all_ve": {"type": "float"},
                "lof_all_pi": {"type": "float"},
                "lof_all__2": {"type": "float"},
                "lof_all_ma": {"type": "float"},
                "lof_all_co": {"type": "float"},
                "primaryobj": {"type": "integer"},
                "createdby": {"type": "keyword"},
                "createddat": {"type": "keyword"},
                "eventid": {"type": "keyword"},
                "groupevent": {"type": "keyword"},
                "groupdescr": {"type": "keyword"},
                "rfapipemat": {"type": "keyword"},
                "rfapipepla": {"type": "keyword"},
                "rfismetall": {"type": "keyword"},
                "rfisplasti": {"type": "keyword"},
                "sme_xbore": {"type": "float"},
                "sme_nf_fem": {"type": "float"},
                "sme_nf_nea": {"type": "float"},
                "sme_landsl": {"type": "float"},
                "lof_met_ma": {"type": "float"},
                "lof_pls_ma": {"type": "float"},
                "lof_all_el": {"type": "float"},
                "rfapipeins": {"type": "date", "format": "yyyy-MM-dd"},
                "sme_oof_el": {"type": "keyword"},
                "rfasme_joi": {"type": "keyword"},
                "lof_all__3": {"type": "float"},
                "sme_plasin": {"type": "float"},
                "sme_excav": {"type": "float"},
                "lof_all_ts": {"type": "float"},
                "sme_tsunam": {"type": "float"},
                "sme_cpa_le": {"type": "float"},
                "sme_leaked": {"type": "float"},
                "sme_leak_1": {"type": "float"},
                "sme_leak_2": {"type": "float"},
                "sme_leak_3": {"type": "float"},
                "sme_leak_4": {"type": "float"},
                "sme_elecma": {"type": "float"},
                "sme_leak_5": {"type": "float"},
                "sme_age_ma": {"type": "float"},
                "sme_age_fu": {"type": "float"},
                "sme_age__1": {"type": "float"},
                "lof_pls_te": {"type": "float"},
                "rfasme_xbo": {"type": "keyword"},
                "main_job": {"type": "keyword"},
                "objectid_1": {"type": "keyword"},
                "areaname_1": {"type": "keyword"},
                "analysis_1": {"type": "keyword"},
                "rof_all_ex": {"type": "float"},
                "rof_met_in": {"type": "float"},
                "rof_met_ex": {"type": "float"},
                "rof_met_at": {"type": "float"},
                "rof_all_xb": {"type": "float"},
                "rof_all_in": {"type": "float"},
                "rof_met_we": {"type": "float"},
                "rof_pls_fu": {"type": "float"},
                "rof_all_ea": {"type": "float"},
                "rof_all_qu": {"type": "float"},
                "rof_all_fl": {"type": "float"},
                "rof_all_li": {"type": "float"},
                "rof_all_ot": {"type": "float"},
                "rof_all_ro": {"type": "float"},
                "rof_all_fi": {"type": "float"},
                "rof_all__1": {"type": "float"},
                "rof_all_pr": {"type": "float"},
                "rof_all_3r": {"type": "float"},
                "rof_all_va": {"type": "float"},
                "rof_all_ve": {"type": "float"},
                "rof_all__2": {"type": "float"},
                "rof_all_pi": {"type": "float"},
                "rof_all_ma": {"type": "float"},
                "rof_all_co": {"type": "float"},
                "primaryo_1": {"type": "keyword"},
                "createdby_": {"type": "keyword"},
                "createdd_1": {"type": "keyword"},
                "eventid_1": {"type": "keyword"},
                "groupeve_1": {"type": "keyword"},
                "groupdes_1": {"type": "keyword"},
                "rof_cor": {"type": "float"},
                "rof_ed": {"type": "float"},
                "rof_ef": {"type": "float"},
                "rof_io": {"type": "float"},
                "rof_mf": {"type": "float"},
                "rof_nf": {"type": "float"},
                "rof_oof": {"type": "float"},
                "rof_oth": {"type": "float"},
                "rof_tot": {"type": "float"},
                "rfacof_efv": {"type": "keyword"},
                "cof_census": {"type": "float"},
                "cof_met_at": {"type": "float"},
                "cof_met_ex": {"type": "float"},
                "cof_met_in": {"type": "float"},
                "cof_all_ex": {"type": "float"},
                "cof_all_co": {"type": "float"},
                "cof_all_ma": {"type": "float"},
                "cof_all_in": {"type": "float"},
                "cof_all_xb": {"type": "float"},
                "cof_met_ma": {"type": "float"},
                "cof_met_we": {"type": "float"},
                "cof_pls_fu": {"type": "float"},
                "cof_pls_ma": {"type": "float"},
                "cof_migrat": {"type": "float"},
                "cof_all_ea": {"type": "float"},
                "cof_all_fl": {"type": "float"},
                "cof_all_li": {"type": "float"},
                "cof_all_ot": {"type": "float"},
                "cof_all_qu": {"type": "float"},
                "cof_all_ro": {"type": "float"},
                "cof_all_3r": {"type": "float"},
                "cof_all_el": {"type": "float"},
                "cof_all_fi": {"type": "float"},
                "cof_all_pr": {"type": "float"},
                "cof_all__1": {"type": "float"},
                "cof_all_va": {"type": "float"},
                "cof_all_ve": {"type": "float"},
                "cof_all_pi": {"type": "float"},
                "cof_pipepr": {"type": "float"},
                "cof_popden": {"type": "float"},
                "rof_all__3": {"type": "float"},
                "cof_all__2": {"type": "float"},
                "rof_met_ma": {"type": "float"},
                "rof_pls_ma": {"type": "float"},
                "rof_all_el": {"type": "float"},
                "cof_rail_c": {"type": "float"},
                "rfacof_pip": {"type": "keyword"},
                "cof_all_ts": {"type": "float"},
                "rof_all_ts": {"type": "float"},
                "cof_all__3": {"type": "float"},
                "rfismeta_1": {"type": "keyword"},
                "rfisplas_1": {"type": "keyword"},
                "cof_pls_te": {"type": "float"},
                "rof_pls_te": {"type": "float"},
                "main_job_1": {"type": "keyword"},
                "shape_leng": {"type": "float"},
                "type": {"type": "keyword"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res
