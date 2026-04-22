

def create_eq_peak_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            # "index.mapping.ignore_malformed": True,
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "amplitude": {"type": "float"},
                "analyzer_id": {"type": "keyword"},
                "analyzer_status": {"type": "float"},
                "beta": {"type": "float"},
                "bkg_ch4": {"type": "float"},
                "car_bearing": {"type": "float"},
                "car_speed": {"type": "float"},
                "car_speed_east": {"type": "float"},
                "car_speed_north": {"type": "float"},
                "ch4": {"type": "float"},
                "emission_rate": {"type": "float"},
                "emission_rate_uncertainty": {"type": "float"},
                "epoch_plume_end": {"type": "float"},
                "epoch_plume_start": {"type": "float"},
                "epoch_time": {"type": "float"},
                "gmt_string": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSS"},
                "ethane_conc_std": {"type": "float"},
                "ethane_ratio": {"type": "float"},
                "ethane_ratio_std": {"type": "float"},
                "Id": {"type": "keyword"},
                "position": {"type": "geo_point"},
                "lisa_poly": {"type": "geo_shape"},
                "lat": {"type": "float"},
                "lon": {"type": "float"},
                "max_ch4": {"type": "float"},
                "maximum_car_speed": {"type": "float"},
                "maximum_wind_speed": {"type": "float"},
                "mean_wind_dir": {"type": "float"},
                "mean_wind_speed": {"type": "float"},
                "measurement_id": {"type": "float"},
                "survey_id": {"type": "keyword"},
                "survey_mode": {"type": "keyword"},
                "wind_speed_north": {"type": "float"},
                "wind_speed_east": {"type": "float"},
                "wind_speed": {"type": "float"},
                "median_wind_dir": {"type": "float"},
                "wind_dir_std": {"type": "float"},
                "qa_alarm": {"type": "float"},
                "qa_flag":  {"type": "float"},
                "uuid_str": {"type": "text"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_eq_source_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "avg_epoch": {"type": "float"},
                "gmt_string_event": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSS"},
                "emission_rate": {"type": "float"},
                "amplitude": {"type": "float"},
                "car_speed": {"type": "float"},
                "car_speed_east": {"type": "float"},
                "car_speed_north": {"type": "float"},
                "confidence": {"type": "float"},
                "coord": {"type": "geo_shape"},
                "cumul_percent": {"type": "float"},
                "detection_probability": {"type": "float"},
                "disposition": {"type": "float"},
                "emission_rank": {"type": "float"},
                "emission_rate_amean": {"type": "float"},
                "emission_rate_astd": {"type": "float"},
                "emission_rate_gmean": {"type": "float"},
                "emission_rate_gstd": {"type": "float"},
                "ethane_ratio": {"type": "float"},
                "ethane_ratio_uncertainty": {"type": "float"},
                "isfiltered": {"type": "float"},
                "position": {"type": "geo_point"},
                "lisa_poly": {"type": "geo_shape"},
                "num_passes": {"type": "float"},
                "num_peaks": {"type": "float"},
                "wind_dir": {"type": "float"},
                "wind_speed": {"type": "float"},
                "maximum_wind_speed": {"type": "float"},
                "wind_speed_east": {"type": "float"},
                "wind_speed_north": {"type": "float"},
                "uuid_str": {"type": "text"},
                "survey_mode": {"type": "keyword"},
                "first_name": {"type": "keyword"},
                "last_name": {"type": "keyword"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_analytics_source_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "amplitude": {"type": "float"},
                "car_speed": {"type": "float"},
                "car_speed_east": {"type": "float"},
                "car_speed_north": {"type": "float"},
                "confidence": {"type": "float"},
                "coord": {"type": "geo_shape"},
                "emission_rank": {"type": "float"},
                "emission_rate": {"type": "float"},
                "emission_rate_amean": {"type": "float"},
                "emission_rate_astd": {"type": "float"},
                "emission_rate_gmean": {"type": "float"},
                "emission_rate_gstd": {"type": "float"},
                "disposition": {"type": "float"},
                "detection_probability": {"type": "float"},
                "epoch_time": {"type": "float"},
                "ethane_ratio": {"type": "float"},
                "ethane_ratio_uncertainty": {"type": "float"},
                "gmt_string_event": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSS"},
                "isfiltered": {"type": "boolean"},
                "lisa_poly": {"type": "geo_shape"},
                "num_passes": {"type": "float"},
                "num_peaks": {"type": "float"},
                "position": {"type": "geo_point"},
                "priority_score": {"type": "float"},
                "report_id": {"type": "keyword"},
                "wind_dir": {"type": "float"},
                "wind_dir_std": {"type": "float"},
                "wind_speed": {"type": "float"},
                "wind_speed_east": {"type": "float"},
                "wind_speed_north": {"type": "float"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_segments_index(es_obj, index_name, n_shards, n_rep):
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
                # "epoch_time_start": {"type": "date", "format": "epoch_second"},
                # "epoch_time_end": {"type": "date", "format": "epoch_second"},
                "epoch_time_start": {"type": "float"},
                "epoch_time_end": {"type": "float"},
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
                "segment_order": {"type": "integer"},
                "segment_mode": {"type": "integer"},
                "segment_shape": {"type": "geo_shape"},
                "segment_len_m": {"type": "float"},
                "bdy_id": {"type": "keyword"},
                "main_length": {"type": "float"},
                "main_length_unit": {"type": "keyword"},
                "num_svc": {"type": "integer"},
                "car_speed": {"type": "float"},
                "wind_speed": {"type": "float"},
                "position": {"type": "geo_point"},
                "user_name": {"type": "keyword"},
                "unit_name": {"type": "keyword"},
                "serial_num": {"type": "keyword"}
                # "car_speed_dist": {"type": "float"},
                # "wind_speed_dist": {"type": "float"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_fov_index(es_obj, index_name, n_shards, n_rep):
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
                "report_id": {"type": "keyword"},
                "fov_id": {"type": "keyword"},
                "fov_shape": {"type": "geo_shape"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_bdy_survey_cvg_index(es_obj, index_name, n_shards, n_rep):
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
                "total_drive_distance_ft": {"type": "float"},
                "year": {"type": "integer"},
                "drive_main_ratio": {"type": "float"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_time_space_peaks_ind(es_obj, index_name, n_shards, n_rep):
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
                "n_peaks": {"type": "integer"},
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
                "analyzer_ids": {"type": "text"},
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
                "uuids_string": {"type": "text"},
                "survey_mode": {"type": "keyword"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_pipeline_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "feature_shape": {"type": "geo_shape"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_investigation_index(es_obj, index_name, n_shards, n_rep):
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
                "AG/BG": {"type": "keyword"},
                "BoundaryName": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    }},
                "BoxId": {"type": "keyword"},
                "City": {"type": "keyword"},
                # "FoundDateTime": {
                #     "type": "date",
                #     "format": "yyyy-MM-dd"
                # },
                "FoundDateTime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "InvestigationCompleteDateTime": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "FoundDateTimeStr": {
                    "type": "keyword"
                },
                "FoundDateStr": {"type": "text"},
                "LeakFound": {"type": "keyword"},
                "LeakGpsPrecision": {"type": "float"},
                "LeakGrade": {"type": "keyword"},
                "LeakCoord": {"type": "geo_shape"},
                "LisaShape": {"type": "geo_shape"},
                'LeakCoordLatLon': {"type": "text"},
                "LeakCoordPoint": {"type": "geo_point"},
                "LeakLocation": {"type": "keyword"},
                "LeakSource": {"type": "keyword"},
                "LocationRemarks": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    }
                },
                "Notes": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "normalizer": "lower_case_normalizer"
                        }
                    }
                },
                "PCubedReportName": {"type": "keyword"},
                "PeakCoord": {"type": "geo_shape"},
                'PeakCoordLatLon': {"type": "text"},
                "PeakEpoch": {"type": "double"},
                "TimeToInvestigation": {"type": "double"},
                "PipelineMeters": {"type": "double"},
                "Region": {"type": "keyword"},
                "PCubedReportTitle": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    },
                },
                "PCubedReportDate": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "AssetCoverageFrac": {"type": "double"},
                "PCubedReportDateStr": {"type": "text"},
                "PCubedReportGuid": {"type": "keyword"},
                "LastSurveyDate": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "LastSurveyDateStr": {"type": "text"},
                "LeakInvestigatorUserName": {"type": "keyword"},
                "SurveyorUnitName": {"type": "keyword"},
                "UserName": {"type": "keyword"},
                "first_name": {
                    "type": "keyword",
                    "normalizer": "lower_case_normalizer"
                },
                "last_name": {
                    "type": "keyword",
                    "normalizer": "lower_case_normalizer"
                },
                "LeakInvestigationSeconds": {"type": "long"},
                "InvestigationMinutes": {"type": "double"},
                "type1coverage": {"type": "double"},
                "leak_denisty_on_bdy": {"type": "double"},
                "contract": {"type": "keyword"},
                "assignment": {"type": "keyword"},
                'DrivingStatus':  {"type": "keyword"},
                'InvestigationStatus':  {"type": "keyword"},
                'Amplitude': {"type": "double"},
                'CH4': {"type": "double"},
                'AggregatedEthaneRatio': {"type": "double"},
                'AggregatedDisposition': {"type": "keyword"},
                'AggregatedClassificationConfidence': {"type": "double"},
                'PeakNumber': {"type": "keyword"},
                'EthaneRatioSdev': {"type": "double"},
                'Sigma': {"type": "double"},
                'PriorityScore': {"type": "double"},
                'EmissionRate': {"type": "double"},
                'EmissionRateUpperBound': {"type": "double"},
                'EmissionRateLowerBound': {"type": "double"},
                'StreetNumber': {"type": "text"},
                'ApartmentNumber': {"type": "text"},
                'StreetName': {"type": "text"},
                'UserCity': {"type": "text"},
                'State': {"type": "text"},
                'SurfaceReading': {"type": "text"},
                'ReadingUnitSurface': {"type": "keyword"},
                'BarholeReading': {"type": "text"},
                'ReadingUnitBarhole': {"type": "keyword"},
                'NotInvestigatedUuid': {"type": "keyword"},
                'InvestigatedUuid': {"type": "keyword"},
                'InProgressUuid': {"type": "keyword"},
                'FoundGasLeakUuid': {"type": "keyword"},
                'CountBoxUuid': {"type": "keyword"},
                'FoundAtLeastOneUuid': {"type": "keyword"},
                'UpdateTimeString': {"type": "keyword"},
                'InstType': {"type": "keyword"},
                'InstSerNum': {"type": "keyword"},
                'CgiComment': {"type": "text"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_italgas_g2g_index(es_obj, index_name, n_shards, n_rep):
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
        "mappings" : {
        "_meta" : { },
        "properties" : {
            "AggregatedClassificationConfidence" : {"type" : "double"},
            "AggregatedDisposition" : {"type" : "keyword"},
            "AggregatedEthaneRatio" : {"type" : "double"},
            "Amplitude" : {"type" : "double"},
            "AssetCoverageFrac" : {"type" : "double"},
            "BoxId" : {"type" : "keyword"},
            "CH4" : {"type" : "double"},
            "City" : {"type" : "keyword"},
            "DrivingStatus" : {"type" : "keyword"},
            "EmissionRate" : {"type" : "double"},
            "EmissionRateLowerBound" : {"type" : "double"},
            "EmissionRateUpperBound" : {"type" : "double"},
            "EthaneRatioSdev" : {"type" : "double"},
            "LastSurveyDate" : {"type" : "date",
                                "format": "yyyy-MM-dd"},
            "LastSurveyDateStr" : {"type" : "keyword"},
            "LeakCoord": {"type": "geo_shape"},
            'LeakCoordLatLon': {"type": "text"},
            "LeakCoordPoint": {"type": "geo_point"},
            "LeakFound" : {"type" : "keyword"},
            "PCubedReportDate" : {"type" : "date",
                                  "format": "yyyy-MM-dd"},
            "PCubedReportDateStr" : {"type" : "keyword"},
            "PCubedReportGuid" : {"type" : "keyword"},
            "PCubedReportName" : {"type" : "keyword"},
            "PCubedReportTitle" : {"type" : "keyword"},
            "PeakCoord": {"type": "geo_shape"},
            'PeakCoordLatLon': {"type": "text"},
            "PeakEpoch" : {"type" : "keyword"},
            "PeakNumber" : {"type" : "keyword"},
            "PipelineMeters" : {"type" : "double"},
            "PriorityScore" : {"type" : "double"},
            "Region" : {"type" : "keyword"},
            "Sigma" : {"type" : "double"},
            "SurveyorUnitName" : {"type" : "keyword"},
            "UpdateTimeString" : {"type" : "keyword"},
            "UserName" : {"type" : "keyword"},
            "aereoInterrato" : {"type" : "keyword"},
            "cap" : {"type" : "long"},
            "codStato" : {"type" : "long"},
            "codValidazione" : {"type" : "double"},
            "codiceDispersione" : {"type" : "keyword"},
            "comune" : {"type" : "keyword"},
            "contract" : {"type" : "keyword"},
            "dataArrivoSulCampo" : {"type" : "double"},
            "dataInserimento" : {
              "type" : "date",
              "format" : "epoch_second"},
            "dataLocalizzazione" : {"type" : "double"},
            "dataRiparazione" : {"type" : "keyword"},
            "dataUltimaMod" : {
              "type" : "date",
              "format" : "epoch_second"},
            "indirizzo" : {"type" : "keyword"},
            "indirizzoLisa" : {"type" : "keyword"},
            "indirizzoLocalizzazione" : {"type" : "keyword"},
            "indirizzoRiparazione" : {"type" : "keyword"},
            "intervento" : {"type" : "keyword"},
            "leakId" : {"type" : "long"},
            "leak_denisty_on_bdy" : {"type" : "double"},
            "lisa" : {"type" : "keyword"},
            "lisaId" : {"type" : "long"},
            "numProgressivo" : {"type" : "long"},
            "picarroLastUpdated" : {"type" : "date",
              "format" : "epoch_second"},
            "reportId" : {"type" : "keyword"},
            "statoFoglietta" : {"type" : "keyword"},
            "statoValidazione" : {"type" : "keyword"},
            "type1coverage" : {"type" : "double"},
            'NotInvestigatedUuid': {"type": "keyword"},
            'InvestigatedUuid': {"type": "keyword"},
            'InProgressUuid': {"type": "keyword"},
            'FoundGasLeakUuid': {"type": "keyword"},
            'CountBoxUuid': {"type": "keyword"},
            'FoundAtLeastOneUuid': {"type": "keyword"}
            }
        }
    }

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_eq_source_report_index(es_obj, index_name, n_shards, n_rep):
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
                "report_id": {"type": "keyword"},
                "emission_rank": {"type": "integer"},
                "emission_rate": {"type": "float"},
                "position": {"type": "geo_point"},
                "address": {"type": "text"},
                "num_peaks": {"type": "integer"},
                "num_passes": {"type": "integer"},
                "report_tile": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    }
                },
                "avg_epoch": {"type": "float"},
                "coord": {"type": "geo_shape"},
                "gmt_string_event": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss.SSS"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


# def create_main_points_index(es_obj, index_name, n_shards, n_rep):
#     # format the index, settings and mappings for data
#     index_body = {
#         "settings": {
#             "index": {
#                 "number_of_shards": n_shards,
#                 "number_of_replicas": n_rep
#             }
#         },
#         "mappings": {
#             "properties": {
#                 "uuid": {"type": "keyword"},
#                 "lat": {"type": "float"},
#                 "lon": {"type": "float"},
#                 "position": {"type": "geo_point"},
#                 "coord": {"type": "geo_shape"}
#             }
#         }}
#
#     # create the raw data index in the db
#     res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)
#
#     return res


def create_main_lines_index(es_obj, index_name, n_shards, n_rep):
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
                "uuid": {"type": "keyword"},
                "dist_fac": {"type": "float"},
                "line_shape": {"type": "geo_shape"},
                "line_len_m": {"type": "float"},
                "lat": {"type": "float"},
                "lon": {"type": "float"},
                "position": {"type": "geo_point"},
                "bdy_id": {"type": "keyword"},
                "last_pass_epoch": {"type": "date", "format": "epoch_second"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_main_passes_index(es_obj, index_name, n_shards, n_rep):
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
                "main_point_uuid": {"type": "keyword"},
                "num_main_passes": {"type": "integer"},
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
                "segment_order": {"type": "integer"},
                "segment_mode": {"type": "integer"},
                "segment_len_m": {"type": "float"},
                "bdy_id": {"type": "keyword"},
                "main_length": {"type": "float"},
                "main_length_unit": {"type": "keyword"},
                "num_svc": {"type": "integer"},
                "car_speed": {"type": "float"},
                "wind_speed": {"type": "float"},
                "line_shape": {"type": "geo_shape"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_ls_log_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            # "index.mapping.ignore_malformed": True,
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "gmt_time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                "epoch_time": {"type": "float"},
                # "last_heartbeat": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                "last_heartbeat": {"type": "keyword"},
                "address": {"type": "keyword"},
                "db_size": {"type": "float"},
                "available": {"type": "float"},
                "status_str": {"type": "keyword"},
                "status_code": {"type": "integer"}
    }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res
