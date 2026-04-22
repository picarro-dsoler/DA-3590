

def create_italgas_leaks_with_emissions_index(es_obj, index_name, n_shards, n_rep):
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
                "emissionrate_scfh": {"type": "double"},
                "emissionrate_mcmy": {"type": "double"},
                "emissionrank": {"type": "integer"},
                "leak_id": {"type": "keyword"},
                "lat": {"type": "double"},
                "lon": {"type": "double"},
                "position": {"type": "geo_point"},
                "description": {"type": "keyword"},
                "main_length_km": {"type": "double"},
                "fraction_of_total_emissions": {"type": "double"},
                "emissions_cumsum": {"type": "double"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_italgas_risk_based_survey_index(es_obj, index_name, n_shards, n_rep):
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
                "description": {"type": "keyword"},
                "main_length_km": {"type": "double"},
                "prediction": {"type": "double"},
                "residual": {"type": "double"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_sarl_bdy_index(es_obj, index_name, n_shards, n_rep):
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
                "id": {"type": "integer"},
                "bndary_oid": {"type": "keyword"},
                "name": {"type": "keyword"},
                "plant": {"type": "keyword"},
                "region":  {"type": "keyword"},
                "pipe_len": {"type": "double"},
                "code1_len": {"type": "double"},
                "rpt_name": {"type": "keyword"},
                "bdy_shape":  {"type": "geo_shape"},
                "num_service_taps": {"type": "integer"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res
