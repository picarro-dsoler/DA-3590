

def create_bge_demo_region_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "num_leaks": {"type": "integer"},
                "emissions": {"type": "float"},
                "operator": {"type": "keyword"},
                "date": {
                            "type": "date",
                            "format": "yyyy-MM-dd"
                        },
                "surveyor": {"type": "keyword"},
                "main_miles": {"type": "float"},
                "leaks_main_ratio": {"type": "float"},
                "emission_main_ratio": {"type": "float"},
                "emissions_leaks_ratio": {"type": "float"},
                "region_repair_cost": {"type": "float"},
                "emission_rank": {"type": "integer"},
                "risk_rank": {"type": "integer"},
                "position": {"type": "geo_point"},
                "cast_iron_mileage": {"type": "float"},
                "bare_steel_mileage": {"type": "float"},
                "plastic_mileage": {"type": "float"},
                "coated_steel_mileage": {"type": "float"},
                "cast_iron_frac": {"type": "float"},
                "bare_steel_frac": {"type": "float"},
                "plastic_frac": {"type": "float"},
                "coated_steel_frac": {"type": "float"},
                "cast_iron_replacement_cost": {"type": "float"},
                "bare_steel_replacement_cost": {"type": "float"},
                "total_replacement_cost": {"type": "float"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_bge_demo_region_breakout_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "num_leaks": {"type": "integer"},
                "emissions": {"type": "float"},
                "operator": {"type": "keyword"},
                "date": {
                            "type": "date",
                            "format": "yyyy-MM-dd"
                        },
                "surveyor": {"type": "keyword"},
                "main_miles": {"type": "float"},
                "leaks_main_ratio": {"type": "float"},
                "emission_main_ratio": {"type": "float"},
                "emissions_leaks_ratio": {"type": "float"},
                "region_repair_cost": {"type": "float"},
                "emission_rank": {"type": "integer"},
                "risk_rank": {"type": "integer"},
                "position": {"type": "geo_point"},
                "pipe_type": {"type": "keyword"},
                "sub_type_mileage": {"type": "float"},
                "sub_type_frac": {"type": "float"},
                "pipe_replacement_cost": {"type": "float"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res


def create_bge_demo_leak_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "emissions": {"type": "float"},
                "repair_cost": {"type": "float"},
                "position": {"type": "geo_point"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res
