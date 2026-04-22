def create_consumers_leaks_with_emissions_index(es_obj, index_name, n_shards, n_rep):
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
                "leak_id": {"type": "keyword"},
                "lat": {"type": "double"},
                "lon": {"type": "double"},
                "position": {"type": "geo_point"},
                "grid_id": {"type": "keyword"},
                "main_length_miles": {"type": "double"},
                "fraction_of_total_emissions": {"type": "double"},
                "emissions_cumsum": {"type": "double"}
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res