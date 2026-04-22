from dashboard.db_io.elastic_queries import scroll_query


def points_intersect_poly(es_obj,
                          index_query, field_query,
                          poly_vtx,
                          source_keys):
    # geo_shape query
    # query for all geo_shapes that intersect an indexed polygon of geo_shape type
    # Inputs:   index_query - the index containing the shapes to be queried
    #           field_query - the field name to be queried
    #           poly_vtx - vertices of the polygon bounding envelope
    #           source_keys - a list of strings with the field keys to return from the queried documents
    #               use wildcard * to get all keys
    #
    # Outputs:  query_results - a list of dictionaries returning the intersecting shapes

    query_body = """{   "_source" : ["%s"],
                        "query" : {
                            "bool" : {
                                "must" : {
                                    "match_all" : {}
                                },
                                "filter" : {
                                    "geo_polygon" : {
                                        "%s" : {
                                            "points" : %s
                                        }
                                    }
                                }
                            }
                        }   
                    }""" % ('", "'.join(source_keys), field_query, poly_vtx)

    query_results = scroll_query(es_obj, index_query, query_body, 500)

    return query_results


def intersects_indexed_poly(es_obj,
                            index_query, field_query,
                            poly_index, poly_id, poly_path, source_keys):
    # geo_shape query
    # query for all geo_shapes that intersect an indexed polygon of geo_shape type
    # Inputs:   index_query - the index containing the shapes to be queried
    #           field_query - the field name to be queried
    #           poly_index - the index for the bounding polygon
    #           poly_id - the document id of the bounding polygon
    #           poly_path - the field name within the document of the bounding polygon
    #           source_keys - a list of strings with the field keys to return from the queried documents
    #               use wildcard * to get all keys
    #
    # Outputs:  query_results - a list of dictionaries returning the intersecting shapes

    query_body = """{   "_source" : ["%s"],
                        "query" : {
                            "bool" : {
                                "filter" : {
                                    "geo_shape" : {
                                        "%s" : {
                                            "indexed_shape" : {
                                                "index" : "%s",
                                                "id" : "%s",
                                                "path" : "%s"
                                            }
                                        } 
                                    }
                                }
                            }
                        }   
                    }""" % ('", "'.join(source_keys), field_query, poly_index, poly_id, poly_path)

    query_results = scroll_query(es_obj, index_query, query_body, 500)

    return query_results
