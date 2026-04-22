

def create_apreti_emissions_index(es_obj, index_name, n_shards, n_rep):
    # format the index, settings and mappings for data
    index_body = {
        "settings": {
            "index": {
                "number_of_shards": n_shards,
                "number_of_replicas": n_rep}
        },
        "mappings": {
            "properties": {
                "BoundaryName": {"type": "keyword"},
                "Region": {"type": "keyword"},
                "PCubedReportName": {"type": "keyword"},
                "PCubedReportGuid": {"type": "keyword"},
                "LastSurveyDate": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "LastSurveyDateStr": {"type": "text"},
                "PCubedReportTitle": {"type": "keyword"},
                "PCubedReportDate": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
                "PCubedReportDateStr": {"type": "keyword"},
                "AssetCoverageFrac": {"type": "float"},
                "PipelineMeters": {"type": "float"},
                "BoxId": {"type": "keyword"},
                "SurveyorUnitName": {"type": "keyword"},
                "PeakEpoch": {"type": "float"},
                "Amplitude": {"type": "float"},
                "CH4": {"type": "float"},
                "AggregatedEthaneRatio": {"type": "float"},
                "AggregatedDisposition": {"type": "keyword"},
                "AggregatedClassificationConfide35abcdfrs]nce": {"type": "float"},
                "PeakNumber": {"type": "keyword"},
                "EthaneRatioSdev": {"type": "float"},
                "Sigma": {"type": "float"},
                "PriorityScore": {"type": "float"},
                "type1coverage": {"type": "float"},
                "contract": {"type": "keyword"},
                "assignment": {"type": "keyword"},
                "PeakCoord": {"type": "geo_shape"},
                "PeakCoordLatLon": {"type": "text"},
                "EmissionRate": {"type": "float"},
                "EmissionRateUpperBound": {"type": "float"},
                "EmissionRateLowerBound": {"type": "float"},
                "EmissionRank":  {"type": "integer"},
            }
        }}

    # create the raw data index in the db
    res = es_obj.indices.create(index=index_name, body=index_body, ignore=400)

    return res
