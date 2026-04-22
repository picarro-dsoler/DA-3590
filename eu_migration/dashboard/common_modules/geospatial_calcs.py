import ast
import time
import uuid

import geojson
import numpy as np
from geopy import distance
from osgeo import ogr
from shapely import wkt
from shapely.geometry import mapping, shape, LineString, MultiLineString, GeometryCollection
from shapely.wkt import loads

from dal import dynammo, sql
from dashboard.db_io.elastic_geo_queries import points_intersect_poly
from dashboard.db_io.elastic_queries import scroll_query
from helper.logger import Logger


def list_2_line(points_list):
    line = ogr.Geometry(ogr.wkbLineString)
    for pnt in points_list:
        line.AddPoint(pnt[0], pnt[1])

    return line


def wkt_2_geojson(wkt_str):
    geojson_dict = ast.literal_eval(geojson.dumps(mapping(wkt_str)))

    return geojson_dict


def p2p_distance(p1, p2):
    # instantiate the distance calculator object
    d = distance.distance

    p2p_m = d(p1, p2).meters

    return p2p_m


def map_factor_wgs84(ref_coord):
    # ref_coord is (lat,lon)
    # meters per degree
    map_fac_lon = 100.0 * p2p_distance(ref_coord,
                                       ref_coord + np.array([0.0, 0.01]))
    map_fac_lat = 100.0 * p2p_distance(ref_coord + np.array([0.01, 0.0]),
                                       ref_coord)

    # root mean square distance factor
    dist_fac = 1.0 / ((map_fac_lon ** 2.0 + map_fac_lat ** 2.0) / 2.0) ** 0.5

    return map_fac_lon, map_fac_lat, dist_fac


def segment_length_wgs84(segment_wkt):
    if type(segment_wkt) == dict:
        segment_wkt = shape(geojson.loads(geojson.dumps(segment_wkt))).wkt

    line_obj = wkt.loads(segment_wkt)
    line_coords = list(line_obj.coords)
    segment_len_m = 0.0

    # instantiate the distance calculator object
    d = distance.distance
    for seg_ind in range(1, len(line_coords)):
        point_2_point_m = d([line_coords[seg_ind - 1][1], line_coords[seg_ind - 1][0]],
                            [line_coords[seg_ind][1], line_coords[seg_ind][0]]).meters
        segment_len_m += point_2_point_m

    return segment_len_m


def segmentize_line_string(line_feat, line_meta, seg_dist_m, simplify_tol, main_buffer_m):
    # convert wkt or geojason lines to ogr object
    if isinstance(line_feat, geojson.geometry.LineString) or \
            isinstance(line_feat, geojson.geometry.MultiLineString) or isinstance(line_feat, dict):
        line_obj = ogr.CreateGeometryFromJson(str(line_feat))  # create ogr geometry
    elif isinstance(line_feat, str):
        line_obj = ogr.CreateGeometryFromWkt(line_feat)  # create ogr geometry
    else:
        raise TypeError('Line Feature input must be a LineString/MultiLineString WKT or geojson format!')

    # handle multi part geometries
    geometry_name = line_obj.GetGeometryName()
    if geometry_name == 'LINESTRING':
        line_feat_list = [line_obj]
    elif geometry_name == 'MULTILINESTRING':
        line_feat_list = [l for l in line_obj]
    else:
        raise TypeError('Input must be a LineString/MultiLineString type!')

    # segmentize the line geometries and flatten multipart lines
    # line_points = []
    line_shapes = []
    for line in line_feat_list:
        # clone the iterator to a new variable to modify
        working_line = ogr.CreateGeometryFromWkt(line.ExportToWkt())
        # calculate the local map factor and rms distance factor
        map_fac_lon, map_fac_lat, dist_fac = map_factor_wgs84(working_line.GetPoints()[0][::-1])
        # simplify the input geometry to remove any redundant points
        simple_line = ogr.CreateGeometryFromWkt(working_line.Simplify(simplify_tol * dist_fac).ExportToWkt())
        # segmentize the input geometry onto a pathwise regular interval
        simple_line.Segmentize(seg_dist_m * dist_fac)

        # # get the simplified coordinates
        # line_points.append({'line_points': simple_line.GetPoints(), 'dist_fac': dist_fac})
        line_points = simple_line.GetPoints()
        for ind in range(1, len(line_points)):
            line_shape = list_2_line(line_points[ind - 1:ind + 1])
            line_shape_wkt = line_shape.ExportToWkt()
            line_shapes.append({'line_shape': line_shape_wkt,
                                'dist_fac': dist_fac,
                                'line_len_m': segment_length_wgs84(line_shape_wkt),
                                'lon': line_points[ind - 1][0],
                                'lat': line_points[ind - 1][1],
                                'uuid': uuid.uuid1(),
                                'bdy_id': ''})

            # if an input dictionary is passed add the line_metadata onto the line_point
            if len(line_meta.keys()) != 0:
                for key in line_meta:
                    line_shapes[-1][key] = line_meta[key]

    return line_shapes


def analyze_segment_intersection(segment_obj):
    if isinstance(segment_obj, LineString):
        wkt_list = [segment_obj.wkt]
        segment_len_list = [segment_length_wgs84(wkt_list[-1])]
    elif isinstance(segment_obj, MultiLineString):
        wkt_list = []
        segment_len_list = []
        for iter_segment in segment_obj:
            wkt_list.append(iter_segment.wkt)
            segment_len_list.append(segment_length_wgs84(wkt_list[-1]))
    else:
        wkt_list = []
        segment_len_list = []

    return wkt_list, segment_len_list


def get_car_speed_analyzer_time(creds, analyzer_id, start_epoch, end_epoch):
    cursor_factory = sql.get_conn()

    # get the valid survey peaks for the specified customer
    with cursor_factory.get_cursor_pymssql() as cur:
        cur.execute("""
            SELECT POWER(POWER(Measurement.CarSpeedNorth, 2) + POWER(Measurement.CarSpeedEast, 2), 0.5) AS CarSpeed,
              POWER(POWER(Measurement.WindSpeedNorth, 2) + POWER(Measurement.WindSpeedEast, 2), 0.5) AS WindSpeed,
              Measurement.EpochTime, Measurement.GpsFit
            FROM Measurement
            WHERE Measurement.AnalyzerId = '%s'
            AND Measurement.EpochTime >= %s
            AND Measurement.EpochTime <= %s
            ORDER BY Measurement.EpochTime 
            """ % (analyzer_id, start_epoch, end_epoch))

        rows = cur.fetchall()

    car_speed = []
    wind_speed = []
    epoch_time = []
    gps_fit = []
    for row in rows:
        car_speed.append(row[0])
        wind_speed.append(row[1])
        epoch_time.append(row[2])
        gps_fit.append(row[3])

    return np.asarray(car_speed), np.asarray(wind_speed), np.asarray(epoch_time), np.asarray(gps_fit)


def get_car_speed_analyzer_time_dynamo(dynamocreds, analyzer_id, start_epoch, end_epoch):
    dynamo_dal = dynammo.get_conn()
    tst = time.time()
    records = dynamo_dal.load_measurement_data_forkeys(analyzer_id, start_epoch, end_epoch,
                                                       ['CarSpeedNorth',
                                                        'CarSpeedEast',
                                                        'WindSpeedNorth',
                                                        'WindSpeedEast',
                                                        'EpochTime',
                                                        'GpsFit'])

    elapsed = time.time() - tst
    Logger.log.info('Dynamo returned %d measurements in %f seconds' % (len(records), elapsed))

    car_speed = []
    wind_speed = []
    epoch_time = []
    gps_fit = []
    for record in records:
        try:
            # try to convert all values to float, if this fails no values are appended to any of the lists
            test = [float(record['CarSpeedNorth']), float(record['CarSpeedEast']),
                    float(record['WindSpeedNorth']), float(record['WindSpeedEast']),
                    float(record['EpochTime']),
                    int(record['GpsFit'])]
            car_speed.append(np.sqrt(float(record['CarSpeedNorth']) ** 2.0 + float(record['CarSpeedEast']) ** 2.0))
            wind_speed.append(np.sqrt(float(record['WindSpeedNorth']) ** 2.0 + float(record['WindSpeedEast']) ** 2.0))
            epoch_time.append(float(record['EpochTime']))
            gps_fit.append(int(record['GpsFit']))
        except TypeError:
            continue

    return np.asarray(car_speed), np.asarray(wind_speed), np.asarray(epoch_time), np.asarray(gps_fit)


def driving_data_4_segment(prodcreds, dynamocreds, segment_linestring_dict):
    car_speed, wind_speed, epoch_time, gps_fit = \
        get_car_speed_analyzer_time_dynamo(dynamocreds,
                                           segment_linestring_dict['analyzer_id'],
                                           segment_linestring_dict['epoch_time_start'],
                                           segment_linestring_dict['epoch_time_end'])
    if len(car_speed) == 0:
        car_speed, wind_speed, epoch_time, gps_fit = \
            get_car_speed_analyzer_time(prodcreds,
                                        segment_linestring_dict['analyzer_id'],
                                        segment_linestring_dict['epoch_time_start'],
                                        segment_linestring_dict['epoch_time_end'])

    # nominal_car_speed = np.median(car_speed[car_speed != None])
    # nominal_wind_speed = np.median(wind_speed[wind_speed != None])
    Logger.log.info( '%s, %f, %f' %(segment_linestring_dict['analyzer_id'],
          segment_linestring_dict['epoch_time_start'],
          segment_linestring_dict['epoch_time_end']))
    assert len(car_speed) == len(epoch_time)
    assert len(car_speed) == len(wind_speed)
    assert len(car_speed) == len(gps_fit)
    meas_qc_bool = (car_speed != None) * (gps_fit >= 1)
    car_speed = car_speed[meas_qc_bool]
    wind_speed = wind_speed[meas_qc_bool]
    epoch_time = epoch_time[meas_qc_bool]
    # gps_fit = gps_fit[meas_qc_bool]
    car_accel = np.gradient(car_speed, epoch_time)

    meas_qc_bool = (np.abs(car_accel) < 2.5)  # 2.5 m/s/s
    car_speed = car_speed[meas_qc_bool]
    wind_speed = wind_speed[meas_qc_bool]

    if len(car_speed) > 0:
        max_car_speed = car_speed[car_speed != None].max()
        nominal_car_speed = np.percentile(car_speed[car_speed != None], 90)
        if nominal_car_speed > max_car_speed:
            nominal_car_speed = max_car_speed.copy()
    else:
        nominal_car_speed = 0.0

    if len(wind_speed) > 0:
        max_wind_speed = wind_speed[wind_speed != None].max()
        nominal_wind_speed = np.percentile(wind_speed[wind_speed != None], 90)
        if nominal_wind_speed > max_wind_speed:
            nominal_wind_speed = max_wind_speed.copy()
    else:
        nominal_wind_speed = 0.0

    return nominal_car_speed, nominal_wind_speed


def ref_segment_coord(segment_linestring):
    mean_segment_coord = np.mean(np.asarray(segment_linestring.coords), axis=0)
    mean_coord = {'lon': mean_segment_coord[0], 'lat': mean_segment_coord[1]}
    return mean_coord


def segment_peak_assoc(es, segment_linestring, peak_index_name):
    # Associate eqpeaks with segments
    ref_coord = np.mean(np.asarray(segment_linestring.coords), axis=0)

    map_fac_lon = 100.0 * p2p_distance(ref_coord[::-1],
                                       ref_coord[::-1] + np.array([0.0, 0.01]))
    map_fac_lat = 100.0 * p2p_distance(ref_coord[::-1] + np.array([0.01, 0.0]),
                                       ref_coord[::-1])

    buffer_factor = 40.0 / (map_fac_lon ** 2.0 + map_fac_lat ** 2.0) ** 0.5
    buffered_segment = segment_linestring.buffer(buffer_factor, resolution=3)
    bdy_shp_envelope = [list(x) for x in buffered_segment.exterior.coords]

    docs_in_bdy = points_intersect_poly(es,
                                        peak_index_name, 'position',
                                        bdy_shp_envelope,
                                        '*')
    if len(docs_in_bdy) > 0:
        nominal_car_speed = np.percentile([x['maximum_car_speed'] for x in docs_in_bdy], 90)
        nominal_wind_speed = np.percentile([x['maximum_wind_speed'] for x in docs_in_bdy], 90)
    else:
        nominal_car_speed = -9999.0
        nominal_wind_speed = -9999.0

    return nominal_car_speed, nominal_wind_speed


def intersect_segment_boundary(es,
                               segment_dicts,
                               shape_index_name,
                               shape_id_field,
                               shape_bdy_field,
                               shape_main_len_field,
                               shape_n_svc_field,
                               prodcreds,
                               dynamocreds):
    source_list = [shape_id_field, shape_bdy_field]
    if shape_main_len_field is not None:
        source_list.append(shape_main_len_field)
    if shape_n_svc_field is not None:
        source_list.append(shape_n_svc_field)

    intersected_segment_dicts = []
    last_survey_id = ''
    nominal_car_speed = -9999.0
    nominal_wind_speed = -9999.0
    for idx, segment_dict in enumerate(segment_dicts):

        # for segment_dict in segment_dicts
        query_body = {
            "_source": source_list,
            "size": 10000,
            "query": {
                "bool": {
                    "filter": {
                        "geo_shape": {
                            shape_bdy_field: {
                                "shape": {
                                    "type": "linestring",
                                    "coordinates": segment_dict['segment_shape']['coordinates']
                                },
                                "relation": "intersects"
                            }
                        }
                    }
                }
            }
        }

        # convert the segment geojson to shapely linestring object
        segment_shape = shape(geojson.loads(geojson.dumps(segment_dict['segment_shape'])))
        # determine which boundaries that the line passes through
        result_json = scroll_query(es, shape_index_name, query_body, include_id=False)

        # # the segment car speed and wind speed are available from SQL Server >= 6.2.x, so this step is not needed
        # if segment_dict['survey_id'] != last_survey_id:
        #     # calulate segment car speed and wind speed from measurements
        #     nominal_car_speed, nominal_wind_speed = driving_data_4_segment(prodcreds,
        #                                                                    dynamocreds,
        #                                                                    segment_dict)
        #
        #     last_survey_id = segment_dict['survey_id']
        # # populate the segment dictionary
        # segment_dict['car_speed'] = nominal_car_speed
        # segment_dict['wind_speed'] = nominal_wind_speed

        segment_in_bdy = False
        for bdy in result_json:
            # for each boundary in calculate the intersection of the segment with that boundary
            # the parent segment will be broken up into child segments contained within each boundary
            intersected_segment = segment_shape.intersection(loads(bdy[shape_bdy_field]))

            intersected_segment_dict = {}
            # if the resultant is a LineString type object
            if type(intersected_segment) == LineString:
                # intersected_segment = MultiLineString([list(intersected_segment.coords)])
                coord_list = list(intersected_segment.coords)
                if len(coord_list) > 0:
                    intersected_segment = MultiLineString([coord_list])
                else:
                    continue

            elif type(intersected_segment) == GeometryCollection and len(intersected_segment) == 0:
                continue

            elif type(intersected_segment) != MultiLineString:
                Logger.log.info('Intersected (child) segment has and invalid shape object type. '
                      'Type is %s but it should be MultiLineString or empty GeometryCollection' %
                      type(intersected_segment))
                raise TypeError()

            # loop through the MultiLineString fragments and append them as individual objects
            for intersected_segment_fragment in intersected_segment:
                # create a copy of the data from the parent segment
                intersected_segment_dict = segment_dict.copy()
                # overwrite the shape from the parent segment with the child segment
                intersected_segment_dict['segment_shape'] = \
                    wkt_2_geojson(intersected_segment_fragment)
                # ast.literal_eval(geojson.dumps(mapping(intersected_segment_fragment)))

                intersected_segment_dict['position'] = ref_segment_coord(intersected_segment_fragment)

                # this is not used >= 6.2.x since every segment has a speed in SQL Server
                # # if there is a peak event on the segment, use the peak event speed as representative
                # customer_peak_index = '%s_eqpeaks' % shape_index_name.split('_')[0]
                # car_speed_peak, wind_speed_peak = segment_peak_assoc(es,
                #                                                      intersected_segment_fragment,
                #                                                      customer_peak_index)
                # if car_speed_peak >= 0.0 and wind_speed_peak >= 0.0:
                #     intersected_segment_dict['car_speed'] = car_speed_peak
                #     intersected_segment_dict['wind_speed'] = wind_speed_peak

                # zero unphysical values for favorable filtering on visualization
                if intersected_segment_dict['car_speed'] is None:
                    intersected_segment_dict['car_speed'] = 0.0
                elif intersected_segment_dict['car_speed'] > 90.0:
                    intersected_segment_dict['car_speed'] = 0.0

                if intersected_segment_dict['wind_speed'] is None:
                    intersected_segment_dict['wind_speed'] = 0.0
                elif intersected_segment_dict['wind_speed'] > 90.0:
                    intersected_segment_dict['wind_speed'] = 0.0

                # assign the boundary id to the child segment data
                intersected_segment_dict['bdy_id'] = bdy[shape_id_field]
                if shape_main_len_field is not None:
                    intersected_segment_dict['main_length'] = bdy[shape_main_len_field]
                    intersected_segment_dict['main_length_unit'] = shape_main_len_field
                if shape_n_svc_field is not None:
                    intersected_segment_dict['num_svc'] = bdy[shape_n_svc_field]
                # calculate the length of the segment fragment
                intersected_segment_dict['segment_len_m'] = segment_length_wgs84(intersected_segment_fragment.wkt)

                intersected_segment_dicts.append(intersected_segment_dict)

    return intersected_segment_dicts
