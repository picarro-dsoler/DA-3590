import ast
import math
import numpy as np
from shapely.wkt import loads
from shapely.geometry import mapping
import geojson

from dashboard.common_modules.time_calcs import time_string_regularizer, epoch_2_gmtstring


def set_none_nan(value):
    # check and fix None and nan values because they are incompatible with elasticsearch

    # set the missing data value
    missing_value = -9999.0

    # fix none values
    if value is None:
        return missing_value
    # fix nan values
    elif type(value) == float:
        if math.isnan(value):
            return missing_value
        else:
            return value
    elif type(value) == np.float64:
        if np.isnan(value):
            return missing_value
        else:
            return value
    # if the value is valid return the input unchanged
    else:
        return value


def eqpeak_2_dict(eqpeak):
    # covert the eqpeak object to a dictionary for import to elastic search and programmatic reference

    eqpeak_dict = dict()
    eqpeak_dict['mean_wind_dir'] = set_none_nan(eqpeak.lisa_bearing)
    # eqpeak_dict['lisa_poly'] = eqpeak.lisa_wedge.wkt
    eqpeak_dict['lisa_poly'] = eqpeak.Lisa
    eqpeak_dict['analyzer_status'] = set_none_nan(eqpeak.analyzer_status)
    eqpeak_dict['analyzer_id'] = eqpeak.AnalyzerId.lower()
    eqpeak_dict['uuid_str'] = eqpeak.uuid_str.lower()
    eqpeak_dict['wind_dir_std'] = set_none_nan(eqpeak.WindDirectionStdDev)
    eqpeak_dict['ethane_conc_std'] = set_none_nan(eqpeak.EthaneConcentrationSdev)
    eqpeak_dict['car_bearing'] = eqpeak.car_bearing
    eqpeak_dict['wind_speed'] = set_none_nan(eqpeak.wind_speed)
    # eqpeak_dict['beta'] = eqpeak.beta
    eqpeak_dict['disposition'] = set_none_nan(eqpeak.verdict)
    eqpeak_dict['car_speed_east'] = set_none_nan(eqpeak.CarSpeedEast)
    eqpeak_dict['measurement_id'] = set_none_nan(eqpeak.MeasurementType)
    eqpeak_dict['wind_speed_east'] = set_none_nan(eqpeak.WindSpeedEast)
    eqpeak_dict['bkg_ch4'] = set_none_nan(eqpeak.CH4 - eqpeak.Amplitude)
    eqpeak_dict['epoch_plume_end'] = eqpeak.PlumeEpochEnd
    eqpeak_dict['emission_rate_uncertainty'] = eqpeak.PlumeEmissionRateUncertainty
    eqpeak_dict['survey_id'] = eqpeak.SurveyId.lower()
    eqpeak_dict['max_ch4'] = set_none_nan(eqpeak.CH4)
    eqpeak_dict['wind_speed_north'] = set_none_nan(eqpeak.WindSpeedNorth)
    eqpeak_dict['epoch_time'] = eqpeak.EpochTime
    eqpeak_dict['car_speed_north'] = set_none_nan(eqpeak.CarSpeedNorth)
    eqpeak_dict['emission_rate'] = eqpeak.PlumeEmissionRate
    eqpeak_dict['ethane_ratio'] = set_none_nan(eqpeak.EthaneRatio)
    eqpeak_dict['ethane_ratio_std'] = set_none_nan(eqpeak.EthaneRatioSdev)
    # eqpeak_dict['Id'] = set_none_nan(eqpeak.Id.lower())
    eqpeak_dict['Id'] = set_none_nan(None)
    eqpeak_dict['PeakId'] = set_none_nan(eqpeak.Id.lower())
    eqpeak_dict['qa_alarm'] = set_none_nan(eqpeak.qa_alarm)
    eqpeak_dict['qa_flag'] = set_none_nan(eqpeak.QAFlag)
    eqpeak_dict['maximum_car_speed'] = set_none_nan(eqpeak.PlumeCarSpeedMaximum)
    eqpeak_dict['car_speed'] = set_none_nan(eqpeak.car_speed)
    eqpeak_dict['ch4'] = eqpeak.CH4
    eqpeak_dict['amplitude'] = eqpeak.Amplitude
    eqpeak_dict['maximum_wind_speed'] = set_none_nan(eqpeak.PlumeWindSpeedMaximum)
    eqpeak_dict['median_wind_dir'] = set_none_nan(eqpeak.lisa_bearing)
    eqpeak_dict['mean_wind_speed'] = set_none_nan(eqpeak.PlumeWindSpeedMean)
    eqpeak_dict['epoch_plume_start'] = eqpeak.PlumeEpochStart
    eqpeak_dict['lat'] = eqpeak.GpsLatitude
    eqpeak_dict['lon'] = eqpeak.GpsLongitude

    # define the gmt time string
    eqpeak_dict['gmt_string'] = epoch_2_gmtstring(eqpeak.EpochTime)

    return eqpeak_dict


def eqsource_2_dict(eqsource):
    # covert the eqsource object to a dictionary for import to elastic search and programmatic reference

    eqsource_dict = dict()
    eqsource_dict['avg_epoch'] = eqsource.EpochTime
    eqsource_dict['emission_rate'] = eqsource.EmissionRate
    eqsource_dict['amplitude'] = eqsource.MaxAmplitude
    eqsource_dict['car_speed'] = eqsource.RepresentativePeak.car_speed
    eqsource_dict['car_speed_east'] = eqsource.RepresentativePeak.CarSpeedEast
    eqsource_dict['car_speed_north'] = eqsource.RepresentativePeak.CarSpeedNorth
    eqsource_dict['confidence'] = set_none_nan(eqsource.ClassificationConfidence)
    eqsource_dict['detection_probability'] = eqsource.DetectionProbability
    eqsource_dict['disposition'] = eqsource.Disposition
    eqsource_dict['emission_rank'] = eqsource.PeakNumber
    eqsource_dict['emission_rate_amean'] = eqsource.EmissionRateAMean
    eqsource_dict['emission_rate_astd'] = eqsource.EmissionRateAStd
    eqsource_dict['emission_rate_gmean'] = eqsource.EmissionRateGMean
    eqsource_dict['emission_rate_gstd'] = eqsource.EmissionRateGStd
    eqsource_dict['ethane_ratio'] = set_none_nan(eqsource.EthaneRatio)
    eqsource_dict['ethane_ratio_uncertainty'] = set_none_nan(eqsource.EthaneRatioUncertainty)
    eqsource_dict['isfiltered'] = set_none_nan(int(eqsource.IsFiltered))
    eqsource_dict['lat'] = eqsource.GpsLatitude
    eqsource_dict['lisa_poly'] = eqsource.Lisa
    eqsource_dict['lon'] = eqsource.GpsLongitude
    eqsource_dict['num_passes'] = eqsource.NumberOfPasses
    eqsource_dict['num_peaks'] = eqsource.NumberOfPeaks
    eqsource_dict['uuid_str'] = eqsource.uuid_str.lower()
    eqsource_dict['wind_dir'] = eqsource.RepresentativePeak.lisa_bearing
    eqsource_dict['wind_speed'] = eqsource.RepresentativePeak.wind_speed
    eqsource_dict['wind_speed_east'] = eqsource.RepresentativePeak.WindSpeedEast
    eqsource_dict['wind_speed_north'] = eqsource.RepresentativePeak.WindSpeedNorth

    # create a wkt version of the source coordinate
    eqsource_dict['coord'] = 'POINT (%s %s)' % (eqsource_dict['lon'], eqsource_dict['lat'])

    # define the gmt time string
    eqsource_dict['gmt_string_event'] = epoch_2_gmtstring(eqsource.EpochTime)

    return eqsource_dict


def analytics_source_2_dict(analytics_source):
    # covert the analytics_source object to a dictionary for import to elastic search and programmatic reference
    analytics_source_dict = dict()
    analytics_source_dict['amplitude'] = analytics_source.amplitude
    analytics_source_dict['car_speed'] = analytics_source.car_speed
    analytics_source_dict['car_speed_east'] = analytics_source.car_speed_east
    analytics_source_dict['car_speed_north'] = analytics_source.car_speed_north
    analytics_source_dict['confidence'] = set_none_nan(analytics_source.confidence)
    analytics_source_dict['emission_rank'] = analytics_source.emission_rank
    analytics_source_dict['emission_rate'] = analytics_source.emission_rate
    analytics_source_dict['emission_rate_amean'] = analytics_source.emission_rate_amean
    analytics_source_dict['emission_rate_astd'] = analytics_source.emission_rate_astd
    analytics_source_dict['emission_rate_gmean'] = analytics_source.emission_rate_gmean
    analytics_source_dict['emission_rate_gstd'] = analytics_source.emission_rate_gstd
    analytics_source_dict['disposition'] = analytics_source.disposition
    analytics_source_dict['detection_probability'] = analytics_source.detection_probability
    analytics_source_dict['epoch_time'] = analytics_source.epoch_time
    analytics_source_dict['ethane_ratio'] = set_none_nan(analytics_source.ethane_ratio)
    analytics_source_dict['ethane_ratio_uncertainty'] = set_none_nan(analytics_source.ethane_ratio_uncertainty)
    analytics_source_dict['isfiltered'] = set_none_nan(analytics_source.isfiltered)
    analytics_source_dict['lat'] = analytics_source.latitude
    analytics_source_dict['lisa_poly'] = analytics_source.lisa_poly
    analytics_source_dict['lon'] = analytics_source.longitude
    analytics_source_dict['num_passes'] = analytics_source.n_passes
    analytics_source_dict['num_peaks'] = analytics_source.n_peaks
    analytics_source_dict['priority_score'] = analytics_source.priority_score
    analytics_source_dict['wind_dir'] = analytics_source.wind_dir
    analytics_source_dict['wind_speed'] = analytics_source.wind_speed
    analytics_source_dict['wind_speed_east'] = analytics_source.wind_speed_east
    analytics_source_dict['wind_speed_north'] = analytics_source.wind_speed_north

    # create a wkt version of the source coordinate
    analytics_source_dict['coord'] = 'POINT (%s %s)' % (analytics_source_dict['lon'], analytics_source_dict['lat'])

    # define the gmt time string
    analytics_source_dict['gmt_string_event'] = epoch_2_gmtstring(analytics_source.epoch_time)

    return analytics_source_dict


def segment_2_dict(sql_segment_list):
    # covert the segment data from prod sql to a dictionary format
    segment_dict = {}

    key_list = ['epoch_time_start', 'epoch_time_end', 'gmt_string_start', 'gmt_string_end', 'survey_id', 'description',
                'analyzer_id', 'surveyor_unit_id', 'customer_name', 'survey_tag', 'first_name', 'last_name',
                'segment_order', 'segment_mode', 'segment_shape',
                'user_name', 'unit_name', 'serial_num', 'car_speed', 'wind_speed']

    for list_ind, key in enumerate(key_list):
        segment_dict[key] = sql_segment_list[list_ind]

    # convert the segment_shape wkt to geojson for maps compatibility
    segment_dict['segment_shape'] = ast.literal_eval(geojson.dumps(mapping(loads(segment_dict['segment_shape']))))

    # format the time string
    segment_dict['gmt_string_start'] = time_string_regularizer(
        segment_dict['gmt_string_start'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-4])
    segment_dict['gmt_string_end'] = time_string_regularizer(
        segment_dict['gmt_string_end'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-4])

    return segment_dict


def peak_2_dict(sql_peak_list):
    # covert the segment data from prod sql to a dictionary format
    peak_dict = {}

    key_list = ['gmt_string_start', 'gmt_string_end', 'description', 'analyzer_id', 'surveyor_unit_id', 'survey_tag',
                'customer_name', 'first_name', 'last_name', 'epoch_time', 'amplitude', 'ch4', 'position_wkt',
                'lisa_wkt', 'lisa_opening_angle', 'lisa_bearing', 'wind_direction_std_dev', 'wind_speed_north',
                'wind_speed_east', 'sigma', 'lat', 'lon', 'survey_id', 'ethane_ratio', 'uuid_str']

    for list_ind, key in enumerate(key_list):
        peak_dict[key] = sql_peak_list[list_ind]

    # format the time string
    # peak_dict['gmt_string_start'] = peak_dict['gmt_string_start'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-4] + 'Z'
    # peak_dict['gmt_string_end'] = peak_dict['gmt_string_end'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-4] + 'Z'
    peak_dict['gmt_string_start'] = time_string_regularizer(
        peak_dict['gmt_string_start'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-4])
    peak_dict['gmt_string_end'] = time_string_regularizer(
        peak_dict['gmt_string_end'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-4])
    peak_dict['gmt_string_event'] = epoch_2_gmtstring(peak_dict['epoch_time'])

    return peak_dict


def report_eqsource_2_dict(sql_eq_source_list):
    # covert the segment data from prod sql to a dictionary format
    eq_source_dict = {}

    # ReportId, EmissionRank, EmissionRate, Latitude, Longitude, Address, NumberOfPeaks, NumberOfPasses, ReportTitle, AvgEpoch
    key_list = ['report_id', 'emission_rank', 'emission_rate', 'lat', 'lon', 'address', 'num_peaks',
                'num_passes', 'report_title', 'avg_epoch', 'date_started_string']

    for list_ind, key in enumerate(key_list):
        eq_source_dict[key] = sql_eq_source_list[list_ind]

    # round the emission rate value
    eq_source_dict['emission_rate'] = round(eq_source_dict['emission_rate'], 1)
    # create a wkt version of the source coordinate
    eq_source_dict['coord'] = 'POINT (%s %s)' % (eq_source_dict['lon'], eq_source_dict['lat'])

    # format the time string
    eq_source_dict['gmt_string_event'] = epoch_2_gmtstring(eq_source_dict['avg_epoch'])

    # convert the report started date to epoch
    eq_source_dict['date_started_epoch'] = eq_source_dict['date_started_string'].timestamp()

    return eq_source_dict
