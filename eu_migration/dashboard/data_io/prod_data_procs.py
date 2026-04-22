import matplotlib.pyplot as pl

import os
import time
import traceback
from uuid import UUID, uuid4
from collections import namedtuple

import _mssql
import pymssql

import numpy as np
import pandas as pd
from shapely import wkt

from dal import sql, dynammo
from dashboard.common_modules.array_calcs import most_frequent
from dashboard.db_io.prod_sql_queries import get_survey_info, get_survey_segments, \
    get_peaks_by_analyzer_time, get_peaks_by_analyzer_time_archive, \
    get_survey_analyzer_time

from surveyworker import utilities
from clusterworker import surveyordal
from clusterworker.clusterjobprocessor import ClusterJobProcessor
from clusterworker.filedal import FileDal
from workercommon.dal import reportdal
from pseq.EmissionQuantification import EmissionQuantification
from psanalytics.dto.Peak import Peak

from helper.logger import Logger


params_type_alias_map = {
    "amplitude_threshold": (float, "minimum_reportable_amplitude"),

    # Measurement validation parameters
    "width_max": (float, "peak_width_max"),
    "width_min": (float, "peak_width_min"),
    "variation_max": float,
    "car_speed_max": (float, "peak_car_speed_max"),
    "car_speed_min": (float, "peak_car_speed_min"),
    "car_wind_angle_max": (float, "peak_car_wind_angle_max"),
    "car_wind_angle_min": (float, "peak_car_wind_angle_min"),
    "shape_correlation_max": float,
    "shape_correlation_min": float,
    "peak_acceleration_min": float,
    "peak_acceleration_max": float,

    # TimeSpaceCluster parameters
    "cluster_id": (int, "clustering"),
    "dbscan_distance_scale": (float, "spatial_scale"),
    "dbscan_min_cluster_size": int,
    "wind_aware_threshold": (float, "threshold"),
    "exclusion_radius": float,
    "space_time_cluster": bool,  # ???
    "space_time_cluster_scale_hours": float,

    # EthanePartition parameters
    "do_ethane_partition": bool,
    "max_num_candidates": int,
    "source_range": float,
    "prior_prob_factor_per_source": float,

    # Calculation parameters, passed to EmissionSource DTO
    "ethane_regularization": (float, "reg"),
    "max_confidence": float,
    "ng_lower": float,
    "ng_upper": float,
    "nng_lower": float,
    "nng_upper": float,
    "nng_prior": float,
    "prior_scale": (float, "measurement_sigma"),
    "prior_scale_weight": (float, "measurement_scale_weight"),
    "run_geocode": bool,
    "thresh_confidence": float,

    # Ranking parameters
    "priority_score_filter_threshold": (
        float, "reportable_risk_priority_score_ps_threshold"),
    "priority_score_first": (float, "risk_priority_score_ps_threshold_for_tier_1"),
    "priority_score_second": (float, "risk_priority_score_ps_threshold_for_tier_2"),
    "priority_score_third": (float, "risk_priority_score_ps_threshold_for_tier_3"),
}


def get_survey_job_payload(survey_id):
    report_dal = reportdal.ReportDal(sql.get_conn())

    survey_info = get_survey_info(None, survey_id)

    survey_params = report_dal.get_customer_calibration_config(survey_id)

    job = {
        "id": str(uuid4()),
        "type_id": report_dal.survey_job_type_id,
        "sequence": 0,

        "peaks_flag": True,
        "fov_flag": False,
        "capture_flag": False,
        "survey_id": str(survey_id),
        "calibration_config": survey_params,
        "delete_results": False,

        "name": "Survey",
        "mode": "batch",
        "stability_class": survey_info[-1]
    }
    return job


def get_measurements(dynamo_dal, dal, survey_details):
    start_time = time.time()
    # measurements = dynamo_dal.get_survey_record(survey_details)
    measurements = dynamo_dal.get_measurements(survey_details.analyzer_id, survey_details.start, survey_details.end)
    if len(measurements) == 0:
        Logger.log.info('Dynamo returned 0 measurements data, trying SQL DB')
        measurements = dal.get_survey_record(survey_details)
        Logger.log.info('SQL DB returned %d measurements' % len(measurements))
        if len(measurements) == 0:
            raise IndexError('No data found in Dynamo or SQL Server for Survey ID: %s' % survey_details.survey_id)
    else:
        elapsed = time.time() - start_time
        Logger.log.info('Dynamo returned %d measurements in %f seconds' % (len(measurements['CH4']), elapsed))

    return measurements


def populate_peak_uuids(eqpeaks, analyzer_ids, survey_ids):
    assert len(eqpeaks) == len(analyzer_ids) == len(survey_ids)

    eqpeaks_ids = []
    for peak_i, eqpeak in enumerate(eqpeaks):
        eqpeaks_id = eqpeak
        eqpeaks_id.uuid_str = '%s_%.3f' % (analyzer_ids[peak_i].lower(), eqpeak.EpochTime)
        eqpeaks_id.AnalyzerId = analyzer_ids[peak_i]
        eqpeaks_id.SurveyId = survey_ids[peak_i]

        # fix the lisa bearing to be on the correct compass heading
        if eqpeaks_id.lisa_bearing < 0.0:
            eqpeaks_id.lisa_bearing += 360.0
        if eqpeaks_id.lisa_bearing >= 360.0:
            eqpeaks_id.lisa_bearing -= 360.0

        eqpeaks_ids.append(eqpeaks_id)

    assert len(eqpeaks_ids) == len(eqpeaks)

    return eqpeaks_ids


def populate_source_uuids(emission_sources):
    eqsources_ids = []
    for emission_source in emission_sources:
        eqsource_id = emission_source
        eqsource_id.uuid_str = '; '.join([x.uuid_str for x in emission_source.peaks])
        eqsource_id.AnalyzerId = most_frequent([x.AnalyzerId for x in emission_source.peaks])
        eqsource_id.SurveyId = most_frequent([x.SurveyId for x in emission_source.peaks])
        eqsource_id.EpochTime = np.median([x.EpochTime for x in emission_source.peaks])

        # add the representative peak data to the emission source DTO
        eqsource_id.RepresentativePeak = [x for x in emission_source.peaks
                                          if x.Amplitude == emission_source.MaxAmplitude][0]

        eqsources_ids.append(eqsource_id)

    assert len(eqsources_ids) == len(emission_sources)

    return eqsources_ids


def process_eq_peaks(survey_id, creds, sql_addr, dynamocreds):
    eqpeaks = []
    survey_segments = []

    # instantiate a new connection every time to avoid db downtime
    dal = surveyordal.SurveyorDal(sql.get_conn())

    # get the data needed to process EQ results, returns a named tuple
    survey_details = dal.get_survey_details(survey_id)

    # try to grab the eqpeaks if they are already available in the database
    eqpeaks = dal.get_peaks([survey_id])
    # get the survey segments
    survey_segments = get_survey_segments(creds, [survey_id])

    # if peaks were already available in the db for the survey, use those
    if len(eqpeaks) > 0:
        survey_segments = [{'WKT': x.wkt} for x in survey_segments]  # typecast survey segment data to dictionary
        Logger.log.info('Found %d eq peaks in SQL Server for survey %s' % (len(eqpeaks), survey_id))

    # else generate the peaks from the measurements data
    else:

        job_data = get_survey_job_payload(survey_id)
        ahct = dal.get_analyzer_hardware_capability_type(survey_details.analyzer_id)
        job_data.update(ahct)

        measurements = get_measurements(dynammo.get_conn(), dal, survey_details)

        if len(measurements) > 0:
            pipeline_utils = utilities.Utilities()
            pipeline_results = pipeline_utils.get_pipeline_results(measurements=measurements, job_config=job_data)

            eqpeaks = pipeline_results['peaks']
            survey_segments = pipeline_results['breadcrumbs']

    # populate the composite uuid string from analyzer id and epoch time miliseconds
    eqpeaks = populate_peak_uuids(eqpeaks,
                                  len(eqpeaks) * [survey_details.analyzer_id],
                                  len(eqpeaks) * [survey_id])

    return eqpeaks, survey_segments


def process_eq_peaks_analyzer_time(analyzer_ids, start_epochs, end_epochs, creds, sql_addr, dynamocreds):
    eqpeaks = []
    # instantiate a new connection every time to avoid db downtime
    dal = surveyordal.SurveyorDal(sql.get_conn())

    # peaks = dal.get_peaks_by_analyzer_time(analyzer_id, start_epoch, end_epoch)
    surveys_wo_peaks = []
    peaks_list = []
    for analyzer_id, start_epoch, end_epoch in zip(analyzer_ids, start_epochs, end_epochs):
        survey_id_4_peak = get_survey_analyzer_time(None, analyzer_id, start_epoch)[0]
        peak_objs = []
        # peaks_list.append(dal.get_peaks_by_analyzer_time(analyzer_id, start_epoch, end_epoch))
        peak_rows = get_peaks_by_analyzer_time(analyzer_id, start_epoch, end_epoch)
        if not peak_rows:

            peak_rows = get_peaks_by_analyzer_time_archive(analyzer_id, start_epoch, end_epoch)

            if not peak_rows:

                Logger.log.warn("No Peak data found for AnalyzerId: %s between %.4f and %.4f" %
                                (analyzer_id, start_epoch, end_epoch))

                surveys_wo_peaks.append(survey_id_4_peak)
                continue

        # else:

        for row in peak_rows:
            peak_objs.append(Peak(**row))

        peak_objs = populate_peak_uuids(peak_objs,
                                        len(peak_objs) * [analyzer_id],
                                        len(peak_objs) * [survey_id_4_peak])
        peaks_list.append(peak_objs)
        # peaks_list.append(pd.DataFrame(peak_rows))

    # if some peaks were not returned in the peak table, calculate the peaks from the corresponding survey data
    if len(surveys_wo_peaks) > 0:
        unq_survey_ids = np.unique(surveys_wo_peaks)

        for survey_id in unq_survey_ids:
            peak_objs, survey_segments = process_eq_peaks(survey_id, (None, None), None, None)
            peaks_list.append(peak_objs)

    eqpeaks = [item for sublist in peaks_list for item in sublist]
    peak_uuids = [x.uuid_str for x in eqpeaks]
    _, unq_peak_inds = np.unique(peak_uuids, return_index=True)
    eqpeaks = [eqpeaks[x] for x in unq_peak_inds]

    assert len(eqpeaks) == len(np.unique([x.uuid_str for x in eqpeaks]))

    return eqpeaks


def eq_peaks_from_survey_ids(creds, dynamocreds, survey_ids):
    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = sql.get_host()

    eqpeaks = []
    survey_segments = []

    for i, row in enumerate(survey_ids):

        if i % 100 == 0:
            Logger.log.info("Processing {} of {} surveys".format(i + 1, len(survey_ids)))
            Logger.log.info(time.time())

        ctr = 0
        invalid_conn_flag = True
        while invalid_conn_flag:
            if ctr == n_retries:
                raise IndexError('Max number of retries reached %d. Could not connect to SQL Server at %s' %
                                 (n_retries, sql_addr))
            ctr += 1
            try:
                # get the eq peaks for the survey
                eqpeaks_list, survey_segments_list = process_eq_peaks(row, creds, sql_addr, dynamocreds)

                eqpeaks.append(eqpeaks_list)
                survey_segments.append(survey_segments_list)

                invalid_conn_flag = False
            except (_mssql.MSSQLDatabaseException, pymssql.OperationalError, TypeError):
                Logger.log.exception(traceback.format_exc())
                Logger.log.error('Connection to SQL Server at %s failed. Retry %d of %d, next retry in %.1f seconds' %
                      (sql_addr, ctr, n_retries, retry_delay))
                time.sleep(retry_delay)
                continue

    return eqpeaks, survey_segments


def eq_peaks_from_analyzer_time(creds, dynamocreds, analyzer_ids, start_epochs, end_epochs):
    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = sql.get_host()
    eqpeaks = []

    ctr = 0
    invalid_conn_flag = True
    while invalid_conn_flag:
        if ctr == n_retries:
            raise IndexError('Max number of retries reached %d. Could not connect to SQL Server at %s' %
                             (n_retries, sql_addr))
        ctr += 1
        try:
            # get the eq peaks for the analyzer id and time range
            eqpeaks = process_eq_peaks_analyzer_time(analyzer_ids, start_epochs, end_epochs, creds, sql_addr,
                                                     dynamocreds)

            invalid_conn_flag = False
        except (_mssql.MSSQLDatabaseException, pymssql.OperationalError, TypeError):
            Logger.log.exception(traceback.format_exc())
            Logger.log.error('Connection to SQL Server at %s failed. Retry %d of %d, next retry in %.1f seconds' %
                  (sql_addr, ctr, n_retries, retry_delay))
            time.sleep(retry_delay)
            continue
    return eqpeaks


def process_eq_sources(peaks, segments):

    job_data_dict = {
        'report_id': str(uuid4()),
        'user_id': str(uuid4()),
        'name': 'Cluster',
        'id': str(uuid4()),
        'type_id': '00000000-0000-0000-0028-000000000000',
        'sequence': '1',
        'parameters': {
            'clustering': '1',
            'exclusion_radius': '20',
            'spatial_scale': '25',
            'threshold': '0.5',
            'minimum_reportable_amplitude': '0.035',
            'peak_shape_correlation_min': '0',
            'peak_shape_correlation_max': '1',
            'peak_width_min': '5',
            'peak_width_max': '150',
            'peak_car_speed_min': '0.5',
            'peak_car_speed_max': '40',
            'peak_car_wind_angle_min': '0',
            'peak_car_wind_angle_max': '180',
            'peak_acceleration_min': '-20',
            'peak_acceleration_max': '10',
            'ethane_to_methane_ratio_min': '0.02',
            'ethane_to_methane_ratio_max': '0.1',
            'locator_radius': '45.72',
            'leak_rate_report': '0.5',
            'edge_probability': '90',
            'segment_buffer': '2',
            'risk_priority_score_ps_threshold_for_tier_1': '1.5',
            'risk_priority_score_ps_threshold_for_tier_2': '0.5',
            'risk_priority_score_ps_threshold_for_tier_3': '0.1',
            'reportable_risk_priority_score_ps_threshold': '0.0',
            'measurement_sigma': '0.8',
            'measurement_scale_weight': '5',
            'standard_mode_amplitude': '0.035',
            'rapid_response_mode_amplitude': '5.0',
            'risk_ranking_analytics_mode_amplitude': '0.1',
            'emissions_mode_amplitude': '0.1',
            'facility_eq_mode_amplitude': '0.1',
            'methane_ethane_sdev_ratio': '0.2',
            'methane_ethylene_sdev_ratio': '0.09',
            'minimum_ethane_ratio_sdev': '0.001',
            'minimum_ethylene_ratio_sdev': '0.005',
            'nng_lower': '0.00',
            'nng_upper': '0.001',
            'ng_lower': '0.02',
            'ng_upper': '0.3',
            'nng_prior': '0.2694',
            'thresh_confidence': '0.9',
            'reg': '0.05',
            'run_geocode': False,
            've_ethylene_sdev_factor': '2.0',
            've_ethylene_lower': '0.15',
            've_ethane_sdev_factor': '0.0',
            've_ethane_upper': '10.0',
            'minimum_emissions': '0.0',
            'leak_rates_driving_session': '0.1;0.5;1;10',
            'dynamodb_enabled': True,
            'variation_max': '150',
            'max_distance_range': '999999',
            'max_seconds': '999999'
        },
        'read_only': True
    }
    job_data = namedtuple("JobData", list(job_data_dict.keys()))(*list(job_data_dict.values()))

    report_amplitude_kwargs = {
        "analytics_mode_amplitude": float(job_data_dict['parameters']['risk_ranking_analytics_mode_amplitude']),
        "emissions_mode_amplitude": float(job_data_dict['parameters']['emissions_mode_amplitude']),
        "facility_eq_mode_amplitude": float(job_data_dict['parameters']['facility_eq_mode_amplitude']),
        "rapid_response_mode_amplitude": float(job_data_dict['parameters']['rapid_response_mode_amplitude']),
        "standard_mode_amplitude": float(job_data_dict['parameters']['standard_mode_amplitude'])
    }

    report_type_id = '00000000-0000-0000-0001-000000000000'  # compliance report type
    report_mode_type_id = 'B310238A-A5AE-4E94-927B-F0F165E24522'  # Standard survey mode type
    report_boundary_wkt = 'POLYGON((-179.5686105546708 84.01821956893342,179.02513944532916 84.01821956893342,179.02513944532916 -72.70563665697703,-179.5686105546708 -72.70563665697703,-179.5686105546708 84.01821956893342))'
    report_boundary_shape = wkt.loads(report_boundary_wkt)

    if peaks and segments:

        surveyor_dal = surveyordal.SurveyorDal(sql.get_conn())
        file_dal = FileDal(os.getcwd())

        # instantiate the cluster job processor
        processor = ClusterJobProcessor(surveyor_dal,
                                        file_dal,
                                        '')

        # get the report parameters
        report_params = processor.parse_param_datatype(params_type_alias_map, job_data.parameters)

        priority_score_filter_threshold = report_params.pop("priority_score_filter_threshold")
        percentile_ranking_bins = [
            report_params.pop(key) for key in (
                "priority_score_first", "priority_score_second", "priority_score_third")]

        cluster_id = report_params.pop("cluster_id")
        report_params["cluster_method"] = surveyor_dal.get_cluster_method(cluster_id)

        # report_mode_amp_threshold = processor.get_report_mode_amplitude_threshold(
        #     report_params, report_mode_type_id, report_type_id)

        report_mode_amp_threshold = processor.get_report_mode_amplitude_threshold(
            report_type_id,
            report_mode_type_id,
            **report_amplitude_kwargs)

        # instantiate the EQ utility for generating peaks
        emission_utility = EmissionQuantification(
            peaks=peaks,
            segments=segments,
            report_id=job_data.report_id,
            google_geocode_api_key='',
            **report_params
        )
        # calculate emission sources
        emission_sources, invalid_peaks = emission_utility.calculate_sources()
        emission_sources = emission_utility.process_emission_sources(emission_sources)

        sort_sources_kwargs = {
            "exclude_exhaust": True,
            "exclude_biogenic_methane": False,
            "exclude_png": False,
            "priority_score_filter_threshold":
                job_data_dict['parameters']['reportable_risk_priority_score_ps_threshold'],
            "report_mode_amplitude_threshold": report_mode_amp_threshold,
            "scfh_threshold": job_data_dict['parameters']['minimum_emissions'],
            "report_boundary_shape": report_boundary_shape,
        }

        emission_sources = processor.sort_emission_sources(
            emission_sources=emission_sources,
            report_type_id=report_type_id,
            report_mode_type_id=report_mode_type_id,
            **sort_sources_kwargs)

    else:
        Logger.log.info('Warning: Peaks or Segments data returned as None type')
        emission_sources = []
        invalid_peaks = []

    # calculate the valid peaks from the set of invalid peaks
    invalid_uuids = [x.uuid_str for x in invalid_peaks]
    valid_peaks = [x for x in peaks if x.uuid_str not in invalid_uuids]

    # populate unique ids that make up the emission sources
    emission_sources = populate_source_uuids(emission_sources)

    return emission_sources, valid_peaks, invalid_peaks
