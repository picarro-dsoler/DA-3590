import os
import uuid
import configparser
import datetime
import re
import numpy as np
import pandas as pd
import sqlite3

from dashboard.common_modules.time_calcs import epoch_2_gmtstring
from dashboard.db_io.prod_sql_queries import read_cred_file, sql_query_wrapper, get_report_source_schema

from dashboard.etl_scripts.energy_service.energy_service_modules import format_investigation_time, \
    format_investigation_status, qc_investigtion_record, format_investigation_notes
from helper.logger import Logger
from helper.util import get_abs_path, get_cur_dir


def get_client_name(inv_df, cust_name):
    #client_names = np.unique(inv_df['client'])
    client_names = inv_df['client'].dropna().unique()
    for client_name in client_names:
        if re.search(cust_name, client_name.lower()) is not None:
            return client_name

    raise ValueError('Could not find strict customer name in List of Formatted customer names')


def get_investigation_field_meta(creds, meta_table_name):
    # get leak investigation field/value meta data

    sql_inv_meta_data = '''
                SELECT Id, Name FROM %s
                ''' % meta_table_name

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None

    rows = sql_query_wrapper(sql_inv_meta_data, sql_addr, creds, n_retries, retry_delay)
    meta_key_value = dict(rows)

    return meta_key_value


def get_report_last_survey_date(creds, report_id):
    sql_report_last_survey_date = '''
                SELECT MAX( EndEpoch ) FROM ReportDrivingSurvey
                LEFT JOIN Survey ON Survey.Id = ReportDrivingSurvey.SurveyId
                WHERE ReportId LIKE '{}'
                '''.format(report_id)

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None

    rows = sql_query_wrapper(sql_report_last_survey_date, sql_addr, creds, n_retries, retry_delay)
    report_last_survey_date = rows[0][0]

    return report_last_survey_date


def get_inv_box_status(creds, report_id):
    n_retries = 40
    retry_delay = 30.0
    inv_boxes = []

    report_source_schema = get_report_source_schema(report_id)

    if report_source_schema == 'na':
        inv_boxes = []

    elif report_source_schema == '<=6.1.x':
        sql_inv_box = '''
                    SELECT
                        Box.Id AS BoxId,
                        BoxTypes.Description as BoxTypeStr,
                        ReportInvestigation.Id as ReportInvestigationId, ReportInvestigation.FoundDateTime,
                        ReportInvestigation.LeakLatitude, ReportInvestigation.LeakLongitude,
                        ReportInvestigation.GpsPrecision, ReportInvestigation.Notes,
                        [User].FirstName, [User].LastName,
                        InvestigationStatusTypes.Name,
                        RPA.EpochTime as PeakEpoch,
                        RPA.GpsLatitude as PeakLatitude, RPA.GpsLongitude as PeakLongitude,
                        SurveyorUnit.Description,
                        RPA.Amplitude, RPA.CH4,
                        RPA.AggregatedEthaneRatio, EthaneAnalysisDispositionTypes.Description,
                        RPA.AggregatedClassificationConfidence, [User].UserName,
                        RPA.PeakNumber, RPA.Lisa.STAsText(),
                        RPA.EthaneRatioSdev, RPA.Sigma,
                        APA.PriorityScore, Box.InvestigationCompleteDateTime,
                        Box.CantGetIn,
                        APA.EmissionRate, 
                        null as EmissionRateUpperBound,  null as EmissionRateLowerBound
                    FROM Box
                    LEFT JOIN BoxTypes ON  Box.BoxTypeId = BoxTypes.Id
                    LEFT JOIN ReportPeakArchive RPA on Box.ReportPeakId = RPA.Id
                    LEFT JOIN AnalyticsPeakArchive APA on RPA.Id = APA.ReportPeakId
                    LEFT JOIN EthaneAnalysisDispositionTypes ON RPA.AggregatedDisposition = EthaneAnalysisDispositionTypes.Id
                    LEFT JOIN ReportInvestigation ON Box.Id = ReportInvestigation.BoxId
                    LEFT JOIN [User] ON ReportInvestigation.LeakFinderUserId = [User].Id
                    LEFT JOIN InvestigationStatusTypes ON Box.InvestigationStatusTypeId = InvestigationStatusTypes.Id
                    LEFT JOIN Survey ON RPA.SurveyId = Survey.Id
                    LEFT JOIN Analyzer ON Survey.AnalyzerId = Analyzer.Id
                    LEFT JOIN SurveyorUnit ON Analyzer.SurveyorUnitId = SurveyorUnit.Id
                    WHERE Box.ReportId = '{}'
                    AND BoxTypes.Description = 'Indication'
                    '''.format(report_id)

        sql_addr = None

        rows = sql_query_wrapper(sql_inv_box, sql_addr, creds, n_retries, retry_delay)
        inv_boxes = []
        for box_data_sqlrow in rows:
            inv_boxes.append(box_data_sqlrow)

    elif report_source_schema == '>=6.2.x':
        sql_inv_box = '''
                    SELECT
                        Box.Id AS BoxId,
                        BoxTypes.Description as BoxTypeStr,
                        ReportInvestigation.Id as ReportInvestigationId, ReportInvestigation.FoundDateTime,
                        ReportInvestigation.LeakLatitude, ReportInvestigation.LeakLongitude,
                        ReportInvestigation.GpsPrecision, ReportInvestigation.Notes,
                        [User].FirstName, [User].LastName,
                        InvestigationStatusTypes.Name,
                        Peak.EpochTime as PeakEpoch,
                        EmissionSource.GpsLatitude as PeakLatitude, EmissionSource.GpsLongitude as PeakLongitude,
                        SurveyorUnit.Description,
                        Peak.Amplitude, Peak.CH4,
                        EmissionSource.EthaneRatio, EthaneAnalysisDispositionTypes.Description,
                        EmissionSource.ClassificationConfidence, [User].UserName,
                        EmissionSource.PeakNumber, EmissionSource.Lisa.STAsText(),
                        EmissionSource.EthaneRatioUncertainty, Peak.Sigma,
                        EmissionSource.PriorityScore, Box.InvestigationCompleteDateTime,
                        Box.CantGetIn,
                        EmissionSource.EmissionRate, EmissionSource.EmissionRateUpperBound, 
                        EmissionSource.EmissionRateLowerBound
                    FROM Box
                    LEFT JOIN BoxTypes ON Box.BoxTypeId = BoxTypes.Id
                    LEFT JOIN EmissionSource ON Box.EmissionSourceId = EmissionSource.Id
                    LEFT JOIN Peak ON  EmissionSource.RepresentativePeakId = Peak.Id
                    LEFT JOIN EthaneAnalysisDispositionTypes ON EmissionSource.Disposition = EthaneAnalysisDispositionTypes.Id
                    LEFT JOIN ReportInvestigation ON Box.Id = ReportInvestigation.BoxId
                    LEFT JOIN [User] ON ReportInvestigation.LeakFinderUserId = [User].Id
                    LEFT JOIN InvestigationStatusTypes ON Box.InvestigationStatusTypeId = InvestigationStatusTypes.Id
                    LEFT JOIN Survey ON Peak.SurveyId = Survey.Id
                    LEFT JOIN Analyzer ON Survey.AnalyzerId = Analyzer.Id
                    LEFT JOIN SurveyorUnit ON SurveyorUnit.Id = Analyzer.SurveyorUnitId
                    WHERE Box.ReportId = '{}'
                    AND BoxTypes.Description = 'Indication'
                    '''.format(report_id)

        sql_addr = None

        rows = sql_query_wrapper(sql_inv_box, sql_addr, creds, n_retries, retry_delay)
        inv_boxes = []
        for box_data_sqlrow in rows:
            inv_boxes.append(box_data_sqlrow)

    else:
        raise ValueError('ReportPeakId and EmissionSourceId results returned with an unexpected condition! '
                         'Report ID %s' % report_id)

    return inv_boxes


def get_inv_box_fields(creds, report_inv_id):
    # get the investigation fields values for the investigation
    sql_inv_data = '''
                    SELECT
                        MasterInvestigationItem.Label,
                        ReportInvestigationItem.SelectedValue
                    FROM ReportInvestigationItem
                    LEFT JOIN InvestigationTemplateItem ON InvestigationTemplateItem.Id = ReportInvestigationItem.InvestigationTemplateItemId
                    LEFT JOIN MasterInvestigationItem ON MasterInvestigationItem.Id = InvestigationTemplateItem.MasterInvestigationItemId
                    WHERE ReportInvestigationItem.ReportInvestigationId = '{}';
                '''.format(report_inv_id)

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None

    rows = sql_query_wrapper(sql_inv_data, sql_addr, creds, n_retries, retry_delay)
    inv_box_values = dict(rows)

    return inv_box_values


def get_inv_time(box_id, creds):
    n_retries = 40
    retry_delay = 30.0
    sql_addr = '20.80.80.81'

    # leak investigation time query
    sql_query_strng = '''
                            SELECT sum(DATEDIFF (SECOND, SessionStart, SessionEnd)) as InvestigationSeconds
                            FROM InvestigationSession
                            WHERE BoxId = '{}'
                            AND SessionEnd is not NULL;
                            '''.format(box_id)
    try:
        inv_sec = \
            sql_query_wrapper(sql_query_strng, sql_addr, creds, n_retries, retry_delay)[0][0]
        if inv_sec == None:
            inv_sec = 0
    except IndexError:
        inv_sec = 0

    return inv_sec


# def get_investigation_app_data(customer_name, r_id, output_csv, dev_key):
    # report_data = load_smartsheet_report(dev_key[0], r_id, 1000000)
def get_investigation_app_data(customer_name, report_data, outfile_flag, output_csv):

    # ls_creds = read_cred_file(r'/home/ubuntu/cred/lsdb.txt')
    ls_creds = None
    # ls_creds = read_cred_file(r'/home/ubuntu/cred/lsdb.txt')
    # ls_ip_addr = '20.80.80.81'
    ls_ip_addr = None
    n_retries = 40
    retry_delay = 30.0

    # build the cursor factory for pcubed sql db queries
    # cursor_factory = build_cursor_factory(r'/home/ubuntu/cred/lsdb.txt', '20.80.80.81', 'SurveyorProductionLS')

    decoder_config = configparser.ConfigParser()
    # decoder_config.read(r'/home/ubuntu/gitrepos/Elastic/src/dashboard/etl_scripts/energy_service/investigation_decoder.ini')
    decoder_config.read(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'investigation_decoder.ini'))
    # define possible names for the leak grade field
    leak_grade_aliases = set(decoder_config['leak_grade']['leak_grade_aliases'].split(', '))
    other_leak_grade_list = decoder_config['leak_grade']['other_leak_grade_list'].split(', ')
    other_leak_grade_list = [x.upper() for x in other_leak_grade_list]
    # define possible names for the leak type field
    leak_type_aliases = set(decoder_config['leak_type']['leak_type_aliases'].split(', '))
    other_leak_types_list = decoder_config['leak_type']['other_leak_types_list'].split(', ')
    ag_alias_list = decoder_config['leak_type']['ag_alias_list'].split(', ')
    bg_alias_list = decoder_config['leak_type']['bg_alias_list'].split(', ')
    # define possible names for the City field
    city_aliases = set(decoder_config['city']['city_aliases'].split(', '))

    # define possible names for the leak location field
    leak_location_aliases = set(decoder_config['leak_location']['leak_location_aliases'].split(', '))

    # define possible names for the leak location field
    readings_aliases = decoder_config['readings']

    # get leak info meta data
    leak_types_meta = get_investigation_field_meta(ls_creds, 'LeakTypes')

    # get leak location meta data
    leak_location_meta = get_investigation_field_meta(ls_creds, 'LeakLocationTypes')
    # get leak source meta data
    leak_source_meta = get_investigation_field_meta(ls_creds, 'LeakSourceTypes')

    # read the management data from the smartsheet file
    investigation_data = pd.DataFrame(report_data) if type(report_data) == dict else report_data
    client_name_format = get_client_name(investigation_data, customer_name)

    # filter on customer name
    investigation_data = investigation_data[investigation_data['client'] == client_name_format]
    if client_name_format == "RETIPIU'":
        investigation_data['client'] = 'RETI PIU'
    if client_name_format.lower() == "depa-edaattikis":
        investigation_data['client'] = 'DEPA'

    # get only unique report values from the input data, replace nan with empty string
    unq, unq_ind = np.unique(investigation_data['finalreportpcubed'].fillna(value=''), return_index=True)
    investigation_data = investigation_data.iloc[np.sort(unq_ind), :].copy()

    # # get the full report details and attach to the dataframe
    # report_ids = []
    # report_titles = []
    # report_dates = []

    for i, row in investigation_data.iterrows():

        # check if the field for the report name entered into smartsheets is a vaild entry
        reportid_sqlrow = None
        if type(row['finalreportpcubed']) is str:
            sql_report_data = '''
                SELECT TOP 1 Report.Id, Report.ReportTitle, Report.DateStarted, RC.PercentCoverageAssets
                FROM Report
                LEFT JOIN ReportCompliance RC on Report.Id = RC.ReportId
                LEFT JOIN Customer C on Report.CustomerId = C.Id
                WHERE Report.Id LIKE '{}%'
                AND LOWER(C.Name) LIKE '%{}%'
                '''.format(row['finalreportpcubed'][3:], row['client'].lower())
            try:
                reportid_sqlrow = sql_query_wrapper(sql_report_data, ls_ip_addr, ls_creds, n_retries, retry_delay)[0]
            except IndexError:
                reportid_sqlrow = None
        else:
            reportid_sqlrow = None

        # add the report information to the smartsheet data
        if reportid_sqlrow is not None:

            investigation_data.at[i, 'ReportId'] = reportid_sqlrow[0]
            investigation_data.at[i, 'ReportTitle'] = reportid_sqlrow[1].replace(',', ' ')
            investigation_data.at[i, 'ReportDate'] = reportid_sqlrow[2].strftime('%Y-%m-%d')
            try:
                asset_coverage_frac = float(reportid_sqlrow[3])
                if asset_coverage_frac > 1.0:
                    asset_coverage_frac = 1.0
                investigation_data.at[i, 'AssetCoverageFrac'] = asset_coverage_frac
            except TypeError:
                investigation_data.at[i, 'AssetCoverageFrac'] = 0.0

        else:
            Logger.log.info("Did not find a report with Id: %s" % row['finalreportpcubed'])

    # create a data frame that includes the report investigation data
    investigation_data_with_report_details = investigation_data.where((pd.notnull(investigation_data)), None)

    leakdata = []

    # go through the DB and get the leak data for each report
    for i, row in investigation_data_with_report_details.iterrows():
        inv_box_data = []

        # get the investigation data for each investigation box, for europe this is limited to lisa investgation, gap is not included
        if ('ReportId' in row) and (row['ReportId'] is not None):
            Logger.log.info(row['ReportId'])
            inv_box_data = get_inv_box_status(ls_creds, row['ReportId'])
            # get the date for the latest survey included in the report, used as driving completion date
            last_survey_epoch = get_report_last_survey_date(ls_creds, row['ReportId'])
            last_survey_date = epoch_2_gmtstring(last_survey_epoch).split()[0]

        else:
            continue

        # get the investgation data for the investigation boxes in the report query
        for inv_box_result in inv_box_data:
            inv_box_meta = []

            # format the time string for the investigation record
            foundtimestr, foundtimestr_eu, \
                time_to_investigation, investigation_complete_time_str = format_investigation_time(inv_box_result, row)

            investigation_status = format_investigation_status(row['Investigation Status'])

            investigation_seconds = get_inv_time(inv_box_result[0], ls_creds)

            # format the free form cgi comment field to remove commas and other stuff
            cgi_comments = format_investigation_notes(inv_box_result[27])

            # format unparsable pipelinemeters as length 0 for ES index compatibility
            if row['Pipeline length (m)'] == '#UNPARSEABLE':
                row['Pipeline length (m)'] = 0
            # format NaN pipelinemeters as length 0 for ES index compatibility
            if pd.isnull(row['Pipeline length (m)']):
                row['Pipeline length (m)'] = 0

            # if no investgation template was used....
            if inv_box_result[2] is None:

                leakdata.append({'BoundaryName': row['boundaryid'], 'Region': str(row['boundaryname']).replace(',', ''),
                                 'City': str(row['region']).replace(',', ''),
                                 'PCubedReportName': row['finalreportpcubed'],
                                 'PCubedReportGuid': str(row['ReportId']),
                                 'LastSurveyDate': last_survey_date,
                                 'LastSurveyDateStr': last_survey_date.split()[0],
                                 'PCubedReportTitle': row['ReportTitle'].replace(',', ' '), 'PCubedReportDate': row['ReportDate'],
                                 'PCubedReportDateStr': row['ReportDate'],
                                 'AssetCoverageFrac': row['AssetCoverageFrac'],
                                 'PipelineMeters': row['Pipeline length (m)'], 'BoxId': str(inv_box_result[0]),
                                 'LeakGrade': 'No Grade', 'FoundDateTime': foundtimestr,
                                 'FoundDateStr': foundtimestr.split()[0],
                                 'FoundDateTimeStr': foundtimestr_eu,
                                 'LeakInvestigatorUserName': None,
                                 'first_name': None,
                                 'last_name': None,
                                 'SurveyorUnitName': inv_box_result[14],
                                 'LocationRemarks': '', 'Notes': '', 'AG/BG': 'NC',
                                 'LeakFound': inv_box_result[10],
                                 'LeakLocation': '',
                                 'LeakSource': '',
                                 'LeakLatitude': None,
                                 'LeakLongitude': None, 'LeakGpsPrecision': None,
                                 'PeakLatitude': inv_box_result[12], 'PeakLongitude': inv_box_result[13],
                                 'PeakEpoch': inv_box_result[11],
                                 'TimeToInvestigation': time_to_investigation,
                                 'DrivingStatus': row['Driving status'],
                                 'InvestigationStatus': investigation_status,
                                 'Amplitude': inv_box_result[15], 'CH4': inv_box_result[16],
                                 'AggregatedEthaneRatio': inv_box_result[17],
                                 'AggregatedDisposition': inv_box_result[18],
                                 'AggregatedClassificationConfidence': inv_box_result[19],
                                 'PeakNumber': 'LISA %d' % inv_box_result[21],
                                 'LisaShape': inv_box_result[22],
                                 'EthaneRatioSdev': inv_box_result[23],
                                 'Sigma': inv_box_result[24],
                                 'PriorityScore': inv_box_result[25],
                                 'EmissionRate': inv_box_result[28],
                                 'EmissionRateUpperBound': inv_box_result[29],
                                 'EmissionRateLowerBound': inv_box_result[30],
                                 'StreetNumber': '',
                                 'ApartmentNumber': '',
                                 'StreetName': '',
                                 'UserCity': '',
                                 'State': '',
                                 'InstType': '',
                                 'InstSerNum': '',
                                 'SurfaceReading': '',
                                 'ReadingUnitSurface': '',
                                 'BarholeReading': '',
                                 'ReadingUnitBarhole': '',
                                 'UpdateTimeString': datetime.datetime.now().strftime('%d-%m-%Y %H:%M'),
                                 'LeakInvestigationSeconds': investigation_seconds,
                                 'InvestigationCompleteDateTime': investigation_complete_time_str,
                                 'CgiComment': cgi_comments})

            # if one of the investigation templates was used...
            else:
                # get the template data
                inv_box_meta = get_inv_box_fields(ls_creds, inv_box_result[2])

                # qc data from the leak  investigation result
                leakgrade, locationremarks, leaktype, leaklocation, leaksource, address, \
                    sensor_readings, insttype, instsn, city = \
                    qc_investigtion_record(inv_box_meta,
                                           leak_grade_aliases, other_leak_grade_list,
                                           leak_type_aliases, leak_types_meta, other_leak_types_list,
                                           ag_alias_list, bg_alias_list,
                                           leak_location_aliases, leak_location_meta,
                                           leak_source_meta,
                                           readings_aliases, customer_name)
                # get correct city source
                if (city == '') or (city == None):
                    city = str(row['region']).replace(',', '')

                # format the free form notes field to remove commas and other stuff
                notes = format_investigation_notes(inv_box_result[7])

                # create the data dictionry for the elastic index
                leakdata.append({'BoundaryName': row['boundaryid'], 'Region': str(row['boundaryname']).replace(',', ''),
                                 # 'City': str(row['region']).replace(',', ''),
                                 'City': city,
                                 'PCubedReportName': row['finalreportpcubed'],
                                 'PCubedReportGuid': str(row['ReportId']),
                                 'LastSurveyDate': last_survey_date,
                                 'LastSurveyDateStr': last_survey_date.split()[0],
                                 'PCubedReportTitle': row['ReportTitle'].replace(',', ' '), 'PCubedReportDate': row['ReportDate'],
                                 'PCubedReportDateStr': row['ReportDate'],
                                 'AssetCoverageFrac': row['AssetCoverageFrac'],
                                 'PipelineMeters': row['Pipeline length (m)'], 'BoxId': str(inv_box_result[0]),
                                 'LeakGrade': leakgrade, 'FoundDateTime': foundtimestr,
                                 'FoundDateStr': foundtimestr.split()[0],
                                 'FoundDateTimeStr': foundtimestr_eu,
                                 'LeakInvestigatorUserName': inv_box_result[20].split('@')[0],
                                 'first_name': str(inv_box_result[8]).lower(),
                                 'last_name': str(inv_box_result[9]).lower(),
                                 'SurveyorUnitName': inv_box_result[14],
                                 'LocationRemarks': locationremarks, 'Notes': notes,
                                 'AG/BG': leaktype,
                                 'LeakFound': inv_box_result[10],
                                 'LeakLocation': leaklocation,
                                 'LeakSource': leaksource,
                                 'LeakLatitude': inv_box_result[4], 'LeakLongitude': inv_box_result[5],
                                 'LeakGpsPrecision': inv_box_result[6],
                                 'PeakLatitude': inv_box_result[12], 'PeakLongitude': inv_box_result[13],
                                 'PeakEpoch': inv_box_result[11],
                                 'TimeToInvestigation': time_to_investigation,
                                 'DrivingStatus': row['Driving status'],
                                 'InvestigationStatus': investigation_status,
                                 'Amplitude': inv_box_result[15], 'CH4': inv_box_result[16],
                                 'AggregatedEthaneRatio': inv_box_result[17],
                                 'AggregatedDisposition': inv_box_result[18],
                                 'AggregatedClassificationConfidence': inv_box_result[19],
                                 'PeakNumber': 'LISA %d' % inv_box_result[21],
                                 'LisaShape': inv_box_result[22],
                                 'EthaneRatioSdev': inv_box_result[23],
                                 'Sigma': inv_box_result[24],
                                 'PriorityScore': inv_box_result[25],
                                 'EmissionRate': inv_box_result[28],
                                 'EmissionRateUpperBound': inv_box_result[29],
                                 'EmissionRateLowerBound': inv_box_result[30],
                                 'StreetNumber': address['Street Number'],
                                 'ApartmentNumber': address['Apartment Number'],
                                 'StreetName': address['Street Name'],
                                 'UserCity': address['City'],
                                 'State': address['State'],
                                 'InstType': insttype,
                                 'InstSerNum': instsn,
                                 'SurfaceReading': sensor_readings['SurfaceReading'],
                                 'ReadingUnitSurface': sensor_readings['ReadingUnitSurface'],
                                 'BarholeReading': sensor_readings['BarholeReading'],
                                 'ReadingUnitBarhole': sensor_readings['ReadingUnitBarhole'],
                                 'UpdateTimeString': datetime.datetime.now().strftime('%d-%m-%Y %H:%M'),
                                 'LeakInvestigationSeconds': investigation_seconds,
                                 'InvestigationCompleteDateTime': investigation_complete_time_str,
                                 'CgiComment': cgi_comments
                                 })

                if leakdata[-1]['LeakFound'] == 'Found_Gas_Leak':
                    leakdata[-1]['FoundGasLeakUuid'] = uuid.uuid1()

            # adding these additional fields at the end for backward compatibility in the datafiles
            if 'type1coverage' in row.keys():
                type1coverage = row['type1coverage']
                if isinstance(type1coverage, str):
                    type1coverage.replace(",", "").replace(" ", "")
                if (type1coverage is None) or (type1coverage == 0.0):
                    type1coverage = 'None'

                pipeline_len_m = row['Pipeline length (m)']
                if isinstance(pipeline_len_m, str):
                    pipeline_len_m.replace(",", "").replace(" ", "")

                if (pipeline_len_m is None) or (pipeline_len_m == 0.0):
                    pipeline_len_m = 'None'

                try:
                    pipeline_len_m = float(pipeline_len_m)
                except ValueError:
                    pipeline_len_m = 'None'

                # try to write type1coverage as a float
                try:
                    leakdata[-1].update({'type1coverage': float(type1coverage)})
                # if type1coverage can't be converted to a float
                except ValueError:
                    # try to use the total length of pipes in meters instead
                    try:
                        # leakdata[-1].update({'type1coverage': float(pipeline_len_m)})
                        leakdata[-1].update({'type1coverage': float(pipeline_len_m * row['AssetCoverageFrac'])})
                    # otherwise write a large value instead this avoids a divide by zero situation and more correctly
                    # represents the case where you are calculating number of leaks divided by type 1 coverage.
                    # This is a dummy value for for invalid smartsheet entries, if you find this in the data go fix
                    # smartsheet
                    except (ValueError, TypeError):
                        # leakdata[-1].update({'type1coverage': 1e20})
                        leakdata[-1].update({'type1coverage': 0.0})

            if 'contract' in row.keys():
                leakdata[-1].update({'contract': row['contract']})
            if 'assignment' in row.keys():
                leakdata[-1].update({'assignment': row['assignment']})

    # create only one unique id per lisa/box for the doc if the leak was investigated
    box_id = np.asarray([x['BoxId'] for x in leakdata])
    box_ids_unq, unique_indices = np.unique(box_id, return_index=True)
    for box_id_unq, unq_ind in zip(box_ids_unq, unique_indices):
        leakdata[unq_ind]['CountBoxUuid'] = uuid.uuid1()

        # find all possible investigation statuses for the current box id
        leak_found_statuses = np.asarray([leakdata[x]['LeakFound'] for x in np.flatnonzero(box_id == box_id_unq)])
        # if at least one status of Found_Gas_Leak is present
        if 'Found_Gas_Leak' in leak_found_statuses:
            leakdata[unq_ind]['FoundAtLeastOneUuid'] = uuid.uuid1()

        # if that box id has a status of not investigated create a unique ide
        if leakdata[unq_ind]['LeakFound'] == 'Not_Investigated':
            leakdata[unq_ind]['NotInvestigatedUuid'] = uuid.uuid1()
        # otherwise if the box has in progress status, mark as not investigated
        elif leakdata[unq_ind]['LeakFound'] == 'In_Progress':
            leakdata[unq_ind]['InProgressUuid'] = uuid.uuid1()
        # otherwise if the box has any other status, count this as one investigation
        else:
            leakdata[unq_ind]['InvestigatedUuid'] = uuid.uuid1()

    leakdf = pd.DataFrame(leakdata)
    if len(leakdf) == 0:
        Logger.log.info('No Investigation Data found for customer name like %s' % investigation_data.iloc[0]['client'].lower())

        return pd.DataFrame(), ''

    Logger.log.info('Got Investigation Data')

    if outfile_flag:
        Logger.log.info('Writing *csv....')

        leakdf.drop('LisaShape', axis=1).to_csv(output_csv, index=False)

    return leakdf, output_csv


def get_gas2go_data(output_csv):

    Logger.log.info('Querying Gas2Go database')

    # query data
    con = sqlite3.connect(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'italgas_g2g_leak_data_ingester', 'database', 'italgas_g2g.db'))
    g2g_df = pd.read_sql_query("SELECT * FROM Leaks", con)

    if len(g2g_df) == 0:
        Logger.log.info('Query returned no data')

    #format columns
    g2g_df['codStato'] = g2g_df['codStato'].astype('int64')
    g2g_df['codValidazione'] = pd.to_numeric(g2g_df['codValidazione'])
    # g2g_df['dataArrivoSulCampo'] = g2g_df['dataArrivoSulCampo'].astype(pd.Int64Dtype())
    # g2g_df['dataLocalizzazione'] = g2g_df['dataLocalizzazione'].astype(pd.Int64Dtype())
    # g2g_df['dataRiparazione'] = g2g_df['dataRiparazione'].astype(pd.Int64Dtype())
    g2g_df['dataArrivoSulCampo'] = pd.to_numeric(g2g_df['dataArrivoSulCampo'], errors='coerce')
    g2g_df['dataLocalizzazione'] = pd.to_numeric(g2g_df['dataLocalizzazione'], errors='coerce')
    g2g_df['dataRiparazione'] = pd.to_numeric(g2g_df['dataRiparazione'], errors='coerce')
    # g2g_df['dataUltimaMod'] = g2g_df['dataUltimaMod'].astype(pd.Int64Dtype())
    g2g_df["indirizzo"] = g2g_df["indirizzo"].replace(",", "", regex=True)
    g2g_df["indirizzoLisa"] = g2g_df["indirizzoLisa"].replace(",", "", regex=True)
    g2g_df["indirizzoLocalizzazione"] = g2g_df["indirizzoLocalizzazione"].replace(",", "", regex=True)
    g2g_df["indirizzoRiparazione"] = g2g_df["indirizzoRiparazione"].replace(",", "", regex=True)

    # Logger.log.info('Writing Gas2Go data to csv')
    #
    g2g_df.to_csv(output_csv, index=False)

    return g2g_df, output_csv


def get_smartsheet_reports_in_g2g(g2g_leak_data, report_data):

    Logger.log.info('Filtering smartsheet data to match g2g reports')

    # Extract 6digit reportId from leak data (i.e. g2g data)
    g2g_leak_data['reportId'] = g2g_leak_data['lisa'].str.extract(r"([a-zA-Z0-9]{6})").astype(str)

    # Filter report_data (i.e. smartsheet) for italgas data and extract 6 digit reportId
    report_data['reportId'] = report_data['finalreportpcubed'].str.extract(r"([a-zA-Z0-9]{6})").astype(str)
    report_data = report_data[report_data['client'].str.lower() == 'italgas']
    missing_smartsheet_rpts = list(set(g2g_leak_data['reportId']) - set(report_data['reportId']))
    missing_g2g_rpts = list(set(report_data['reportId']) - set(g2g_leak_data['reportId']))

    # Filter smartsheet data for reports present in g2g and drop added columns
    unq_g2g_reports = list(np.unique(g2g_leak_data['reportId']))
    report_data = report_data[report_data['reportId'].isin(unq_g2g_reports)]
    # Ensure that G2G is not ahead of smartsheet
    unq_ss_reports = list(np.unique(report_data['reportId']))
    g2g_leak_data = g2g_leak_data[g2g_leak_data['reportId'].isin(unq_ss_reports)]
    report_data = report_data.drop(['reportId'], axis=1)

    log_msg = 'G2G includes the following reports not in Smartsheet: ' + str(missing_smartsheet_rpts)
    Logger.log.info(log_msg)
    log_msg = 'Smartsheet includes the following reports not in G2G: ' + str(missing_g2g_rpts)
    Logger.log.info(log_msg)

    return g2g_leak_data, report_data


def merge_g2g_and_surveyor_data(g2g_leak_data, leak_data, output_csv):

    Logger.log.info('Pre-processing Surveyor and G2G data before merge')

    leak_data = leak_data.drop('LisaShape', axis=1) # LisaShape is not included in csv
    leak_data['lisaId'] = leak_data['PeakNumber'].str.extract(r"LISA (\d+)")
    leak_data['lisaId'] = pd.to_numeric(leak_data['lisaId'])
    leak_data['reportId'] = leak_data['PCubedReportName'].str[3:9]
    g2g_leak_data['lisaId'] = g2g_leak_data['lisa'].str.extract(r"LISA(\d+)")
    g2g_leak_data['lisaId'] = pd.to_numeric(g2g_leak_data['lisaId'])

    # Find and filter out duplicate LISAs so merge to G2G data won't create duplicates
    leak_data = leak_data.sort_values(by=['reportId', 'lisaId']).reset_index(drop=True)
    leak_data['duplicated'] = leak_data.duplicated(subset=['reportId', 'lisaId'], keep='first')
    leak_data = leak_data[(leak_data['duplicated'] == False)]
    # Drop unnecessary columns
    leak_data = leak_data.drop(columns=['LeakGrade', 'FoundDateTime', 'FoundDateStr', 'FoundDateStr',
                                            'FoundDateTimeStr', 'LeakInvestigatorUserName', 'first_name', 'last_name',
                                            'LocationRemarks', 'Notes', 'AG/BG', 'LeakFound', 'LeakLocation',
                                            'LeakSource',
                                            'LeakLatitude', 'LeakLongitude', 'LeakGpsPrecision', 'TimeToInvestigation',
                                            'InvestigationStatus', 'StreetNumber', 'ApartmentNumber', 'StreetName',
                                            'UserCity', 'State', 'InstType', 'InstSerNum', 'SurfaceReading',
                                            'ReadingUnitSurface',
                                            'BarholeReading', 'ReadingUnitBarhole', 'LeakInvestigationSeconds',
                                            'InvestigationCompleteDateTime', 'CgiComment', 'FoundGasLeakUuid',
                                            'FoundAtLeastOneUuid', 'InvestigatedUuid', 'InProgressUuid',
                                            'NotInvestigatedUuid', 'CountBoxUuid',
                                            'duplicated'])

    # Create LeakFound column based on G2G criteria
    g2g_leak_data['LeakFound'] = ''
    g2g_leak_data.loc[g2g_leak_data['codiceDispersione'].isin(['A1', 'A2', 'B', 'C', 'PRELOCALIZZATA']), 'LeakFound'] = 'Found_Gas_Leak'
    g2g_leak_data.loc[g2g_leak_data['codiceDispersione'] == '', 'LeakFound'] = 'Not_Investigated'
    g2g_leak_data.loc[g2g_leak_data['codiceDispersione'] == '', 'codiceDispersione'] = 'No Grade'
    g2g_leak_data.loc[g2g_leak_data['aereoInterrato'] == '', 'aereoInterrato'] = 'NC'
    g2g_leak_data.loc[g2g_leak_data['intervento'].isin(['INTERESSA IMPIANTI ALTRI SERVIZI-RP',
                                                        'INTERESSA IMPIANTO ALTRO DISTRIBUTORE-RP']), 'LeakFound'] = 'Found_Other_Source'
    g2g_leak_data.loc[g2g_leak_data['intervento'].isin(['COMUNE INDIRIZZO ERRATO-RP', 'ANOMALIA NON RISCONTRATA-RP']),
                      'LeakFound'] = 'No_Gas_Found'

    Logger.log.info('Merging Surveyor and G2G data')

    italgas_leak_data = g2g_leak_data.merge(leak_data, on=['reportId', 'lisaId'], how='left')


    # Convert BoxId to string to ensure consistent data types
    italgas_leak_data['BoxId'] = italgas_leak_data['BoxId'].astype(str)
    # Add in Uuid columns for Dashboard visual creation
    # Create only one unique id per lisa/box for the doc if the leak was investigated
    box_id = np.asarray(italgas_leak_data['BoxId'])
    box_ids_unq, unique_indices = np.unique(box_id, return_index=True)

    italgas_leak_data['CountBoxUuid'] = ''
    italgas_leak_data['FoundAtLeastOneUuid'] = ''
    italgas_leak_data['NotInvestigatedUuid'] = ''
    italgas_leak_data['InvestigatedUuid'] = ''

    for box_id_unq, unq_ind in zip(box_ids_unq, unique_indices):
        italgas_leak_data.loc[[unq_ind], ['CountBoxUuid']] = uuid.uuid1()

        # find all possible investigation statuses for the current box id
        leak_found_statuses = np.asarray([italgas_leak_data['LeakFound'][x] for x in np.flatnonzero(box_id == box_id_unq)])
        # if at least one status of Found_Gas_Leak is present
        if 'Found_Gas_Leak' in leak_found_statuses:
            italgas_leak_data.loc[[unq_ind], ['FoundAtLeastOneUuid']] = uuid.uuid1()

        # if that box id has a status of not investigated create a unique id
        if italgas_leak_data['LeakFound'][unq_ind] == 'Not_Investigated':
            italgas_leak_data.loc[[unq_ind], ['NotInvestigatedUuid']] = uuid.uuid1()
        # otherwise if the box has any other status, count this as one investigation
        else:
            italgas_leak_data.loc[[unq_ind], ['InvestigatedUuid']] = uuid.uuid1()

    italgas_leak_data['PipelineMeters'] = italgas_leak_data['PipelineMeters'].fillna(0)
    italgas_leak_data = italgas_leak_data[italgas_leak_data['BoxId'] != 'c53f2bb2-acd4-49b1-a754-1227357074cc']
    italgas_leak_data.to_csv(output_csv, index=False)

    return italgas_leak_data, output_csv

if __name__ == "__main__":
    # # =====================================
    customer_name_common = 'toscana'
    # customer_name_common = 'italgas'

    smartsheet_report_id = 3006949269759876  # report id for teh service tracking master document
    # # =====================================

    now_date_str = datetime.datetime.now().strftime('%Y%m%d')
    output_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'utility_data', '%s_leak_investigation_%s.csv' % (customer_name_common, now_date_str))

    # read the credential filename
    # TODO: revisit
    smartsheet_key = read_cred_file('/home/ubuntu/cred/smartsheet.txt')

    leak_data, csv_file = get_investigation_app_data(customer_name_common, smartsheet_report_id, True,
                                                     output_filename)

