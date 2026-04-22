import time
#import _mssql
#import pymssql
import traceback

import numpy as np

#from dal import sql
from dashboard.data_io.data_type_transforms import peak_2_dict, report_eqsource_2_dict, segment_2_dict
#from clusterworker import surveyordal
#from helper.logger import Logger


def read_cred_file(filename):
    with open(filename, 'r') as fh:
        creds = fh.readline()
    creds = creds.split(',')
    creds = [x.strip().strip('\n') for x in creds]

    return creds


def sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay, as_dict=False):
    rows = [(-9999)]

    ctr = 0
    invalid_conn_flag = True
    sql_addr = sql.get_host()
    while invalid_conn_flag:
        if ctr == n_retries:
            raise IndexError('Max number of retries reached %d. Could not connect to SQL Server at %s' %
                             (n_retries, sql_addr))
        ctr += 1
        try:
            # cursor_factory = CursorFactory(sql_addr, creds[0], creds[1], 'SurveyorProductionLS', '7.3')
            with sql.get_conn().get_cursor_pymssql(as_dict) as cur:
                cur.execute(query_string)

                rows = cur.fetchall()
            invalid_conn_flag = False
        except (_mssql.MSSQLDatabaseException, pymssql.OperationalError, TypeError):
            Logger.log.exception(traceback.format_exc())
            Logger.log.error('Connection to SQL Server at %s failed. Retry %d of %d, next retry in %.1f seconds' %
                  (sql_addr, ctr, n_retries, retry_delay))
            time.sleep(retry_delay)
            continue

    return rows


def get_report_source_schema(report_id):
    n_retries = 40
    retry_delay = 30.0

    report_source_schema = 'unk'  # set the report schema to unknown

    sql_report_schema = '''
                            SELECT Box.Id, Box.ReportPeakId, Box.EmissionSourceId
                            FROM Box
                            LEFT JOIN BoxTypes ON BoxTypes.Id = Box.BoxTypeId
                            WHERE Box.ReportId = '{}'   
                            AND BoxTypes.Description = 'Indication'
                            '''.format(report_id)

    rows = np.asarray(sql_query_wrapper(sql_report_schema, None, (None, None), n_retries, retry_delay))

    if len(rows) == 0:
        # no sources were found in the report
        report_source_schema = 'na'
    elif None in rows[:, 1]:
        # the report uses the Common Report Pipeline schema released in 6.2, EmissionSource table
        report_source_schema = '>=6.2.x'
    elif None in rows[:, 2]:
        # the report uses legacy report object schema, ReportPeak table, from the 6.1.x and prior versions
        report_source_schema = '<=6.1.x'
    elif (None not in rows[:, 1]) and (None not in rows[:, 2]):
        # the report uses legacy report object schema, ReportPeak table, from the 6.1.x and prior versions
        report_source_schema = '<=6.1.x'
    elif (None in rows[:, 1]) and (None in rows[:, 2]):
        raise ValueError('ReportPeakId and EmissionSourceId cannot both be set in the Box table! '
                         'Report ID %s' % report_id)
    else:
        raise ValueError('ReportPeakId and EmissionSourceId results returned with an unexpected condition! '
                         'Report ID %s' % report_id)

    return report_source_schema


def get_analyzer_sn(creds, analyzer_id):

    query_string = """
                   SELECT SerialNumber
                   FROM Analyzer
                   WHERE Id = '%s'
                   """ % (analyzer_id)

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    # pull the surveys from the sql_database
    analyzer_id = [str(row[0]) for row in rows]

    return analyzer_id[0]


def get_survey_types(creds):

    query_string = """
                   SELECT DISTINCT(SurveyModeType.Description)
                   FROM SurveyModeType
                   """

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    # pull the surveys from the sql_database
    survey_modes = [str(row[0]) for row in rows]

    return survey_modes


def get_surveys_from_reports(creds, report_ids):

    survey_ids = []
    for report_id in report_ids:

        query_string = """
                       SELECT ReportDrivingSurvey.SurveyId, ReportDrivingSurvey.ReportId
                       FROM ReportDrivingSurvey
                       WHERE ReportDrivingSurvey.ReportId = '%s'
                       """ % (report_id)

        n_retries = 40
        retry_delay = 30.0
        # sql_addr = '20.80.80.81'
        sql_addr = None
        rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    # pull the surveys from the sql_database
        survey_ids += [str(row[0]) for row in rows]

    return survey_ids


def get_surveys_from_report_names(creds, report_names, customer_name):

    survey_ids = []
    for report_name in report_names:

        query_string = """
                       SELECT ReportDrivingSurvey.SurveyId, ReportDrivingSurvey.ReportId
                       FROM ReportDrivingSurvey
                       LEFT JOIN Report ON ReportDrivingSurvey.ReportId = Report.Id
                       LEFT JOIN Customer ON Report.CustomerId = Customer.Id
                       WHERE ReportDrivingSurvey.ReportId LIKE '%s%%'
                       --AND Customer.Name = '%s'
                       """ % (report_name.split('-')[-1], customer_name)

        n_retries = 40
        retry_delay = 30.0
        # sql_addr = '20.80.80.81'
        sql_addr = None
        rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

        # pull the surveys from the sql_database
        survey_ids += [(str(row[0]), str(row[1])) for row in rows]

    return survey_ids


def get_report_surveys_cust_time(creds, customer, start_time, end_time):
    query_string = """
                    SELECT Report.Id, Report.ProcessingCompleted, Report.ReportTitle, ReportDrivingSurvey.SurveyId,
                           Survey.Tag AS SurveyTag, SurveyModeType.Description as SurveyMode
                    FROM Report
                    LEFT JOIN ReportDrivingSurvey ON Report.Id = ReportDrivingSurvey.ReportId
                    LEFT JOIN Survey ON ReportDrivingSurvey.SurveyId = Survey.Id
                    LEFT JOIN SurveyModeType ON Survey.SurveyModeTypeId = SurveyModeType.Id
                    LEFT JOIN Customer ON Report.CustomerId = Customer.Id
                    WHERE Customer.Name = '%s'
                    AND ProcessingCompleted > '%s'
                    AND ProcessingCompleted < '%s'
                    AND ProcessingCompleted IS NOT NULL
                    ORDER BY ProcessingCompleted ASC
                    """ % (customer, start_time, end_time)

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    # pull the surveys from the sql_database
    sql_reports = [str(row[0]) for row in rows]

    return sql_reports


def get_survey_ids_cust_time(creds, customer, start_time, end_time):

    query_string = """
                   SELECT Survey.Id, Survey.Tag, SurveyModeType.Description, Survey.StabilityClass
                   FROM Survey
                   INNER JOIN SurveyModeType ON Survey.SurveyModeTypeId = SurveyModeType.Id
                   INNER JOIN Location ON Survey.LocationId = Location.Id
                   INNER JOIN Customer ON Location.CustomerId = Customer.Id
                   WHERE Customer.Name = '%s'
                   AND StartDateTime > '%s'
                   AND StartDateTime < '%s'
                   ORDER BY StartDateTime ASC
                   """ % (customer, start_time, end_time)

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    # pull the surveys from the sql_database
    sql_surveys = [(str(row[0]), str(row[1]), str(row[2]), str(row[3])) for row in rows]

    return sql_surveys


def get_survey_analyzer_time(creds, analyzer_id, epoch_time):

    query_string = """
                   SELECT Survey.Id
                   FROM Survey
                   WHERE Survey.AnalyzerId = '%s'
                   AND EndEpoch >= %f
                   AND StartEpoch <= %f
                   """ % (analyzer_id, epoch_time, epoch_time)

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    # pull the surveys from the sql_database
    sql_surveys = [str(row[0]) for row in rows]

    return sql_surveys


def get_survey_type_cust_time(creds, customer, survey_type, start_time, end_time):

    query_string = """
                   SELECT Survey.Id, Tag, SurveyModeType.Description
                   FROM Survey
                   INNER JOIN SurveyModeType ON Survey.SurveyModeTypeId = SurveyModeType.Id
                   INNER JOIN Location ON Survey.LocationId = Location.Id
                   INNER JOIN Customer ON Location.CustomerId = Customer.Id
                   WHERE Customer.Name = '%s'
                   AND StartDateTime >= '%s'
                   AND StartDateTime < '%s'
                   AND SurveyModeType.Description LIKE '%s'
                   ORDER BY StartDateTime ASC
                   """ % (customer, start_time, end_time, survey_type)
    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    # pull the surveys from the sql_database
    sql_surveys = [str(row[0]) for row in rows]

    return sql_surveys


def get_survey_info(creds, survey_id):

    query_string = """
                   SELECT Survey.AnalyzerId, Survey.StartEpoch, Survey.EndEpoch, SMT.Description, Survey.StabilityClass
                   FROM Survey
                   LEFT JOIN SurveyModeType SMT on Survey.SurveyModeTypeId = SMT.Id
                   WHERE Survey.Id = '%s'
                   """ % survey_id

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    return rows[0]


def get_survey_segments(creds, surveys_list):
    if type(surveys_list) is not list:
        raise TypeError('Survey segments query from SQL Server is expecting input of type '
                        'list of survey ids as strings')

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = sql.get_host()
    segments = [(-9999)]

    ctr = 0
    invalid_conn_flag = True
    while invalid_conn_flag:
        if ctr == n_retries:
            raise IndexError('Max number of retries reached %d. Could not connect to SQL Server at %s' %
                             (n_retries, sql_addr))
        ctr += 1
        try:
            # get the survey driving segments from the segments table
            # cursor_factory = CursorFactory(sql_addr, creds[0], creds[1], 'SurveyorProductionLS', '7.3')
            dal = surveyordal.SurveyorDal(sql.get_conn())

            # get the segments using the SurveyorDal
            segments = dal.get_segment_list(surveys_list)

            invalid_conn_flag = False
        except (_mssql.MSSQLDatabaseException, pymssql.OperationalError, TypeError):
            Logger.log.error('Connection to SQL Server at %s failed. Retry %d of %d, next retry in %.1f seconds' %
                  (sql_addr, ctr, n_retries, retry_delay))
            time.sleep(retry_delay)
            continue

    return segments


def get_segments_by_customer_time(creds, customer, start_time, end_time):

    query_string = """
                   SELECT Survey.StartEpoch, Survey.EndEpoch, Survey.StartDateTime, Survey.EndDateTime, 
                   Segment.SurveyId, SurveyModeType.Description, Survey.AnalyzerId, Survey.SurveyorUnitId, 
                   Customer.Name, Survey.Tag, [User].FirstName, [User].LastName, Segment.[Order], 
                   Segment.Mode, Segment.Shape.STAsText(),
                   [User].UserName, SU.Description, A.SerialNumber,
                   Segment.CarSpeedMaximum, Segment.WindSpeedMaximum
                   FROM Segment
                   LEFT JOIN Survey ON Segment.SurveyId = Survey.Id
                   LEFT JOIN SurveyorUnit SU on Survey.SurveyorUnitId = SU.Id
                   LEFT JOIN Analyzer A on SU.Id = A.SurveyorUnitId
                   INNER JOIN SurveyModeType ON Survey.SurveyModeTypeId = SurveyModeType.Id
                   INNER JOIN [User] ON Survey.UserId = [User].Id
                   INNER JOIN Customer ON [User].CustomerId = Customer.Id
                   WHERE Customer.Name = '%s'
                   AND Survey.StartDateTime >= '%s'
                   AND Survey.StartDateTime < '%s'
                   ORDER BY Survey.StartDateTime ASC
                   """ % (customer, start_time, end_time)

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    # pull the surveys from the sql_database
    sql_segments = []
    segment_dicts = []
    for row in rows:
        result_row = list(row)

        # # compute the length of the segment in meters
        # segment_len_m = segment_length_wgs84(row[-1])
        # # add the segment length to the result
        # result_row.append(segment_len_m)

        sql_segments.append(result_row)

        # convert the sql query row to a dictionary
        segment_dicts.append(segment_2_dict(sql_segments[-1]))

    return sql_segments, segment_dicts


def get_valid_peaks_by_customer_time(creds, customer, start_epoch, end_epoch):

    query_string = """
                    SELECT
                    Survey.StartDateTime, Survey.EndDateTime, SurveyModeType.Description, Survey.AnalyzerId,
                    Survey.SurveyorUnitId, Survey.Tag,
                    Customer.Name,
                    [User].FirstName, [User].LastName,
                    Peak.EpochTime, Peak.Amplitude, Peak.CH4, Peak.Position.STAsText(), Peak.Lisa.STAsText(),
                    Peak.LisaOpeningAngle, Peak.LisaBearing, Peak.WindDirectionStdDev, Peak.WindSpeedNorth,
                    Peak.WindSpeedEast, Peak.Sigma, Peak.GpsLatitude, Peak.GpsLongitude, Peak.SurveyId,
                    Peak.EthaneRatio,
                    CONCAT(Survey.AnalyzerId, '-', CONVERT(DECIMAL(15,3), Peak.EpochTime)) AS UuidStr
                    FROM Peak
                    LEFT JOIN Survey ON Peak.SurveyId = Survey.Id
                    INNER JOIN SurveyModeType ON Survey.SurveyModeTypeId = SurveyModeType.Id
                    INNER JOIN [User] ON Survey.UserId = [User].Id
                    INNER JOIN Customer ON [User].CustomerId = Customer.Id
                    WHERE Customer.Name = '%s'
                    AND Peak.EpochTime BETWEEN %s AND %s
                    AND PassedAutoThreshold = 1
                    AND SurvivedCollection = 1
                    AND (Disposition = 1 OR Disposition = 3)
                    ORDER BY Peak.EpochTime ASC
                    """ % (customer, start_epoch, end_epoch)

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    valid_peak_dicts = []
    for row in rows:
        valid_peak_dicts.append(peak_2_dict(row))

    return valid_peak_dicts


def get_valid_peaks_by_uuid(creds, peak_uuid):


    uuid_parse_list = peak_uuid.split('-')
    analyzer_id = '-'.join(uuid_parse_list[:-1])
    nominal_epoch_time = float(uuid_parse_list[-1])
    start_epoch = nominal_epoch_time - 0.01
    end_epoch = nominal_epoch_time + 0.01

    query_string = """
                    SELECT 
                    Survey.StartDateTime, Survey.EndDateTime, SurveyModeType.Description, Survey.AnalyzerId, 
                    Survey.SurveyorUnitId, Survey.Tag, 
                    Customer.Name,
                    [User].FirstName, [User].LastName,
                    Peak.EpochTime, Peak.Amplitude, Peak.CH4, Peak.Position.STAsText(), Peak.Lisa.STAsText(),
                    Peak.LisaOpeningAngle, Peak.LisaBearing, Peak.WindDirectionStdDev, Peak.WindSpeedNorth,
                    Peak.WindSpeedEast, Peak.Sigma, Peak.GpsLatitude, Peak.GpsLongitude, Peak.SurveyId, 
                    Peak.EthaneRatio,
                    CONCAT(Survey.AnalyzerId, '-', CONVERT(DECIMAL(15,3), Peak.EpochTime)) AS UuidStr
                    FROM Peak
                    LEFT JOIN Survey ON Peak.SurveyId = Survey.Id
                    INNER JOIN SurveyModeType ON Survey.SurveyModeTypeId = SurveyModeType.Id
                    INNER JOIN [User] ON Survey.UserId = [User].Id
                    INNER JOIN Customer ON [User].CustomerId = Customer.Id
                    WHERE Survey.AnalyzerId = '%s'
                    AND Peak.EpochTime BETWEEN %s AND %s
                    AND PassedAutoThreshold = 1
                    AND SurvivedCollection = 1
                    AND (Disposition = 1 OR Disposition = 3)
                    """ % (analyzer_id, start_epoch, end_epoch)

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    valid_peak_dict = []
    for row in rows:
        valid_peak_dict.append(peak_2_dict(row))

    if len(valid_peak_dict) != 1:
        raise ValueError('The input peak_uuid returned non unique results! for peak_uuid: %s; num_results: %d'
                         % (peak_uuid, len(valid_peak_dict)))

    return valid_peak_dict


def get_eq_report_sources_by_customer_time(creds, customer, start_epoch, end_epoch):

    query_string = """
                        SELECT ReportId, EmissionRank, EmissionRate, Latitude, Longitude, Address, NumberOfPeaks, NumberOfPasses, ReportTitle, AvgEpoch, DateStarted
                        FROM EQSource
                        LEFT JOIN (
                            SELECT EQSourceEQPeak.EQSourceReportId, EQSourceEQPeak.EQSourceEmissionRank, AVG(EQSourceEQPeak.EQPeakEpochTime) AS AvgEpoch
                            FROM EQSourceEQPeak
                            GROUP BY EQSourceReportId, EQSourceEmissionRank
                        ) esat
                        ON esat.EQSourceReportId = EQSource.ReportId AND esat.EQSourceEmissionRank = EQSource.EmissionRank
                        LEFT JOIN Report ON EQSource.ReportId = Report.Id
                        LEFT JOIN ReportType ON Report.ReportTypeId = ReportType.Id
                        LEFT JOIN ReportStatusType ON Report.ReportStatusTypeId = ReportStatusType.Id
                        LEFT JOIN Customer ON Report.CustomerId = Customer.Id
                        WHERE ReportType.Description LIKE 'Emissions'
                        AND ReportStatusType.Description LIKE 'Complete'
                        AND Name LIKE '%s'
                        AND AvgEpoch BETWEEN %s AND %s
                        ORDER BY AvgEpoch ASC
                        """ % (customer, start_epoch, end_epoch)

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    eq_source_report_dicts = []
    for row in rows:
        eq_source_report_dicts.append(report_eqsource_2_dict(row))

    return eq_source_report_dicts


def get_peak_user_name(creds, analyzer_id, peak_epoch_time):

    query_string = """
                    SELECT [User].FirstName, [User].LastName
                    FROM [User]
                    WHERE [User].Id = (SELECT Survey.UserId
                                           FROM Survey
                                           WHERE Survey.AnalyzerId='%s'
                                           AND %s >= Survey.StartEpoch
                                           AND %s <= Survey.EndEpoch)
                    """ % (analyzer_id, peak_epoch_time, peak_epoch_time)

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay)

    names = [(row[0], row[1]) for row in rows]
    if len(names) > 1:
        raise ValueError('Query should only return one username, but len(names) = %d' % len(names))

    return names[0]


def get_fleet_info(creds, survey_id):
    sql_fleet_data = """
                         SELECT Survey.Id, [User].UserName, SU.Description, A.SerialNumber
                         FROM Survey
                         LEFT JOIN [User] on Survey.UserId = [User].Id
                         LEFT JOIN SurveyorUnit SU on Survey.SurveyorUnitId = SU.Id
                         LEFT JOIN Analyzer A on SU.Id = A.SurveyorUnitId
                         WHERE Survey.Id = '{}'
                        """.format(survey_id)

    n_retries = 40
    retry_delay = 30.0
    # sql_addr = '20.80.80.81'
    sql_addr = None
    rows = sql_query_wrapper(sql_fleet_data, sql_addr, creds, n_retries, retry_delay)

    inv_boxes = []
    for box_data_sqlrow in rows:
        inv_boxes.append(box_data_sqlrow)

    return inv_boxes


def get_peaks_by_analyzer_time(analyzer_id, start_epoch, end_epoch):
    """ Query for Peak data tied with the given list `survey_ids`.

    Args:
        survey_ids (list<str>): List of Survey Ids.

    Returns:
        peaks (list<obj:Peak>): List of Peak objects.
    """
    sql_peak_query = """
                     SELECT CAST([Id] as varchar(50)) as 'Id'
                        ,[Amplitude]
                        ,CAST([AnalyzerId] as varchar(50)) as 'AnalyzerId'
                        ,[CH4]
                        ,[CarSpeedEast]
                        ,[CarSpeedNorth]
                        ,[ClassificationConfidence]
                        ,[Disposition]
                        ,[Distance]
                        ,[EpochTime]
                        ,[EthaneConcentrationSdev]
                        ,[EthaneRatio]
                        ,[EthaneRatioSdev]
                        ,[EthaneRatioUncertainty]
                        ,[EthyleneConcentrationSdev]
                        ,[EthyleneRatio]
                        ,[EthyleneRatioSdev]
                        ,[EthyleneRatioUncertainty]
                        ,[GpsLatitude]
                        ,[GpsLongitude]
                        ,[LineIntegral]
                        ,[Lisa].STAsText() as 'Lisa'
                        ,[MeasurementType]
                        ,[MethanePeakToPeak] as 'MethanePeaktoPeak'
                        ,[PipEnergy]
                        ,[PlumeCarSpeedMaximum]
                        ,[PlumeCarSpeedMedian]
                        ,[PlumeCarSpeedMinimum]
                        ,[PlumeEmissionRate]
                        ,[PlumeEmissionRateUncertainty]
                        ,[PlumeEpochStart]
                        ,[PlumeEpochEnd]
                        ,[PlumeWindSpeedMaximum]
                        ,[PlumeWindSpeedMedian]
                        ,[PlumeWindSpeedMinimum]
                        ,[PlumeWidth]
                        ,[ProbA]
                        ,[ProbX0]
                        ,[ShapeCorrelation]
                        ,[Sigma]
                        ,CAST([SurveyId] as varchar(50)) as 'SurveyId'
                        ,[VariationCoef]
                        ,[WindDirectionStdDev]
                        ,[WindSpeedEast]
                        ,[WindSpeedNorth]
                     FROM [dbo].[Peak] WITH (NOLOCK)
                     WHERE [AnalyzerId] = '%s'
                     AND [EpochTime] > %f
                     AND [EpochTime] < %f
                     """ % (analyzer_id, start_epoch, end_epoch)
    n_retries = 40
    retry_delay = 30.0
    peak_rows = sql_query_wrapper(sql_peak_query, None, None, n_retries, retry_delay, as_dict=True)

    return peak_rows


def get_peaks_by_analyzer_time_archive(analyzer_id, start_epoch, end_epoch):

    keys = ['EpochTime', 'Amplitude', 'CH4', 'CarSpeedNorth', 'CarSpeedEast',
            'WindDirectionStdDev', 'WindSpeedNorth', 'WindSpeedEast',
            'Sigma', 'Distance', 'GpsLatitude', 'GpsLongitude', 'PassedAutoThreshold',
            'EthaneRatio', 'EthaneConcentrationSdev', 'EthaneRatioSdev', 'EthyleneRatio',
            'EthyleneRatioSdev', 'Disposition', 'ClassificationConfidence',
            'CAST([SurveyId] as varchar(50)) as SurveyId',
            'CAST(Survey.AnalyzerId as varchar(50)) as AnalyzerId',
            'Lisa.STAsText() as Lisa',
            # 'SurvivedCollection', 'CarBearing'
            ]

    column_sql_str = ','.join(keys)

    sql_peak_archive_query = """
            SELECT %s
            FROM PeakArchive
            LEFT JOIN Survey ON PeakArchive.SurveyId = Survey.Id
            WHERE Survey.AnalyzerId = '%s'
            AND PeakArchive.EpochTime BETWEEN %s AND %s
            ORDER BY EpochTime ASC""" % (column_sql_str, analyzer_id, start_epoch, end_epoch)

    n_retries = 40
    retry_delay = 30.0
    peak_rows = sql_query_wrapper(sql_peak_archive_query, None, None, n_retries, retry_delay, as_dict=True)

    return peak_rows


def get_measurement_units(creds):
    sql_fleet_data = """
                         SELECT Id, Description
                         FROM ReadingUnitTypes
                        """

    n_retries = 40
    retry_delay = 30.0
    sql_addr = None
    rows = sql_query_wrapper(sql_fleet_data, sql_addr, creds, n_retries, retry_delay)

    decoder_values = {}
    for data_sqlrow in rows:
            decoder_values[data_sqlrow[0]] = data_sqlrow[1]

    return decoder_values


def get_leak_types(creds):
    sql_fleet_data = """
                         SELECT Id, Name
                         FROM LeakTypes
                        """

    n_retries = 40
    retry_delay = 30.0
    sql_addr = None
    rows = sql_query_wrapper(sql_fleet_data, sql_addr, creds, n_retries, retry_delay)

    decoder_values = {}
    for data_sqlrow in rows:
            decoder_values[data_sqlrow[0]] = data_sqlrow[1]

    return decoder_values
