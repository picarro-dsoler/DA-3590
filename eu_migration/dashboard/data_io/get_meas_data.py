import time
import _mssql
import pymssql
import os
from helper.logger import Logger
import pandas as pd

from dashboard.data_io.prod_data_procs import get_measurements
from dal import sql, dynammo
from clusterworker import surveyordal

from dashboard.common_modules.time_calcs import epoch_2_gmtstring

import boto3
from botocore.exceptions import ClientError


def get_survey_info(survey_id, n_retries, retry_delay):
    survey_details_params = []

    ctr = 0
    invalid_conn_flag = True
    while invalid_conn_flag:
        if ctr == n_retries:
            raise IndexError('Max number of retries reached %d. Could not connect to SQL Server at %s' %
                             (n_retries, sql.get_host()))
        ctr += 1
        try:
            # get the survey driving segments from the segments table
            # cursor_factory = CursorFactory(sql_addr, creds[0], creds[1], 'SurveyorProductionLS', '7.3')
            dal = surveyordal.SurveyorDal(sql.get_conn())

            # get the data needed to process EQ results
            survey_details_params = dal.get_survey_details(survey_id)

            invalid_conn_flag = False
        except (_mssql.MSSQLDatabaseException, pymssql.OperationalError, TypeError):
            Logger.log.error('Connection to SQL Server at %s failed. Retry %d of %d, next retry in %.1f seconds' %
                             (sql.get_host(), ctr, n_retries, retry_delay))
            time.sleep(retry_delay)
            continue
    return survey_details_params


def meas_dynamo_2_csv(sql_surveys, export_file_path):

    csv_file_names = []

    n_surveys = len(sql_surveys)

    idx = -1
    for survey_id, tag, mode, stab_class in sql_surveys:
        idx += 1
        Logger.log.info('%d percent complete' % round(100.0 * idx / n_surveys))

        survey_details = get_survey_info(survey_id, 40, 30.0)
        # check if start and end times are valid
        if (survey_details.end <= survey_details.start) and (abs(survey_details.end - survey_details.start) < 20):
            continue

        measurements = get_measurements(dynammo.get_conn(), None, survey_details)
        measurements = pd.DataFrame.from_dict(measurements)
        measurements['GMT'] = [epoch_2_gmtstring(x) for x in measurements['EpochTime']]
        measurements['CarSpeed'] = (measurements['CarSpeedNorth'] ** 2.0 + measurements['CarSpeedEast'] ** 2.0) ** 0.5
        measurements['AnalyzerId'] = len(measurements) * [survey_details.analyzer_id]
        measurements['SurveyId'] = len(measurements) * [survey_id]

        # analyzer_serial_num = get_analyzer_sn((None, None), survey_details.analyzer_id)

        time_stamp_file_name = epoch_2_gmtstring(survey_details.start).replace('.', '-').replace(' ', '-').replace(':',
                                                                                                                   '-')
        tag_file_name = tag.replace(',', '-').replace('/', '').replace('\\', '').replace('  ', '-').replace(' ', '-')
        measurements = measurements.sort_values(by='EpochTime', ascending=True)

        csv_file_names.append(
            os.path.join(export_file_path,
                         '%s_%s_%s.csv' %
                         (time_stamp_file_name, tag_file_name, survey_id)
                         )
        )

        measurements.to_csv(csv_file_names[-1],
                            columns=['GMT', 'EpochTime', 'SurveyId',
                                     'CH4', 'C2H6',
                                     'GpsLatitude', 'GpsLongitude',
                                     'CarSpeed',
                                     'WindSpeedNorth', 'WindSpeedEast'],
                            index=False
                            )
    Logger.log.info('100 percent complete')

    return csv_file_names


def upload_file(source_dir, file_name, bucket_name, target_folder, aws_cred, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: upload result if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    session = boto3.Session(
        aws_access_key_id=aws_cred[0],
        aws_secret_access_key=aws_cred[1],
    )

    # Upload the file
    s3_client = session.client('s3')
    try:
        # response = s3_client.upload_file(file_name, bucket, object_name)
        response = s3_client.upload_file(
            os.path.join(source_dir, object_name),
            bucket_name,
            '{}/{}'.format(target_folder, object_name)
        )
    except ClientError as e:
        Logger.log.error(e)
        return False
    return response
