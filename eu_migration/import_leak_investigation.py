from shapely.geometry import Point
import numpy as np
import pandas as pd
from dashboard.data_io.load_txt_data import dlm_2_list
from dashboard.db_io.elastic_queries import es_conn_tlimit
from dashboard.db_io.elastic_index_mappings import create_investigation_index
from dashboard.db_io.elastic_import_interface import del_index, insert_dict_doc
from helper.config import Config
from helper.logger import Logger


def standardize_rename_keys(lisa_csv_filename):
    lisa_data_list = dlm_2_list(lisa_csv_filename, ',')
# def standardize_rename_keys(lisa_df):
#     lisa_data_list = lisa_df.to_dict(orient='records')

    # standardize/rename keys
    lisa_data_import = []
    for lisa_data in lisa_data_list:

        # create a copy of the iterator dictionary
        lisa_data_format = lisa_data.copy()

        # for key in lisa_data_format:
        #     if lisa_data_format[key] = np.nan

        # format the leak location into wkt when it is available
        if (lisa_data_format['LeakLongitude'] is not '') and (lisa_data_format['LeakLatitude'] is not ''):
            leak_lon = float(lisa_data_format['LeakLongitude'])
            leak_lat = float(lisa_data_format['LeakLatitude'])
            if (abs(leak_lon) <= 180.0) and (abs(leak_lat) <= 90.0):
                lisa_data_format['LeakCoord'] = Point((leak_lon, leak_lat)).wkt
                lisa_data_format['LeakCoordLatLon'] = '%.12f,%.12f' % (leak_lat, leak_lon)
                lisa_data_format['LeakCoordPoint'] = dict()
                lisa_data_format['LeakCoordPoint']['lon'] = leak_lon
                lisa_data_format['LeakCoordPoint']['lat'] = leak_lat
        del lisa_data_format['LeakLongitude']
        del lisa_data_format['LeakLatitude']

        # add an additional field for the investigation time in hours
        if lisa_data_format['LeakInvestigationSeconds'] is not '':
            lisa_data_format['InvestigationMinutes'] = float(lisa_data['LeakInvestigationSeconds']) / 60.0

        # split off the domain name in the user name field
        if lisa_data_format['LeakInvestigatorUserName'] is not None:
            lisa_data_format['UserName'] = lisa_data_format['LeakInvestigatorUserName'].split('@')[0]
        else:
            lisa_data_format['UserName'] = ''

        # convert the peak vertex location to wkt
        peak_lon = float(lisa_data_format['PeakLongitude'])
        peak_lat = float(lisa_data_format['PeakLatitude'])
        lisa_data_format['PeakCoord'] = Point((peak_lon, peak_lat)).wkt
        lisa_data_format['PeakCoordLatLon'] = '%.12f,%.12f' % (peak_lat, peak_lon)
        del lisa_data_format['PeakLongitude']
        del lisa_data_format['PeakLatitude']

        # leak per km
        if lisa_data_format['LeakFound'] == 'Found_Gas_Leak':
            if float(lisa_data_format['type1coverage']) > 0.0:
                lisa_data_format['leak_denisty_on_bdy'] = 1000.0 * 1.0 / float(lisa_data_format['type1coverage'])
            else:
                lisa_data_format['leak_denisty_on_bdy'] = 0.0
        else:
            lisa_data_format['leak_denisty_on_bdy'] = 0.0

        lisa_data_import.append(lisa_data_format)

    return lisa_data_import


def standardize_g2g_rename_keys(lisa_csv_filename):
    lisa_data_list = dlm_2_list(lisa_csv_filename, ',')

    # standardize/rename keys
    lisa_data_import = []
    for lisa_data in lisa_data_list:

        # create a copy of the iterator dictionary
        lisa_data_format = lisa_data.copy()

        # format the leak location into wkt when it is available
        if (lisa_data_format['xCoord'] != '') and (lisa_data_format['yCoord'] != ''):
            leak_lon = float(lisa_data_format['xCoord'])
            leak_lat = float(lisa_data_format['yCoord'])
            if (abs(leak_lon) <= 180.0) and (abs(leak_lat) <= 90.0):
                lisa_data_format['LeakCoord'] = Point((leak_lon, leak_lat)).wkt
                lisa_data_format['LeakCoordLatLon'] = '%.12f,%.12f' % (leak_lat, leak_lon)
                lisa_data_format['LeakCoordPoint'] = dict()
                lisa_data_format['LeakCoordPoint']['lon'] = leak_lon
                lisa_data_format['LeakCoordPoint']['lat'] = leak_lat
        del lisa_data_format['xCoord']
        del lisa_data_format['yCoord']


        if (lisa_data_format['PeakLongitude'] != '') and (lisa_data_format['PeakLatitude'] != ''):
            # convert the peak vertex location to wkt
            peak_lon = float(lisa_data_format['PeakLongitude'])
            peak_lat = float(lisa_data_format['PeakLatitude'])
            lisa_data_format['PeakCoord'] = Point((peak_lon, peak_lat)).wkt
            lisa_data_format['PeakCoordLatLon'] = '%.12f,%.12f' % (peak_lat, peak_lon)
        else:
            Logger.log.info(f"Invalid PeakLatitude value: {lisa_data_format['PeakLatitude']}, converting to NaN")
            Logger.log.info(f"Invalid PeakLongitude value: {lisa_data_format['PeakLongitude']}, converting to NaN")
            lisa_data_format['PeakCoord'] = np.nan
            lisa_data_format['PeakCoordLatLon'] = np.nan

        del lisa_data_format['PeakLongitude']
        del lisa_data_format['PeakLatitude']

        lisa_data_import.append(lisa_data_format)

    lisa_data_df = pd.DataFrame(lisa_data_import)
    cleaned_filename = lisa_csv_filename.replace('.csv', '_clean.csv')
    lisa_data_df.to_csv(cleaned_filename)

    return lisa_data_import

if __name__ == "__main__":
    Config.init('conf/analytics.conf')
    # index_name = 'toscana_leak_investigation'
    # lisa_data_filename = r'/eng-app-anders/utility_data/toscana_energia/toscana_leak_investigation_20191016.csv', ','
    # index_name = 'toscana_leak_investigation'
    # lisa_data_filename = r'/eng-app-anders/utility_data/toscana_data/toscana_leak_investigation_20191119.csv', ','
    index_name = 'toscana_leak_investigation'
    lisa_data_filename = r'/eng-app-anders/utility_data/toscana_data/toscana_leak_investigation_20191126.csv'

    # index_name = 'italgas_leak_investigation'
    # lisa_data_filename = r'/eng-app-anders/utility_data/italgas_data/italgas_leak_investigation_20191113.csv', ','
    # index_name = 'italgas_leak_investigation'
    # lisa_data_filename = r'/eng-app-anders/utility_data/italgas_data/italgas_leak_investigation_20191119.csv', ','
    # index_name = 'italgas_leak_investigation'
    # lisa_data_filename = r'/eng-app-anders/utility_data/italgas_data/italgas_leak_investigation_20191126.csv', ','

    # get the elastic search login credentials
    # elastic_creds = read_cred_file('/home/ubuntu/cred/elasticdb.txt')

    es = es_conn_tlimit(None, None, None, None)

    # standardize and rename columns for import into elasticsearch
    lisa_data_standard = standardize_rename_keys(lisa_data_filename)

    # delete the exiting index
    res = del_index(es, index_name, True)
    # create the index
    res = create_investigation_index(es, index_name, 1, 1)

    # insert documents to the index
    actions_list, n_success = insert_dict_doc(lisa_data_standard, es, index_name, 500)
