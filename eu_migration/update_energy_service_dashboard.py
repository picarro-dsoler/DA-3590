import datetime
import os
import pandas as pd
import collections
import numpy as np

os.chdir('/home/sandbox/personal-repos/DA-3590/eu_migration')
from dashboard.db_io.prod_sql_queries import read_cred_file
from dashboard.db_io.elastic_queries import es_conn_tlimit
from dashboard.db_io.elastic_index_mappings import create_investigation_index
from dashboard.db_io.elastic_index_mappings import create_italgas_g2g_index
from dashboard.db_io.elastic_import_interface import force_del_index, insert_dict_doc

from picarro_investigation_app_data import get_investigation_app_data
from picarro_investigation_app_data import get_gas2go_data
from picarro_investigation_app_data import get_smartsheet_reports_in_g2g
from picarro_investigation_app_data import merge_g2g_and_surveyor_data
from import_leak_investigation import standardize_rename_keys
from import_leak_investigation import standardize_g2g_rename_keys

from smartsheet_data_access import load_smartsheet_report
from helper.config import Config
from helper.logger import Logger
if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.realpath(__file__))
    utility_data_dir = os.path.join(base_dir, 'utility_data')
    if not os.path.isdir(utility_data_dir):
        os.makedirs(utility_data_dir)
    Config.init(os.path.join(os.path.dirname(base_dir), 'conf', 'analytics.conf'))
    # =====================================
    customer_name_list = [
                        #  ('retipiu', 'retipiu_leak_investigation', 'smartsheet'),
                        # ('depa', 'depa_leak_investigation', 'smartsheet'),
                        ('toscana', 'toscana_leak_investigation', 'smartsheet'),
                        #  ('apreti', 'apreti_leak_investigation', 'smartsheet'),
                        # ('2irete', '2irete_leak_investigation', 'smartsheet'),
                        #  # ('grdf', 'grdf_leak_investigation', 'smartsheet'),
                        #  ('unareti', 'unareti_leak_investigation', 'smartsheet'),
                        #  ('reti-mt', 'retimt_leak_investigation', 'smartsheet'),
                        #  ('acegas', 'acegas_leak_investigation', 'smartsheet'),
                        #  ('aemme', 'aemme_leak_investigation', 'smartsheet'),
                        #  ('onegas',
                        #  'onegas_leak_investigation',
                        #  r'/eng-app-anders/utility_data/onegas_data/final_reports_20200717.csv'),
                           ('italgas', 'italgas_g2g_leak_investigation', 'gas2go')
                          # ('italgas', 'italgas_leak_investigation', 'smartsheet')
                         # ('gp', 'gp_leak_investigation', 'smartsheet')
                         ]


    # =====================================

    for customer_name_common, customer_index_name, final_report_source in customer_name_list:
        Logger.log.info('Updating data for %s in Index %s' % (customer_name_common, customer_index_name))
        smartsheet_report_id = 3006949269759876  # report id for teh service tracking master document

        # get the elastic search login credentials
        elastic_creds = read_cred_file('/home/ubuntu/cred/elasticdb.txt')

        now_date_str = datetime.datetime.now().strftime('%Y%m%d')
        if final_report_source != 'gas2go':
            output_filename = os.path.join(utility_data_dir, '%s_leak_investigation_%s.csv' % (customer_name_common, now_date_str))
        else:
            output_filename = os.path.join(utility_data_dir, '%s_leak_investigation_%s.csv' % (final_report_source, now_date_str))
            smartsheet_output_filename = os.path.join(utility_data_dir, '%s_smartsheet_%s.csv' % (final_report_source, now_date_str))
        # read the credential filename
        # TODO: revist
        smartsheet_key = read_cred_file('/home/ubuntu/cred/smartsheet.txt')

        if final_report_source != 'gas2go':
            if final_report_source == 'smartsheet':
                report_data = load_smartsheet_report(smartsheet_key[0], smartsheet_report_id, 1000000)

            else:
                df = pd.read_csv(final_report_source)
                report_data = df.to_dict('records')
                result = collections.defaultdict(list)
                for d in report_data:
                    for k, v in d.items():
                        result[k].append(v)
                result = dict(result)
                for key in result:
                    result[key] = np.asarray(result[key])
                report_data = result.copy()

            leak_data, csv_file = get_investigation_app_data(customer_name_common, report_data,
                                                             True, output_filename)
        else:
            Logger.log.info('Initiating Gas2Go ETL')

            report_data = load_smartsheet_report(smartsheet_key[0], smartsheet_report_id, 1000000)
            report_data = pd.DataFrame(report_data)
            # report_data.to_csv(smartsheet_output_filename, index=False)

            g2g_leak_data, csv_file = get_gas2go_data(output_filename)
            # g2g_leak_data.to_csv(output_filename_temp)
            g2g_leak_data, report_data = get_smartsheet_reports_in_g2g(g2g_leak_data, report_data)

            leak_data, csv_file = get_investigation_app_data(customer_name_common, report_data,
                                                             True, output_filename)

            # merge Surveyor data with G2G data
            italgas_leak_data, csv_file = merge_g2g_and_surveyor_data(g2g_leak_data, leak_data, output_filename)

        es = es_conn_tlimit(None, None, None, None)        

        if csv_file != '' and final_report_source != 'gas2go':
            # standardize and rename columns for import into elasticsearch
            lisa_data_standard = standardize_rename_keys(csv_file)


            # # delete the exiting index without user input
            # res = force_del_index(es, customer_index_name)
            # # create the index
            # res = create_investigation_index(es, customer_index_name, 1, 1)
            #
            # # insert documents to the index
            # actions_list, n_success = insert_dict_doc(lisa_data_standard, es, customer_index_name, 500)

        elif csv_file != '' and final_report_source == 'gas2go':

            Logger.log.info('Starting Elastic pre-processing')

            # standardize and rename columns for import into elasticsearch
            lisa_data_standard = standardize_g2g_rename_keys(csv_file)

            # Logger.log.info('Writing Gas2Go data to csv')

            # lisa_data_standard.to_csv(csv_file, index=False)

            # # delete the exiting index without user input
            # res = force_del_index(es, customer_index_name)
            #
            # Logger.log.info('Creating Gas2Go ES Index')
            # # create the index
            # res = create_italgas_g2g_index(es, customer_index_name, 1, 1)
            #
            # Logger.log.info('Inserting Gas2Go documents into index')
            # # insert documents to the index
            # actions_list, n_success = insert_dict_doc(lisa_data_standard, es, customer_index_name, 500)
        else:
            Logger.log.info('No data available for customer %s' % customer_name_common)

            # # delete the exiting index without user input
            # res = force_del_index(es, customer_index_name)
