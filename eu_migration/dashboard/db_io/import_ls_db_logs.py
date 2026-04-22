from elasticsearch import Elasticsearch

from dashboard.db_io.prod_sql_queries import read_cred_file
from dashboard.data_io.load_txt_data import dlm_2_list
from dashboard.db_io.elastic_index_mappings import create_ls_log_index
from dashboard.db_io.elastic_import_interface import force_del_index, insert_dict_doc

if __name__ == "__main__":

    index_name = 'ls_db_logs'

    elastic_creds = read_cred_file('/home/ubuntu/cred/elasticdb.txt')


    log_file_dicts = dlm_2_list('/home/ubuntu/Desktop/realtime_update_logs/db_status/db_status_log.log', delimiter=', ')
    for idx in range(len(log_file_dicts)):
        log_file_dicts[idx]['db_size'] = log_file_dicts[idx]['db_size'].strip(' MB')
        log_file_dicts[idx]['available'] = log_file_dicts[idx]['available'].strip(' MB')

    # # read the data into a pandas dataframe
    # df = pd.read_csv('/home/ubuntu/Desktop/realtime_update_logs/db_status/db_status_log.log',  encoding='utf-8')
    # # replace NaN with python None type so that null can be insterted properly to the ES document
    # df1 = df.where((pd.notnull(df)), None)
    # # transform the data into a list if records (python dictionaries)
    # log_file_dicts = df1.to_dict('records')

    # Connect to the elastic cluster
    es = Elasticsearch(['https://20.80.30.92:9200'], http_auth=(elastic_creds[0], elastic_creds[1]), verify_certs=False)
    # es = Elasticsearch(['localhost:9200'], verify_certs=False)
    # delete the exiting index without user input
    res = force_del_index(es, index_name)
    # create the index
    res = create_ls_log_index(es, index_name, 5, 1)
    # insert documents to the index
    actions_list, n_success = insert_dict_doc(log_file_dicts, es, index_name, 500)
