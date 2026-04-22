import time

import _mssql
import pymssql

from dashboard.db_io.prod_sql_queries import read_cred_file
from workercommon.dal.cursorfactory import CursorFactory


def sql_query_wrapper(query_string, sql_addr, creds, n_retries, retry_delay):
    rows = [(-9999)]

    ctr = 0
    invalid_conn_flag = True
    while invalid_conn_flag:
        if ctr == n_retries:
            raise IndexError('Max number of retries reached %d. Could not connect to SQL Server at %s' %
                             (n_retries, sql_addr))
        ctr += 1
        try:
            cursor_factory = CursorFactory(sql_addr, creds[0], creds[1], 'SurveyorProductionLS', '7.3')

            with cursor_factory.get_cursor_pymssql() as cur:
                cur.execute(query_string)

                rows = cur.fetchall()
            invalid_conn_flag = False
        except (_mssql.MSSQLDatabaseException, pymssql.OperationalError, TypeError):
            # print(traceback.format_exc())
            # print('Connection to SQL Server at %s failed. Retry %d of %d, next retry in %.1f seconds' %
            #       (sql_addr, ctr, n_retries, retry_delay))
            time.sleep(retry_delay)
            continue

    return rows


if __name__ == "__main__":
    user_auth = read_cred_file('/home/ubuntu/cred/lsdb.txt')

    db_addr = '20.80.80.81'

    try:
        # mark the current time
        epoch_time = time.time()
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(epoch_time))

        # get teh most recent time from the analyzer heartbeat
        sql_report_data = '''
                             SELECT MAX(EventDateTime)
                             FROM AnalyzerHeartbeat
                          '''
        query_result = sql_query_wrapper(sql_report_data, db_addr, user_auth, 1, 0.0)
        most_recent_record_time = query_result[0][0].strftime('%Y-%m-%d %H:%M:%S')

        # get the database size
        query_result = sql_query_wrapper('EXEC sp_spaceused', '20.80.80.81', user_auth, 1, 1)
        db_size = query_result[0][1]
        unallocated_size = query_result[0][2]

        print('%s, %f, %s, %s, %s, %s, Online, 1' % (time_str, epoch_time, most_recent_record_time,
                                                     db_addr, db_size, unallocated_size))
    except (_mssql.MSSQLDatabaseException, pymssql.OperationalError, TypeError, IndexError):
        epoch_time = time.time()
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(epoch_time))

        most_recent_record_time = -9999
        db_size = -9999
        unallocated_size = -9999

        print('%s, %f, %s, %s, %s, %s, Offline, 0' % (time_str, epoch_time, most_recent_record_time,
                                                      db_addr, db_size, unallocated_size))
