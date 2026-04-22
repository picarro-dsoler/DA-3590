import sys
import logging.config
import datetime
from leak_ingester_config import LOGGING_CONFIG, italgas_g2g_api_url_dict

# Configure the logger to output to the screen (console)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

from leak_data_ingest_helpers import (
    get_italgas_leak_updates,
    convert_column_types,
    verify_request_success,
    verify_nested_success,
    get_start_day_and_today_date_times,
    verify_last_modified_times,
    verify_expected_leak_columns_received,
    remove_extra_columns_received,
    create_primary_key_from_columns,
    prepare_sql_execute_many_string,
    add_picarro_last_updated_column,
    update_leak_database,
    log_existing_table_length,
    log_updated_table_length,
    select_last_modified_from_duplicates,
    parse_args
)

logger = logging.getLogger("ingest_leak_data")

args = parse_args(sys.argv[1:])
number_of_prior_days_given = args.number_of_prior_days

# ensure correct password used for the selected URL (i.e. 'production' or 'test')
url = italgas_g2g_api_url_dict["production"]

if __name__ == '__main__':
    start_date, today = get_start_day_and_today_date_times(days_prior_to_today=number_of_prior_days_given)

    logger.info("Starting the process for Italgas G2G Leak Data Ingester")
    response = get_italgas_leak_updates(url, start_date, today)

    logger.info("GET request returned size of %d bytes", len(response.content))

    verify_request_success(response)

    response_dict = response.json()

    verify_nested_success(response_dict)

    leaks = response_dict.get("Response")

    number_of_leaks = len(leaks) if leaks else 0
    logger.info("GET request with last-modified date range %s to %s returned %d leak records", start_date.date(),
                today.date(),
                number_of_leaks)

    if leaks:
        leaks = create_primary_key_from_columns(leaks)

        verify_expected_leak_columns_received(leaks)

        leaks = remove_extra_columns_received(leaks)

        leaks = convert_column_types(leaks)

        verify_last_modified_times(start_date, today, leaks)

        leaks = select_last_modified_from_duplicates(leaks)

        leaks = add_picarro_last_updated_column(leaks)

        execute_many_string = prepare_sql_execute_many_string(leaks)
        leak_value_tuples = [tuple(leak.values()) for leak in leaks]

        log_existing_table_length()
        update_leak_database(execute_many_string, leak_value_tuples)
        log_updated_table_length()
