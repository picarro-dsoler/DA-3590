import sys
import logging.config
import datetime
from config import LOGGING_CONFIG, italgas_g2g_api_url_dict

# Configure the logger to output to the screen (console)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

from helpers import *

logger = logging.getLogger("ingest_leak_data")

args = parse_args(sys.argv[1:])
number_of_prior_days_given = args.number_of_prior_days

# ensure correct password used for the selected URL (i.e. 'production' or 'test')
url = italgas_g2g_api_url_dict["production"]

start_date = datetime(2025, 5, 17)
end_date = datetime.now()

tries = 0
max_tries = 3

while start_date < end_date and tries < max_tries:
    if tries == 0:
        time_window = start_date + timedelta(days =1)
    try:
        response = get_italgas_leak_updates(url, start_date, time_window)
        verify_request_success(response)
        response_dict = response.json()
        verify_nested_success(response_dict)
        leaks = response_dict.get("Response")
        number_of_leaks = len(leaks) if leaks else 0
        logger.info("GET request with last-modified date range %s to %s returned %d leak records", start_date.date(), (start_date + timedelta(days =1)).date(), number_of_leaks)
        tries = 0
        if leaks:
            leaks = create_primary_key_from_columns(leaks)

            verify_expected_leak_columns_received(leaks)

            leaks = remove_extra_columns_received(leaks)

            leaks = convert_column_types(leaks)

            verify_last_modified_times(start_date, time_window, leaks)

            leaks = select_last_modified_from_duplicates(leaks)

            leaks = add_picarro_last_updated_column(leaks)

            execute_many_string = prepare_sql_execute_many_string(leaks)
            leak_value_tuples = [tuple(leak.values()) for leak in leaks]

            log_existing_table_length()
            update_leak_database(execute_many_string, leak_value_tuples)
            log_updated_table_length()
            start_date = start_date + timedelta(days =1)

    except Exception as e:
        logger.error("Error getting leaks: %s", e)
        logger.error("Tries: %d", tries)
        time.sleep(5)
        tries += 1
if tries == max_tries:
    logger.error("Max tries reached for date range %s to %s", start_date.date(), (start_date + timedelta(days =1)).date())
