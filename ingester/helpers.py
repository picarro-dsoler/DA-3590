import argparse
import logging
import os
import copy
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple
import sqlite3
import dateutil.parser
import requests

os.chdir('/home/sandbox/personal-repos/DA-3590/eu_migration/italgas_g2g_leak_data_ingester')
from config import EXPECTED_LEAK_COLUMNS, DATE_COLUMNS, DATABASE_PATH

logger = logging.getLogger('ingest_leak_data.helpers')


def get_italgas_leak_updates(api_url: str, earliest_date: datetime, latest_date: datetime) -> requests.Response:
    """
    Requests the leak records that have been modified (and/or added) by Italgas between yesterday and today.

    :param api_url: production, pre-production, or test URL
    :param earliest_date:
    :param latest_date:
    :return: requests.Response object containing modified leak records from Italgas
    """
    logger.info("Attempting to request leaks from Gas-2-Go API.")

    earliest_date_str = earliest_date.strftime('%Y-%m-%d')
    latest_date_str = latest_date.strftime('%Y-%m-%d')

    payload = {"dataInizio": earliest_date_str, "dataFine": latest_date_str}

    try:
        with requests.Session() as session:
            response = session.get(
                api_url,
                auth=(
                    'Picarro',
                    'UGljYXJyb1Byb2Q='
                ),
                params=payload
            )

    except requests.exceptions.ConnectionError as e:
        logger.info("failed attempt to request leaks (with payload=%s) from %s", str(payload),
                    api_url)
        logger.exception(e)
        raise e

    return response


def verify_request_success(response: requests.Response) -> None:
    if response.status_code != 200:
        logger.error("Response status code: %d, %s", response.status_code, response.reason)
        raise response.raise_for_status()


def verify_nested_success(response_dict: dict) -> None:
    """
    For the inner Italgas HTTP status code that can differ from the top-level response status code.
    :param response_dict: JSON from requests' response.
    :return: None
    """
    italgas_outcome_dict = response_dict["Esito"]  # 'Esito' means outcome
    italgas_status_code = italgas_outcome_dict["codice"]  # 'codice' means code
    italgas_description = italgas_outcome_dict["descrizione"]  # 'descrizione' means description

    if italgas_status_code != "200":
        logger.error("Bad nested status code. Received code %s with description: %s", italgas_status_code,
                     italgas_description)


def create_primary_key_from_columns(leaks: List[Dict]) -> List[Dict]:
    """
    Since there is no single unique column and numProgressivo resets every year, this function extracts the year
    from dataInsermiento (in UTC) and prepends it to numProgessivo to create the primary key 'leakId'.
    Leaks with bad (one that can't convert to an int) or key components are of little use and are discarded.
    :param leaks:
    :return: leaks with only good primary keys.
    """
    leaks = copy.deepcopy(leaks)
    for i, leak in enumerate(leaks):
        numProgressivo = leak.get("numProgressivo")
        dataInserimento = leak.get("dataInserimento")
        if numProgressivo and dataInserimento:
            try:
                insertion_year = dateutil.parser.isoparse(dataInserimento).replace(tzinfo=timezone.utc).year
                leaks[i]["leakId"] = int(str(insertion_year) + str(numProgressivo).zfill(10))
            except ValueError:
                del leaks[i]
                logger.error("Could not create surrogate primary key from leak record: %s.", leak)
        else:
            del leaks[i]
            logger.error("Data missing. Could not create surrogate primary key from leak record: %s.", leak)
    return leaks


def verify_expected_leak_columns_received(leaks: List[Dict]) -> None:
    """
    Log when a response includes less columns than expected.
    :param leaks:
    :return: None
    """
    missing_columns = {}

    for leak in leaks:
        leak_id = leak['leakId']
        received_columns = set(leak.keys())

        missing_columns_from_received_leak = EXPECTED_LEAK_COLUMNS - received_columns
        if missing_columns_from_received_leak:
            missing_columns_key_value = {leak_id: missing_columns_from_received_leak}
            missing_columns.update(missing_columns_key_value)

    if missing_columns:
        logger.error("Response is missing columns {leakId : {missing columns}}: %s", missing_columns)


def remove_extra_columns_received(leaks: List[Dict]) -> List[Dict]:
    leaks = copy.deepcopy(leaks)
    extra_columns = set()

    for i, leak in enumerate(leaks):
        received_columns = set(leak.keys())
        extra_columns_received = received_columns - EXPECTED_LEAK_COLUMNS

        for extra_column in extra_columns_received:
            if extra_column != "leakId":
                del leaks[i][extra_column]
                extra_columns.add(extra_column)

    if extra_columns:
        logger.info("Removed %d extra columns from received leaks.",
                    len(extra_columns))
    return leaks


def convert_column_types(leaks: List[Dict]) -> List[Dict]:
    """
    Dates are stored as INTEGER epoch seconds timestamps, coordinates as REAL, and primary key as INTEGER
    :param leaks:
    :return: leaks with type conversions
    """
    leaks = copy.deepcopy(leaks)

    for i, leak in enumerate(leaks):
        leak["numProgressivo"] = int(leak["numProgressivo"])

        try:
            leak["xCoord"] = float(leak.get("xCoord"))
        except TypeError:
            logger.error("leak ID %d has no xCoord", leak["leakId"])
            leak["xCoord"] = None
        except ValueError:
            logger.error("could not convert leak ID %d xCoord", leak["leakId"])
            leak["xCoord"] = None

        try:
            leak["yCoord"] = float(leak.get("yCoord"))
        except TypeError:
            logger.error("leak ID %d has no yCoord", leak["leakId"])
            leak["yCoord"] = None
        except ValueError:
            logger.error("could not convert leak ID %d yCoord", leak["leakId"])
            leak["yCoord"] = None

        for date_column in DATE_COLUMNS:
            try:
                leak[date_column] = convert_iso_date_to_epoch_seconds(leak.get(date_column))
            except TypeError:
                logger.error("leak ID %d has no %s column", leak["leakId"], date_column)
                leak[date_column] = None
            except ValueError:
                logger.error("could not convert leak ID %d %s", leak["leakId"], date_column)
                leak[date_column] = None

        leaks[i] = leak
    return leaks


def verify_last_modified_times(earliest_day: datetime, latest_day: datetime, leaks: List[Dict]) -> None:
    """
    Ensure GET request provides records in the right window of time.
    :param earliest_day: Leak records should not have been modified before this time.
    :param latest_day: Leak records should not have been modified after this time.
    :param leaks: Leak records from Italgas server.
    :return: None
    """
    for leak in leaks:
        leak_id = leak['leakId']
        try:
            last_modified_date = datetime.fromtimestamp(leak["dataUltimaMod"], timezone.utc).date()
        except TypeError:
            logger.error("leak ID %d 'dataUltimaMod' missing so could not verify last modified time", leak_id)
            continue

        if last_modified_date < earliest_day.date():
            logger.error("'dataUltimaMod' for leak ID %s too early", leak_id)
        elif latest_day.date() < last_modified_date:
            logger.error("'dataUltimaMod' for leak ID %s too early", leak_id)


def add_picarro_last_updated_column(leaks: List[Dict]) -> List[Dict]:
    """
    Add column to each leak dict to be able to record when the leak was last modified in the Picarro database.
    :param leaks:
    :return: leaks with additional column
    """
    leaks = copy.deepcopy(leaks)
    for i in range(len(leaks)):
        leaks[i]["picarroLastUpdated"] = int(datetime.utcnow().replace(tzinfo=timezone.utc).timestamp())
    return leaks


def prepare_sql_execute_many_string(leaks: List[Dict]) -> str:
    """
    Produces SQL string:

    INSERT OR REPLACE INTO leaks(sistema, numProgressivo, lisa, codStato, statoFoglietta, ...
    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ...

    Where the columns in each dictionary in the given list of leaks are verified to be in the same order.
    This is to be used in conjunction with a list of tuples of leak values in a SQL INSERT OR REPLACE statement.
    :param leaks:
    :return: SQL string
    """
    leak_columns = list(leaks[0].keys())
    leak_columns_str = ", ".join(leak_columns)
    question_marks = ["?"] * len(leak_columns)
    question_marks_str = ", ".join(question_marks)
    for leak in leaks:
        assert list(leak.keys()) == leak_columns  # ensure same key order

    execute_many_string = f"INSERT OR REPLACE INTO leaks({leak_columns_str}) values ({question_marks_str})"
    return execute_many_string


def update_leak_database(execute_many: str, leak_values: List[Tuple]):
    """
    Executes the execute_many statement below, given a list of tuples of leak_values.

    INSERT OR REPLACE INTO leaks(sistema, numProgressivo, lisa, codStato, statoFoglietta, ...
    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ...

    :param execute_many:
    :param leak_values:
    :return: None
    """
    try:
        con = sqlite3.connect(DATABASE_PATH)
        with con:
            _ = con.executemany(
                execute_many, leak_values
            )
    except sqlite3.OperationalError as e:
        logger.exception(e)
    except sqlite3.IntegrityError as e:
        logger.exception(e)


def log_existing_table_length():
    row_count = count_leak_table_rows()
    logger.info("Before update, table had %d rows.", row_count)


def log_updated_table_length():
    row_count = count_leak_table_rows()
    logger.info("After update, table has %d rows.", row_count)


def select_last_modified_from_duplicates(leaks: List[Dict]) -> List[Dict]:
    """
    Italgas will return leaks having both the same numProgressivo and the same dataInserimento, hence resulting in
    duplicate leak IDs (primary keys). This function selects the most recently updated, based on dataUltimaMod from
    those duplicates.

    :param leaks: containing duplicate leak IDs (primary keys)
    :return: leaks: with duplicates removed (only the most recently updated leak remains)
    """
    leaks = copy.deepcopy(leaks)
    primary_keys = [int(leak["leakId"]) for leak in leaks]
    duplicated_leak_ids = find_duplicate_primary_keys(primary_keys)

    unique_leaks = [leak for leak in leaks if leak["leakId"] not in duplicated_leak_ids]
    duplicate_leaks = [leak for leak in leaks if leak["leakId"] in duplicated_leak_ids]

    de_duplicated_leaks = de_duplicate_leaks(duplicate_leaks, duplicated_leak_ids)

    cleaned_leaks = unique_leaks + de_duplicated_leaks
    logger.info("Total leaks after selecting last modified from duplicate leak IDs: %d", len(cleaned_leaks))
    return cleaned_leaks


def de_duplicate_leaks(duplicate_leaks, duplicated_leak_ids):
    de_duplicated_leaks = []
    for duplicate_leak_id in duplicated_leak_ids:
        same_leaks = [leak for leak in duplicate_leaks if leak["leakId"] == duplicate_leak_id]

        latest_data_ultima_mod_index = 0
        latest_data_ultima_mod_epoch = 0
        for i, duplicate_leak in enumerate(same_leaks):
            leak_last_modified_epoch = duplicate_leak["dataUltimaMod"]
            if leak_last_modified_epoch > latest_data_ultima_mod_epoch:
                latest_data_ultima_mod_index = i
                latest_data_ultima_mod_epoch = leak_last_modified_epoch
        de_duplicated_leaks.append(same_leaks[latest_data_ultima_mod_index])
    return de_duplicated_leaks


def find_duplicate_primary_keys(primary_keys):
    seen = {}
    dupes = []
    for x in primary_keys:
        if x not in seen:
            seen[x] = 1
        else:
            if seen[x] == 1:
                dupes.append(x)
            seen[x] += 1
    return dupes


def count_leak_table_rows():
    try:
        con = sqlite3.connect(DATABASE_PATH)
        with con:
            cursor = con.execute(
                "SELECT COUNT(*) FROM leaks;"
            )
    except sqlite3.OperationalError as e:
        logger.exception(e)
    except sqlite3.IntegrityError as e:
        logger.exception(e)

    results = cursor.fetchall()

    if len(results) > 1:
        logger.error("Too many rows returned from table row count. Expected 1 row.")

    row_count = results[0][0]
    return row_count


def get_start_day_and_today_date_times(days_prior_to_today) -> Tuple[datetime, datetime]:
    today = datetime.now(tz=timezone.utc)  # UTC is 1 or 2 hours behind Italy and should be OK
    # today = datetime(year=2024, month=4, day=27, tzinfo=timezone.utc)
    start_day = today - timedelta(days=days_prior_to_today)
    # start_day = datetime(year=2024, month=4, day=15, tzinfo=timezone.utc)
    return start_day, today


def convert_iso_date_to_epoch_seconds(iso_date: str) -> int:
    epoch_seconds = int(
        dateutil.parser.isoparse(iso_date).replace(tzinfo=timezone.utc).timestamp()) if iso_date else None
    return epoch_seconds


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description='Specify number of days prior to today to include in G2G API leak GET request.')
    parser.add_argument('--number_of_prior_days', '-n',
                        default=12, type=int, choices=range(1, 14),
                        help=(
                            "Specify the number of days prior to today to include in GET request. "
                            "More than 13 usually causes the remote connection to close before completing request. "
                            "Default is 12."))
    parser.add_argument('--start_date', '-s',
                        default=datetime.now().strftime('%Y-%m-%d'), type=str,
                        help=(
                            "Specify the start date to include in GET request. "
                            "Default is today."))
    return parser.parse_args(args)
