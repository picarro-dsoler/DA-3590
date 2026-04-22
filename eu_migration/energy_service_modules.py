import datetime
from dashboard.db_io.prod_sql_queries import get_measurement_units, get_leak_types


def format_investigation_time(inv_box_result, sql_row):
    # format the found time string to datestring and euro date string
    if inv_box_result[3] is not None:
        foundtimestr = inv_box_result[3].strftime('%Y-%m-%d %H:%M:%S')
        foundtimestr_eu = inv_box_result[3].strftime('%d-%m-%Y')
        # calculate the time between running the report and doing the investigation
        time_to_investigation = (inv_box_result[3].timestamp() - inv_box_result[11]) / 3600.0 / 24.0

    else:
        # some leaks don't have a FoundDateTime value which screws up the kibana index pattern time filter reference,
        # without a FoundDate time value they can't be displayed on the dashoboard
        # this checks if FoundDateTime is missing and fill it in with the timestamp from the Report
        foundtimestr = sql_row['ReportDate'] + ' 00:00:00'
        reportdate_obj = datetime.datetime.strptime(foundtimestr, '%Y-%m-%d %H:%M:%S')
        foundtimestr_eu = reportdate_obj.strftime('%d-%m-%Y')
        time_to_investigation = -1.0

    # format the time when the investigation was completed
    try:
        investigation_complete_time_str = inv_box_result[26].strftime('%Y-%m-%d %H:%M:%S')
    except AttributeError:
        investigation_complete_time_str = foundtimestr

    return foundtimestr, foundtimestr_eu, time_to_investigation, investigation_complete_time_str


def format_investigation_status(investigation_status_raw):
    if investigation_status_raw is None:
        investigation_status = 'No Status'

        return investigation_status
    else:
        return investigation_status_raw


def format_investigation_notes(notes_raw):
    if notes_raw is not None:
        notes = notes_raw.replace(',', ' ').replace('\r', ' ').replace('\n', ' ')
    else:
        notes = ''

    return notes


def qc_investigtion_record(inv_box_meta,
                           leak_grade_aliases, other_leak_grade_list,
                           leak_type_aliases, leak_types_meta, other_leak_types_list,
                           ag_alias_list, bg_alias_list,
                           leak_location_aliases, leak_location_meta,
                           leak_source_meta,
                           readings_aliases, customer_name = None):

    leak_grade_alias = list((leak_grade_aliases & set(inv_box_meta.keys())))

    # format the leak grade field to remove typos, and check if it exists in the template
    leakgrade = 'No Grade'
    if len(leak_grade_alias) == 1:

        leakgrade = inv_box_meta[leak_grade_alias[0]]
        if leakgrade is not None:
            leakgrade = leakgrade.upper()
            leakgrade = leakgrade.replace(" ", "")
            leakgrade = leakgrade.replace('"', "")
            leakgrade = leakgrade.replace("''", "")

            # handle GRDF grade classifications starting with type
            if leakgrade.startswith('Type'):
                leakgrade = leakgrade[-1]

        else:
            leakgrade = 'No Grade'

    if leakgrade not in other_leak_grade_list:
        leakgrade = 'No Grade'

    # format the free form leak location remarks to remove commas and other stuff,
    # and check if it exists in the template
    locationremarks = ''
    if 'Leak Location Remarks' in inv_box_meta.keys() and inv_box_meta['Leak Location Remarks'] is not None:
        locationremarks = inv_box_meta['Leak Location Remarks']. \
            replace(',', ' '). \
            replace('\r', ' '). \
            replace('\n', ' ')

    leak_type_alias = list((leak_type_aliases & set(inv_box_meta.keys())))

    # format the ag/bg leak type, also check if it exists in the template
    leaktype = ''
    if len(leak_type_alias) == 1:
        if leak_type_alias[0] in inv_box_meta.keys():
            try:
                leaktype = leak_types_meta[int(inv_box_meta[leak_type_alias[0]])].strip()
            except ValueError or KeyError:
                leaktype = inv_box_meta[leak_type_alias[0]].strip()

        if leaktype not in list(leak_types_meta.values()) + other_leak_types_list:
            leaktype = 'NC'
    else:
        leaktype = 'NC'

    # reformat possible aliases for above ground and below ground
    if leaktype in ag_alias_list:
        leaktype = 'Above_Ground'
    if leaktype in bg_alias_list:
        leaktype = 'Below_Ground'

    leak_location_alias = list((leak_location_aliases & set(inv_box_meta.keys())))

    # format the leak location description, and also check if it exists in the template
    leaklocation = ''
    if len(leak_location_alias) == 1:
        if leak_location_alias[0] in inv_box_meta.keys():
            try:
                if int(inv_box_meta[leak_location_alias[0]]) in leak_location_meta:
                    leaklocation = leak_location_meta[int(inv_box_meta[leak_location_alias[0]])]
                else:
                    leaklocation = inv_box_meta[leak_location_alias[0]]
            except ValueError:
                leaklocation = inv_box_meta[leak_location_alias[0]]
    else:
        leaklocation = 'NC'

    # format the leak source description for 'Other Source' templates, and also check if it exists in the template
    leaksource = ''
    if 'Source' in inv_box_meta.keys():
        try:
            leaksource = leak_source_meta[int(inv_box_meta['Source'])]
        except ValueError:
            leaksource = inv_box_meta['Source']

    city = ''
    if 'City RetiPiu' in inv_box_meta.keys():
        city = str(inv_box_meta['City RetiPiu']).replace(',', '')
    # if (customer_name == "DEPA") and ('City' in inv_box_meta.keys()):
    #         city = str(inv_box_meta['City']).replace(',', '')



    # 'Street Number', 'Apartment Number', 'Street Name', 'City', 'State'
    address = {'Street Number': '',
               'Apartment Number': '',
               'Street Name': '',
               'City': '',
               'State': ''}
    for key in address:
        if key in inv_box_meta:
            if inv_box_meta[key] is not None:
                address[key] = inv_box_meta[key]. \
                                   replace(',', ' '). \
                                   replace('\r', ' '). \
                                   replace('\n', ' ')

    # 'Surface Reading', 'Reading Unit', 'Barhole Reading'
    sensor_readings = {'SurfaceReading': 'surface_reading_alias',
                       'ReadingUnitSurface': 'surface_unit_alias',
                       'BarholeReading': 'barhole_reading_alias',
                       'ReadingUnitBarhole': 'barhole_unit_alias'}

    for reading_key in sensor_readings.keys():
        alias_list = readings_aliases[sensor_readings[reading_key]].split(', ')
        for alias_key in alias_list:
            if alias_key in inv_box_meta:
                if inv_box_meta[alias_key] is not None:
                    sensor_readings[reading_key] = inv_box_meta[alias_key]. \
                                                       replace(',', ' '). \
                                                       replace('\r', ' '). \
                                                       replace('\n', ' ')

        # check if the Picarro default field was used, and lookup description from coded value domain table
        if 'Unit' in reading_key:
            try:
                unit_coded_value = int(sensor_readings[reading_key])
                value_decoder = get_measurement_units(None)
                sensor_readings[reading_key] = value_decoder[unit_coded_value]

            except ValueError:
                pass

        if sensor_readings[reading_key].endswith('_alias'):
            sensor_readings[reading_key] = ''

    # get instrument type
    insttype = ''
    instsn = ''
    for key in inv_box_meta:
        if key.lower().endswith('instrument type'):
            insttype = inv_box_meta[key].replace(',', ' ')
        if key.lower().endswith('instrument s/n'):
            instsn = inv_box_meta[key].replace(',', ' ')

    return leakgrade, locationremarks, leaktype, leaklocation, leaksource, address, sensor_readings, insttype, instsn, city
