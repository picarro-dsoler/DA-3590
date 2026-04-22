# Install the smartsheet sdk with the command: pip install smartsheet-python-sdk
import smartsheet
import numpy as np
import pandas as pd
from dashboard.db_io.prod_sql_queries import read_cred_file
from helper.config import Config


def load_smartsheet_report(api_key, report_id, num_records):
    # Initialize client. Uses the API token in the environment variable "SMARTSHEET_ACCESS_TOKEN"
    smart = smartsheet.Smartsheet(access_token=api_key)
    # Make sure we don't miss any error
    smart.errors_as_exceptions(True)

    # Log all calls
    # logging.basicConfig(filename='rwsheet.log', level=logging.INFO)

    # Load entire report
    report = smart.Reports.get_report(report_id=report_id, page_size=num_records)

    headers = [x.title for x in report.columns]
    report_data = {}
    for header in headers:
        report_data[header] = list()

    for row in report.rows:
        for col_n, cell in enumerate(row.cells):
            report_data[headers[col_n]].append(cell.value)

    for key in report_data:
        report_data[key] = np.asarray(report_data[key])

    return report_data


def load_box_report(file_path, report_data):

    # Convert report_data to DataFrame if it is a dictionary
    if isinstance(report_data, dict):
        report_data = pd.DataFrame(report_data)

    sheet_names = ['Italgas', 'ToscanaEnergia']

    for sheet in sheet_names:
        try:
            box_df = pd.read_excel(file_path, sheet_name=sheet)
        except Exception as e:
            raise ValueError(f"Error reading the Excel sheet '{sheet}': {e}")

        # Modify CustomerName for ToscanaEnergia sheet
        if sheet == 'ToscanaEnergia':
            box_df.loc[box_df['CustomerName'] == 'ToscanaEnergia', 'CustomerName'] = 'Toscana Energia'

        # Ensure necessary columns are present
        required_columns = {'ReportLabelFinal', 'ReportName', 'CustomerName'}
        if not required_columns.issubset(box_df.columns):
            raise ValueError(f"Sheet '{sheet}' must contain the following columns: {required_columns}")

        # Filter rows where ReportLabelFinal is 'Final Checkbox'
        box_df_filtered = box_df[box_df['ReportLabelFinal'] == 'Final Checkbox']

        # Extract relevant columns
        client = box_df_filtered['CustomerName']
        finalreportpcubed = box_df_filtered['ReportName']
        region = box_df_filtered['BoundaryName']
        city = box_df_filtered['BoundaryRegion']

        # Create intermediate DataFrame
        intermediate_df = pd.DataFrame({'client': client, 'finalreportpcubed': finalreportpcubed, 'region': region,
                                        'city': city})

        # Create sets of existing and new combinations
        existing_combinations = set(zip(report_data['client'], report_data['finalreportpcubed']))
        new_combinations = set(zip(intermediate_df['client'], intermediate_df['finalreportpcubed']))

        # Get unique combinations
        unique_combinations = new_combinations - existing_combinations

        # Filter the intermediate DataFrame based on unique combinations
        mask = intermediate_df.apply(lambda x: (x['client'], x['finalreportpcubed']) in unique_combinations, axis=1)
        filtered_df = intermediate_df[mask]

        # Concatenate the filtered DataFrame with the report_data
        report_data = pd.concat([report_data, filtered_df], ignore_index=True)

    return report_data.to_dict(orient='list')


if __name__ == '__main__':
    Config.init('conf/analytics.conf')
    # read the credential filename
    # TODO: revist
    dev_key = read_cred_file('/home/ubuntu/cred/smartsheet.txt')
    r_id = 3006949269759876
    report_data = load_smartsheet_report(dev_key[0], r_id, 1000000)

