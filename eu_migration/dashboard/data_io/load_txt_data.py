import numpy as np


def dlm_2_list(dlm_filename, delimiter):
    with open(dlm_filename, 'r') as fh:
        lines = fh.readlines()
    header = lines[0].strip('\r\n').replace('"', '').split(delimiter)
    data_dicts = []
    for line_num, line in enumerate(lines[1:]):
        line_values = line.strip('\r\n').replace('"', '').split(delimiter)
        line_dict = {}
        for n_col, key in enumerate(header):
            line_dict[key] = line_values[n_col]
        data_dicts.append(line_dict)

    return data_dicts


def dlm_2_dict(dlm_filename, delimiter):
    data_dicts = dlm_2_list(dlm_filename, delimiter)

    data_combined = {}
    for k in data_dicts[0].keys():
        data_combined[k] = list(d[k] for d in data_dicts)

    for key in data_combined:
        data_combined[key] = np.asarray(data_combined[key])
        try:
            data_combined[key] = data_combined[key].astype(float)
        except ValueError:
            continue

    return data_combined
