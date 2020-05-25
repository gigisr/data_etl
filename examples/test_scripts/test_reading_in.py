# This file contains the information required for listing files and reading in
# tables of data
import os

import pandas as pd


def list_the_files(path):
    list_files = os.listdir(path)
    list_files = [os.path.abspath(os.path.join(path, x)) for x in list_files]
    list_files = [x for x in list_files if '.xlsx' in x.lower()]
    list_files = [x for x in list_files if '~' not in x.lower()]
    list_files = [x for x in list_files if 'header' not in x.lower()]
    return list_files


def read_files(list_files):
    dict_files = dict()
    for file in list_files:
        xl = pd.ExcelFile(file)
        for sheet in xl.sheet_names:
            df = xl.parse(
                sheet_name=sheet, dtype=str, keep_default_na=False, header=None)
            key = '{} -:- {}'.format(
                file.split('\\')[-1].lower().replace('.xlsx', ''), sheet)
            dict_files[key] = df.copy()
    return dict_files


def read_headers(filepath):
    if not os.path.exists(filepath):
        raise ValueError(
            'The passed file path does not exist: {}'.format(filepath))
    dict_headers = dict()
    file = pd.ExcelFile(filepath)
    dict_headers['ideal_headers'] = file.parse(
        'IdealHeaders', header=None).values.tolist()[0]
    for sheet in [sheet for sheet in
                  file.sheet_names if sheet != 'IdealHeaders']:
        df_header = file.parse(sheet, header=None)
        dict_headers[sheet] = {
            'expected_headers': df_header[
                df_header[0] == 'Header'].iloc[:, 1:].values.tolist()[0],
            'new_headers': df_header[
                df_header[0] == 'New name'].iloc[:, 1:].values.tolist()[0],
            'remove': df_header[
                df_header[0] == 'Remove'].iloc[:, 1:].values.tolist()[0],
            'notes': df_header[
                df_header[0] == 'Notes'].iloc[:, 1:].values.tolist()[0]
        }
    return dict_headers


def link_headers(dfs, df_headers):
    dict_link = dict()
    for key_df in dfs.keys():
        for key_header in df_headers.keys():
            check_shape = (
                # + 1 because the headers have an index to explain the
                # row purposes
                dfs[key_df].shape[1] + 1 == df_headers[key_header].shape[1])
            if check_shape is True:
                dict_link[key_df] = str(key_header)
                break
    return dict_link
