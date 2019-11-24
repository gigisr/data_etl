from datetime import datetime
import pickle

import pandas as pd
import numpy as np

from data_curation import DataCuration, Checks


var_cnv_1_start_time = datetime.now()
data_cnv_1 = DataCuration(var_cnv_1_start_time , 'test')
df_convert_issues = pd.DataFrame(
    [
        ('A', '1', '0.6', '2019-02-29'),
        ('B', '4.5', 'A', '2019-22-05'),
        ('C', '1', '5.6', '2018-12-17'),
        ('D', 'b', '15.9', '2019-09-31'),
        (5, '-8', '4.7', '2018-03-09')
    ],
    columns=['object', 'int', 'float', 'date']
)
data_cnv_1.set_table({'df_convert_issues.tsv': df_convert_issues})


def func_try_float_cnv(x):
    try:
        var = float(x)
    except:
        return True
    return False


def func_try_int_cnv(x):
    try:
        var = int(x)
    except:
        return True
    return False


def func_str_cnv(s):
    var_is_null_pre = s.isnull().sum()
    s_cnv = s.map(func_to_int).str.strip()
    var_is_null_post = s_cnv.isnull().sum()
    if var_is_null_post != var_is_null_pre:
        raise ValueError
    return s_cnv


def func_to_int(x):
    try:
        return int(x)
    except:
        return x


def func_try_str_cnv(s):
    var_is_null_pre = s.isnull().sum()
    s_cnv = s.map(func_to_int).str.strip()
    var_is_null_post = s_cnv.isnull().sum()
    return s != s_cnv


def func_try_date_cnv(x):
    if pd.isnull(x):
        return False
    if pd.isnull(pd.to_datetime(x, format='%Y-%m-%d', errors='coerce')):
        return True
    return False


dict_cnv_1 = {
    'float': {
        'columns': ['float'],
        'dtypes': ['float'],
        'functions': {
            1: lambda df, col, **kwargs: df[col].astype(float)
        },
        'idx_function': lambda df, col, **kwargs: df[col].map(func_try_float_cnv)
    },
    'int': {
        'columns': ['int'],
        'dtypes': ['int'],
        'functions': {
            1: lambda df, col, **kwargs: df[col].astype(int)
        },
        'idx_function': lambda df, col, **kwargs: df[col].map(func_try_int_cnv)
    },
    'object': {
        'columns': ['object'],
        'dtypes': [],
        'functions': {
            1: lambda df, col, **kwargs: func_str_cnv(df[col])
        },
        'idx_function': lambda df, col, **kwargs: func_try_str_cnv(df[col])
    },
    'date': {
        'columns': ['date'],
        'dtypes': ['date', '[ns]'],
        'functions': {
            1: lambda df, col, **kwargs: pd.to_datetime(
                df[col], format='%Y-%m-%d')
        },
        'idx_function': lambda df, col, **kwargs: df[col].map(func_try_date_cnv)
    }
}

df_cnv_1_expected_df_issues = pd.DataFrame(
    [
        ('test', 'None', 'None', 'df_convert_issues.tsv', np.nan, 0, np.nan, '',
         'The conversion failed to format float', 'float', 1, '1',
         var_cnv_1_start_time),
        ('test', 'None', 'None', 'df_convert_issues.tsv', np.nan, 0, np.nan, '',
         'The conversion failed to format int', 'int', 2, '1, 3',
         var_cnv_1_start_time),
        ('test', 'None', 'None', 'df_convert_issues.tsv', np.nan, 0, np.nan, '',
         'The conversion failed to format object', 'object', 1, '4',
         var_cnv_1_start_time),
        ('test', 'None', 'None', 'df_convert_issues.tsv', np.nan, 0, np.nan, '',
         'The conversion failed to format date', 'date', 3, '0, 1, 3',
         var_cnv_1_start_time)
    ],
    columns=['key_1', 'key_2', 'key_3', 'file', 'sub_file', 'step_number',
       'category', 'issue_short_desc', 'issue_long_desc', 'column',
       'issue_count', 'issue_idx', 'grouping']
)


def test_cnv_1():
    data_cnv_1.convert_columns(dictionary=dict_cnv_1)
    assert data_cnv_1.df_issues.fillna('').equals(
        df_cnv_1_expected_df_issues.fillna(''))


var_alter_1_start_time = datetime.now()
data_alter_1 = DataCuration(var_alter_1_start_time, 'test')

data_alter_1.set_table(
    {
        'df_alterations.tsv': pd.DataFrame(
            [
                ('A', 2, 'key_1'),
                ('B', 199, 'key_2'),
                ('C', -1, 'key_1'),
                ('D', 20, 'key_3'),
                ('E', 6, 'key_2')
            ],
            columns=['to_map', 'add_1', 'merge_key']
        ),
        'df_alterations_issues.tsv': pd.DataFrame(
            [
                ('A', 2, 'key_1'),
                ('B', 199, 2),
                ('C', -1, 'key_1'),
                (['D'], 'a', 'key_3'),
                ('E', 6, 'key_2')
            ],
            columns=['to_map', 'add_1', 'merge_key']
        )
    }
)


df_mapping = pd.DataFrame(
    [
        ('key_1', 1),
        ('key_2', 2),
        ('key_3', 3)
    ],
    columns=['merge_key', 'out_value']
)


def func_alter_merge(df, df_mapping):
    df_mapped = pd.merge(
        df,
        df_mapping,
        on='merge_key',
        how='left'
    )
    if (
        df_mapped['out_value'].isnull().sum() !=
        df['merge_key'].isnull().sum()
    ):
        raise ValueError
    return df_mapped


dict_alter_1 = {
    '01': {
        'type': 'new_col',
        'col_name': 'key',
        'function': lambda df, keys, **kwargs: keys[0]
    },
    '02': {
        'type': 'new_col',
        'col_name': 'done_add_1',
        'function': lambda df, keys, **kwargs: df['add_1'] + 1,
        'idx_function': lambda df, keys, **kwargs:
            df['add_1'].map(
                lambda x: type(x).__name__).map(
                lambda x: ('int' in x) | ('float' in x)).map(
                {True: False, False: True})
    },
    '03': {
        'type': 'new_col',
        'col_name': 'mapped',
        'function': lambda df, keys, **kwargs: df['to_map'].map({
            'A': 1, 'B': 2, 'C': 3, 'D': 4, 'E': 5}),
        'idx_function': lambda df, keys, **kwargs:
            ~df['to_map'].astype(str).isin(['A', 'B', 'C', 'D', 'E'])
    },
    '04': {
        'type': 'map_df',
        'function': lambda df, keys, **kwargs:
            func_alter_merge(df, kwargs['df_mapping']),
        'idx_function': lambda df, keys, **kwargs:
            ~df['merge_key'].isin(['key_1', 'key_2', 'key_3', np.nan])
    }
}

df_alter_1_expected_df_issues = pd.DataFrame(
    [
        ('test', 'None', 'None', 'df_alterations_issues.tsv', np.nan, 0, np.nan,
         '', 'For type new_col the function for alter_key 02 has not worked',
         'done_add_1', 1, '3', var_alter_1_start_time),
        ('test', 'None', 'None', 'df_alterations_issues.tsv', np.nan, 0, np.nan,
         '', 'For type new_col the function for alter_key 03 has not worked',
         'mapped', 1, '3', var_alter_1_start_time),
        ('test', 'None', 'None', 'df_alterations_issues.tsv', np.nan, 0, np.nan,
         '', 'For type map_df the function for alter_key 04 has not worked',
         np.nan, 1, '1', var_alter_1_start_time)
    ],
    columns=['key_1', 'key_2', 'key_3', 'file', 'sub_file', 'step_number',
       'category', 'issue_short_desc', 'issue_long_desc', 'column',
       'issue_count', 'issue_idx', 'grouping']
)


def test_alter_1():
    data_alter_1.alter_tables(dictionary=dict_alter_1, df_mapping=df_mapping)
    assert data_alter_1.df_issues.fillna('').equals(
        df_alter_1_expected_df_issues.fillna(''))
