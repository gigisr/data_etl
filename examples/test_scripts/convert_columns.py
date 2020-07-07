import re

import pandas as pd

dict_convert = dict()


def func_string_to_int(df, col):
    s = df[col].copy()
    s = s.str.replace(',', '')  # thousand separators
    s = s.str.replace('%', '')  # percentage sign
    s = s.str.replace('£', '')  # pound stirling sign
    s = s.str.replace('$', '')  # dollar sign
    s = s.str.replace('€', '')  # euro sign
    s = s.str.replace('¥', '')  # yen sign
    s = s.astype(int)
    return s


def func_string_to_float(df, col):
    s = df[col].copy()
    s = s.str.replace(',', '')  # thousand separators
    s = s.str.replace('%', '')  # percentage sign
    s = s.str.replace('£', '')  # pound stirling sign
    s = s.str.replace('$', '')  # dollar sign
    s = s.str.replace('€', '')  # euro sign
    s = s.str.replace('¥', '')  # yen sign
    s = s.astype(float)
    return s


dict_convert['int'] = {
    'columns': lambda df, **kwargs: ['a_number'],
    'dtypes': ['int', 'float'],
    'functions': {
        1: lambda df, col, **kwargs: df[col].astype(int),
        2: lambda df, col, **kwargs: func_string_to_int(df, col),
        3: lambda df, col, **kwargs: df[col].astype(float),
        4: lambda df, col, **kwargs: func_string_to_float(df, col)
    }
}
dict_convert['float'] = {
    'columns': ['lat', 'lng'],
    'dtypes': ['float'],
    'functions': {
        1: lambda df, col, **kwargs: df[col].astype(float),
        2: lambda df, col, **kwargs: func_string_to_float(df, col)
    }
}
# TODO have a mash-up function that also takes care of Excel dates?
dict_convert['date'] = {
    'columns': ['date_1', 'date_2'],
    'dtypes': ['datetime'],
    'functions': {
        1: lambda df, col, *kwargs:
            pd.to_datetime(df[col], format='%Y-%m-%d %H:%M:%S')
    }
}


def func_string_format(df, col):
    s = df[col].copy()
    s_null = s.isnull()
    s = s.astype(str)
    s = s.str.strip()
    reg_ex = re.compile(' +')
    s = s.map(lambda x: re.sub(reg_ex, ' ', x))
    s.loc[s_null] = pd.np.nan
    return s


dict_convert['string'] = {
    'columns': ['string'],
    'dtypes': [],
    'functions': {
        1: lambda df, col, **kwargs: func_string_format(df, col)
    },
    'idx_function': lambda df, col, **kwargs: pd.Series(True, index=df.index)
}
