import pandas as pd

dict_alter = dict()

dict_alter['01'] = {
    'type': 'new_col',
    'col_name': 'number_2',
    'function': lambda df, keys, **kwargs: df['a_number'] * 2
}
dict_alter['02'] = {
    'type': 'new_col',
    'col_name': 'key_1',
    'function': lambda df, keys, **kwargs: keys[0]
}
dict_alter['03'] = {
    'type': 'new_col',
    'col_name': 'key_2',
    'function': lambda df, keys, **kwargs: keys[1]
}
dict_alter['04'] = {
    'type': 'map_df',
    'function': lambda df, keys, **kwargs: df,
    'idx_function': lambda df, keys, **kwargs: pd.Series(True, index=df.index)
}
