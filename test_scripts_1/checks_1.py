dict_checks = dict()

# short_description is the key for the dict_checks

dict_checks['This check is for numbers being greater than 0'] = {
    'columns': ['a_number', 'number_2'],
    'check_condition': lambda df, col, **kwargs: df[col] > 0,
    # 'count_condition': lambda df, col, condition, **kwargs: condition.sum(),
    # 'index_position': lambda df, col, condition, **kwargs: df,
    'long_description': lambda df, col, condition, **kwargs: df,
    # 'relevant_columns': lambda df, col, **kwargs: df
}
