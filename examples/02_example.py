import pandas as pd

dict_cat_1_map = {
    'A': ['a', 'z'],
    'B': ['b'],
    'C': ['c'],
    'D': ['d'],
    'Y': ['y'],
    'Z': ['z']
}

dict_checks = {
    'Number should be greater than 0': {
        'calc_condition': lambda df, col, **kwargs: df['number'] <= 0
    },
    'Number should be greater than 2': {
        "columns": ['number'],
        'calc_condition': lambda df, col, **kwargs: df[col] <= 2,
        'category': 'severe'
    },
    'check values in list': {
        'columns': ['category_1'],
        'calc_condition': lambda df, col, **kwargs: ~df[col].isin(['A', 'B', 'C', 'D']),
        'long_description': lambda df, col, condition, **kwargs: 
            f"The invalid values are: {df.loc[~df[col].isin(['A', 'B', 'C', 'D'])][col].unique().tolist()}"
    },
    'The category_1 column can only map to certain values': {
        'calc_condition': lambda df, col, **kwargs: [
            item[1] not in dict_cat_1_map[item[0]] for item in 
            df[['category_1', 'category_2']].values.tolist()
        ],
        'check_condition': lambda df, col, condition, **kwargs: sum(condition) > 0,
        'count_condition': lambda df, col, condition, **kwargs: sum(condition),
        'index_position': lambda df, col, condition, **kwargs: pd.Series(condition),
        'relevant_columns': lambda df, col, condition, **kwargs: 'category_1, category_2',
        'long_description': lambda df, col, condition, **kwargs: (
            f"The values that have no mapping are: "
            f"{df.loc[pd.Series(condition)]['category_1'].unique().tolist()}"
        )
    }
}
