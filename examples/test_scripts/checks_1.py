import pandas as pd

dict_checks = dict()

dict_checks["This check is for numbers being greater than 6"] = {
    "columns": ["a_number", "number_2"],
    "calc_condition": lambda df, col, **kwargs: df[col] <= 6,
    "long_description": lambda df, col, condition, **kwargs:
        "There are numbers less than or equal to 6",
    "index_position": lambda df, col, condition, **kwargs:
        pd.Series(False, df.index)
}

dict_checks["This check is for the column to be not null"] = {
    "columns": ['string'],
    "calc_condition": lambda df, col, **kwargs: df[col].isnull(),
    "long_description": lambda df, col, condition, **kwargs:
        f"The column `{col}` should not be null",
    "category": 'must be resolved'
}
