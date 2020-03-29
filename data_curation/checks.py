# Here we are defining a class that will deal with checking data sets
import logging
import importlib
import os
from inspect import getfullargspec

import pandas as pd
import numpy as np

module_logger = logging.getLogger(__name__)


class Checks:
    __step_no = 0
    __key_1 = None
    __key_2 = None
    __key_3 = None
    __grouping = None
    df_issues = None
    __key_separator = " -:- "
    __checks_defaults = {
        'columns': [np.nan],
        'check_condition':
            lambda df, col, condition, **kwargs: condition.sum() > 0,
        'count_condition': lambda df, col, condition, **kwargs: condition.sum(),
        'index_position': lambda df, col, condition, **kwargs: condition,
        'relevant_columns': lambda df, col, condition, **kwargs: col,
        'long_description': lambda df, col, condition, **kwargs: "",
        'idx_flag': True,
        'category': np.nan
    }

    def __init__(self, grouping, key_1, key_2=None, key_3=None):
        module_logger.info("Initialising `Checks` object")
        # Three keys, all good things come in threes
        self.__key_1 = str(key_1)
        self.__key_2 = str(key_2)
        self.__key_3 = str(key_3)
        self.__grouping = grouping
        # Initialise the `df_issues` table
        df_issues = pd.DataFrame(
            columns=[
                "key_1", "key_2", "key_3", "file", "sub_file", "step_number",
                "category", "issue_short_desc", "issue_long_desc", "column",
                "issue_count", "issue_idx", "grouping"
            ]
        )
        df_issues["step_number"] = df_issues["step_number"].astype(int)
        self.df_issues = df_issues
        module_logger.info("Initialising `Checks` object complete")

    def error_handling(self, file, subfile, issue_short_desc, issue_long_desc,
                       column, issue_count, issue_idx, category=np.nan):
        """
        If an error is handled, as they all should be, we need to specify what
        happens with the error. By putting it into a single function it will
        hopefully make the code briefer.
        """
        # TODO work out how to add in `file` and `subfile` where data is a
        #  dictionary
        module_logger.info("Logging an error with `error_handling`")
        df = self.df_issues.copy()
        list_vals = [
            self.__key_1, self.__key_2, self.__key_3, file, subfile,
            self.__step_no, category, issue_short_desc, issue_long_desc, column,
            issue_count, issue_idx, self.__grouping
        ]
        try:
            df.loc[df.shape[0]] = list_vals
            self.df_issues = df.copy()
        except:
            var_msg = f"Logging the issue failed, values: {list_vals}"
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        module_logger.info(f"Error logged: {list_vals}")

    def set_defaults(
            self, columns=None, check_condition=None, count_condition=None,
            index_position=None, relevant_columns=None, long_description=None,
            idx_flag=None):
        module_logger.info("Starting `set_defaults`")
        if columns is not None:
            if type(columns).__name__ != 'list':
                var_msg = 'The `columns` argument is not a list as required'
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            if len(columns) == 0:
                var_msg = ('The `columns` argument is empty, it needs to be '
                           'at least length 1, this can be a null')
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            self.__checks_defaults['columns'] = columns
        if check_condition is not None:
            self.__set_defaults_check(check_condition, 'check_condition')
            self.__checks_defaults['check_condition'] = check_condition
        if count_condition is not None:
            self.__set_defaults_check(count_condition, 'count_condition')
            self.__checks_defaults['count_condition'] = count_condition
        if index_position is not None:
            self.__set_defaults_check(index_position, 'index_position')
            self.__checks_defaults['index_position'] = index_position
        if relevant_columns is not None:
            self.__set_defaults_check(relevant_columns, 'relevant_columns')
            self.__checks_defaults['relevant_columns'] = relevant_columns
        if long_description is not None:
            self.__set_defaults_check(long_description, 'long_descriptions')
            self.__checks_defaults['long_description'] = long_description
        if idx_flag is not None:
            if idx_flag not in [True, False]:
                var_msg = 'The value of `idx_flag` need to be True or False'
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            self.__checks_defaults['idx_flag'] = idx_flag
        module_logger.info("Completed `set_defaults`")

    @staticmethod
    def __set_defaults_check(function, label):
        module_logger.info("Starting `__set_defaults_check`")
        if type(function).__name__ != 'function':
            var_msg = f'The passed value for `{label}` is not a function'
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        arg_spec = getfullargspec(function)
        if arg_spec.args != ['df', 'col', 'condition']:
            var_msg = (
                f'The arguments passed in for the function `{label}` does not '
                f'match with the required args: df, col, condition')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        if arg_spec.varkw != 'kwargs':
            var_msg = (f'The **kwargs argument has not been provided for '
                       f'`{label}` and is required')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        module_logger.info("Completed `__set_defaults_check`")

    def set_key_separator(self, separator):
        module_logger.info("Starting `set_key_separator`")
        if (type(separator).__name__ != "str") | (len(separator) == 0):
            var_msg = ("The argument `separator` for function "
                       "`set_key_separator` should be a string of length "
                       "greater than 0")
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        self.__key_separator = separator
        module_logger.info(f"Completed `set_key_separator`, the key separator "
                           f"is: {self.__key_separator}")

    def apply_checks(
            self, tables, script_name=None, path=None,
            object_name="dict_checks", dictionary=None, **kwargs):
        module_logger.info("Starting `apply_checks`")
        if (script_name is not None) & (object_name is not None):
            if not os.path.exists(os.path.join(path, f"{script_name}.py")):
                raise ValueError("The script does not exist")
            mod = importlib.import_module(script_name)
            dict_checks = getattr(mod, object_name)
        elif dictionary is not None:
            if type(dictionary).__name__ != "dict":
                var_msg = "The `dictionary` argument is not a dictionary"
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            dict_checks = dictionary
        else:
            var_msg = ("Either `dictionary` or both of `script_name` and "
                       "`path` need to be none null")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        if type(tables).__name__ == "dict":
            for table_key in tables.keys():
                for check_key in dict_checks.keys():
                    self.__apply_the_check(
                        tables[table_key], dict_checks[check_key], check_key,
                        table_key, **kwargs)
        elif type(tables).__name__ == "DataFrame":
            for check_key in dict_checks.keys():
                self.__apply_the_check(tables, dict_checks[check_key],
                                       check_key, np.nan, **kwargs)

        module_logger.info("Completed `apply_checks`")

    def __apply_the_check(
            self, df, dict_check_info, check_key, table_key, **kwargs):
        module_logger.info(f"Starting check `{check_key}`")
        var_required_keys = 0
        if "calc_condition" not in dict_check_info:
            var_required_keys += 1
            var_msg = "The check requires a value for key `calc_condition`"
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        func_calc_condition = dict_check_info["calc_condition"]
        func_long_description = (
            self.__checks_defaults['long_description'] if
            "long_description" not in dict_check_info else
            dict_check_info["long_description"])
        func_check_condition = (
            self.__checks_defaults['check_condition'] if
            "check_condition" not in dict_check_info else
            dict_check_info["check_condition"])
        list_columns = (
            self.__checks_defaults['columns'] if
            "columns" not in dict_check_info else
            dict_check_info["columns"])
        if type(list_columns).__name__ == 'str':
            list_columns = [list_columns]
        func_count_condition = (
            self.__checks_defaults['count_condition'] if
            "count_condition" not in dict_check_info else
            dict_check_info["count_condition"])
        func_index_position = (
            self.__checks_defaults['index_position'] if
            "index_position" not in dict_check_info else
            dict_check_info["index_position"])
        func_relevant_columns = (
            self.__checks_defaults['relevant_columns'] if
            "relevant_columns" not in dict_check_info else
            dict_check_info["relevant_columns"])
        var_idx_flag = (
            self.__checks_defaults['idx_flag'] if
            "idx_flag" not in dict_check_info else
            dict_check_info['idx_flag'])
        var_category = (
            self.__checks_defaults['category'] if
            "category" not in dict_check_info else
            dict_check_info['category'])
        if len(list_columns) == 0:
            var_msg = ('The `list_columns` value somehow has length 0, needs '
                       'to have at least one element, which can be `np.nan`')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        for col in list_columns:
            self.__evaluate_check(
                check_key, df, col, func_calc_condition,
                func_check_condition, func_count_condition, func_index_position,
                func_relevant_columns, func_long_description, var_idx_flag,
                var_category, table_key, **kwargs)

        module_logger.info(f"Completed check `{check_key}`")

    def __evaluate_check(
            self, check_key, df, col, func_calc_condition, func_check_condition,
            func_count_condition, func_index_position, func_relevant_columns,
            func_long_description, var_idx_flag, var_category, table_key,
            **kwargs):
        module_logger.info(
            f"Starting evaluating check `{check_key}` for column {col}")
        s_calc_condition = func_calc_condition(df, col, **kwargs)
        var_check_condition = func_check_condition(
            df, col, s_calc_condition, **kwargs)
        var_count_condition = func_count_condition(
            df, col, s_calc_condition, **kwargs)
        s_index_conditions = func_index_position(
            df, col, s_calc_condition, **kwargs)
        if var_idx_flag is False:
            s_index_conditions = s_index_conditions.map(
                {True: False, False: True})
        var_relevant_columns = func_relevant_columns(
            df, col, s_calc_condition, **kwargs)
        var_long_description = func_long_description(
            df, col, s_calc_condition, **kwargs)
        if type(var_long_description).__name__ != "str":
            var_msg = (
                f"The variable `var_long_description` is not a string! It is a"
                f" {type(var_long_description).__name__}")
            module_logger.warning(var_msg)
        if (
            (type(var_relevant_columns).__name__ != "str") &
            (pd.isnull(var_relevant_columns) is False)
        ):
            var_msg = (
                f"The variable `var_relevant_columns` is not a string or null! "
                f"It is a {type(var_relevant_columns).__name__}")
            module_logger.warning(var_msg)
        if "int" not in type(var_count_condition).__name__:
            var_msg = (
                f"The variable `var_count_condition` is not an integer! It is a"
                f" {type(var_count_condition).__name__}")
            module_logger.warning(var_msg)
        if type(s_calc_condition).__name__ != "Series":
            var_msg = (
                f"The variable `s_calc_condition` is not a Series! It is a "
                f"{type(s_calc_condition).__name__}")
            module_logger.warning(var_msg)
        if type(s_index_conditions).__name__ != "Series":
            var_msg = (
                f"The variable `s_index_conditions` is not a Series! It is a "
                f"{type(s_index_conditions).__name__}")
            module_logger.warning(var_msg)
        if (
            (type(var_category).__name__ != 'str') &
            (pd.isnull(var_category) is False)
        ):
            var_msg = (f'The variable `category` is not a string or null! It '
                       f'is a {type(var_category).__name__}')
            module_logger.warning(var_msg)
        if var_check_condition:
            if pd.isnull(table_key):
                var_file = np.nan
                var_subfile = np.nan
            else:
                var_file = table_key.split(self.__key_separator)[0]
                var_subfile = (table_key.split(self.__key_separator)[1] if
                               self.__key_separator in table_key else np.nan)
            self.error_handling(
                var_file, var_subfile, check_key, var_long_description,
                var_relevant_columns, var_count_condition,
                ", ".join(
                    [
                        str(item) for item in
                        s_index_conditions.loc[
                            s_index_conditions].index.tolist()
                    ]
                ),
                var_category
            )
        module_logger.info(
            f"Completed evaluating check `{check_key}` for column {col}")

    def get_issue_count(self, issue_number_min=None, issue_number_max=None):
        module_logger.info("Starting `get_issue_count`")
        df = self.df_issues.copy()
        if issue_number_min is not None:
            df = df.loc[df["step_number"] >= issue_number_min].copy()
        if issue_number_max is not None:
            df = df.loc[df["step_number"] <= issue_number_max].copy()
        var_count = df.shape[0]
        module_logger.info("Completed `get_issue_count`")
        return var_count

    def table_look(self, table, issue_idx):
        module_logger.info("Starting `table_look`")
        if issue_idx not in self.df_issues.index.tolist():
            var_msg = (f"The requested issue index, {issue_idx}, is not "
                       f"present in the `df_issues` table")
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        if type(table).__name__ != 'DataFrame':
            var_msg = 'The `table` argument is not a DataFrame as required'
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        df_check = table.loc[
            [
                int(item) for item in
                self.df_issues.loc[issue_idx, "issue_idx"].split(", ")
            ]
        ]
        module_logger.info("Completed `table_look`")
        return self.df_issues.loc[[issue_idx]], df_check

    def set_step_no(self, step_no):
        """
        Set the step number, this allows errors to be recorded against a
        specific step which in turn can help with issue tracking and checking
        once issues are recorded.

        The argument step_no needs to be convertible to integer format.
        """
        module_logger.info("Starting `set_step_no`")
        try:
            self.__step_no = int(step_no)
        except ValueError:
            var_msg = (f"Function set_step_no: The value {step_no} can not be "
                       f"converted to int.")
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        module_logger.info(
            f"Completed `set_step_no`, the step number is {self.__step_no}")

    def get_step_no(self):
        module_logger.info("Starting `get_step_no`")
        module_logger.info("Completed `get_step_no`")
        return self.__step_no
