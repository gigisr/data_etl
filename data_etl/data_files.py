# Here we are defining a class that will deal with all the data storage and
# manipulations
import os
import logging
import importlib

import pandas as pd
import numpy as np

module_logger = logging.getLogger(__name__)


class DataCuration:
    __step_no = 0
    df_issues = None
    headers = None
    __key_1 = None
    __key_2 = None
    __key_3 = None
    __grouping = None
    tables = dict()
    formed_tables = dict()
    list_files = list()
    __key_separator = " -:- "
    __link_headers = dict()

    def __init__(self, grouping, key_1, key_2=None, key_3=None):
        """
        All data actions are taken on all tables, the aim is to process data to
        end up with a uniform data set that can be utilised and is consistent.

        The three arguments are individual identifiers for the data.

        The end form would be a pipeline that has regular data ingests.
        """
        module_logger.info("Initialising `DataCuration` object")
        # Three keys, all good things come in threes
        self.__key_1 = str(key_1)
        self.__key_2 = str(key_2)
        self.__key_3 = str(key_3)
        self.__grouping = grouping
        # sub_file, e.g. sheet for a spreadsheet, may not always be applicable
        df_issues = pd.DataFrame(
            columns=[
                "key_1", "key_2", "key_3", "file", "sub_file", "step_number",
                "category", "issue_short_desc", "issue_long_desc", "column",
                "issue_count", "issue_idx", "grouping"
            ]
        )
        df_issues["step_number"] = df_issues["step_number"].astype(int)
        self.df_issues = df_issues
        module_logger.info("Initialising `DataCuration` object complete")

    def error_handling(self, file, subfile, issue_short_desc, issue_long_desc,
                       column, issue_count, issue_idx, category=np.nan):
        """
        If an error is handled, as they all should be, we need to specify what
        happens with the error. By putting it into a single function it will
        hopefully make the code briefer.
        """
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
            var_msg = f"Logging the issue failed for values: {list_vals}"
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        module_logger.info(f"Error logged: {list_vals}")

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

    def set_key_separator(self, separator):
        """
        The key separator is used in the error handling section to split out the
        file and sub file portions of the dictionary keys of the files read in.

        So if you have a key of 'file name -:- sheet name', for tables read in
        from an Excel file, and an issue is found. The associated issues log
        entry will then have a file value of 'file name' and a sub file value of
        'sheet name'.
        """
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

    def __import_attr(self, path, script_name, attr_name):
        if (path is None) | (path == '.'):
            mod = importlib.import_module(script_name)
        else:
            var_script_path = os.path.join(path, f"{script_name}.py")
            if not os.path.exists(var_script_path):
                var_msg = f"The script does not exist: {script_name}.py"
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            spec = importlib.util.spec_from_file_location(
                script_name, var_script_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
        attr = getattr(mod, attr_name)

        return attr

    def set_file_list(self, list_files, append=False):
        """
        If there is a know list of files then define them here rather than
        setting a function to find the files.
        """
        module_logger.info("Starting `set_file_list`")
        var_type = type(list_files).__name__
        if (var_type != "list") & (var_type != "str"):
            var_msg = ("The type of the `list_files` argument is not a list or "
                       "a string.")
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        elif var_type == "str":
            if len(list_files) == 0:
                var_msg = ("The length of the `list_files` argument is 0, it "
                           "needs to be a valid value.")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            list_files = [list_files]
        elif var_type == 'list':
            if len(list_files) == 0:
                var_msg = ("The length of the `list_files` argument is 0, it "
                           "needs to be a valid value.")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            list_files = list_files
        else:
            var_msg = (f"Unhandled type for function `set_file_list`: "
                       f"{var_type}")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        if append:
            self.list_files += list_files
        else:
            self.list_files = list_files
        module_logger.info(f"Completed `set_file_list`, the list of files is: "
                           f"{self.list_files}")

    def find_files(self, function=None, path=None, script_name=None,
                   func_name="list_the_files", files_path='.', append=False,
                   **kwargs):
        """
        Using an externally defined function, as specified in the module
        argument script, acquire a list of files to be read in.

        In the case that we want to accumulate a list of files from different
        main paths there is an append option.
        """
        module_logger.info("Starting `find_files`")
        # TODO move this to an internal function as it's used so often!
        if script_name is not None:
            function = self.__import_attr(path, script_name, func_name)
        elif function is not None:
            if type(function).__name__ != "function":
                var_msg = "The `function` argument needs to be a function"
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        else:
            var_msg = ("One of `script_name` or `function` needs to be not "
                       "None in the function `find_files`")
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        list_files = function(files_path, **kwargs)
        # TODO move these to be calls on the self.set_file_list function instead
        #  of setting the value here
        if append:
            self.list_files += list_files
        else:
            self.list_files = list_files
        module_logger.info(
            f"Completed `find_files`, the list of files is: {self.list_files}")

    def reading_in(self, function=None, path=None, script_name=None,
                   func_name="read_files", overwrite=True, **kwargs):
        """
        Using an externally defined reading in function, and the internally
        defined list of files, read in each of the tables required.

        `path` being the relative script file path
        """
        module_logger.info("Starting `reading_in`")
        if type(self.tables).__name__ != "dict":
            var_msg = ("The tables need to be in dictionary format for this "
                       "`self.reading_in` step")
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        if function is not None:
            if type(function).__name__ != "function":
                var_msg = ("The function passed to `self.reading_in` is not a "
                           "function.")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif script_name is not None:
            function = self.__import_attr(path, script_name, func_name)
        else:
            var_msg = ("One of the `function` or `script_name` arguments needs "
                       "to be completed. And if `script name is then `path` "
                       "needs to be too.")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        try:
            dfs = function(self.list_files, **kwargs)
        except AttributeError:
            if len([x for x in kwargs.keys()]) > 0:
                var_msg = (f"Function reading_in, kwargs may have been passed "
                           f"when the function {func_name} in the script "
                           f"{script_name} does not take kwargs")
            else:
                var_msg = (f"Function reading in: The {func_name} function "
                           f"does not exist in the {script_name} script.")
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        if overwrite is False:
            df_org = self.tables.copy()
            df_org.update(dfs)
        elif overwrite is True:
            pass
        else:
            var_msg = ("The attribute `overwrite` in the function "
                       "`reading_in` needs to be `True` or `False`")
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        self.set_table(dfs, overwrite=overwrite)
        if type(dfs).__name__ == "DataFrame":
            module_logger.info(f"The table has shape '{dfs.shape}'")
        else:
            for key in dfs:
                module_logger.info(
                    f"The table with key '{key}' has shape '{dfs[key].shape}'")

        module_logger.info("Completed `reading_in`")

    def set_table(self, tables, dict_key=None, overwrite=True):
        """
        If self.tables is a dictionary set df to key else overwrite existing
        table if argument is True
        """
        module_logger.info("Starting `set_table`")
        if (overwrite is True) & (dict_key is None):
            self.tables = tables
        elif (
            (overwrite is True) &
            (dict_key is not None) &
            (type(self.tables).__name__ == 'dict') &
            (type(tables).__name__ == 'DataFrame')
        ):
            self.tables[dict_key] = tables
        elif (
            (overwrite is False) &
            (dict_key is not None) &
            (type(self.tables).__name__ == 'dict') &
            (type(tables).__name__ == 'DataFrame')
        ):
            if dict_key not in [key for key in self.tables.keys()]:
                self.tables[dict_key] = tables
            else:
                var_msg = (
                    f'The combination of attributes has resulted in no change: '
                    f'`self.tables` type - {type(self.tables).__name__}, '
                    f'`tables` type - {type(tables).__name__}, `dict_key` - '
                    f'{dict_key}, `overwrite` - {overwrite}')
                module_logger.error(var_msg)
                raise AttributeError(var_msg)
        else:
            var_msg = (
                f'The combination of attributes has resulted in no change: '
                f'`self.tables` type - {type(self.tables).__name__}, `tables` '
                f'type - {type(tables).__name__}, `dict_key` - {dict_key}, '
                f'`overwrite` - {overwrite}')
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        module_logger.info("Completed `set_table`")

    def concatenate_tables(self):
        """
        Where the tables are in a dictionary format put them into a DataFrame
        """
        module_logger.info("Starting `concatenate_tables`")
        if type(self.tables).__name__ != "dict":
            var_msg = ("For the function `concatenate_tables` the `tables` "
                       "should be in dictionary format")
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        if len([key for key in self.tables.keys()]) > 1:
            df = pd.concat(self.tables, axis=1)
        elif len([key for key in self.tables.keys()]) == 1:
            dict_df = self.tables.copy()
            dict_key = [key for key in dict_df.keys()][0]
            df = dict_df[dict_key].copy()
            df['level_0'] = dict_key
        else:
            var_msg = "The dictionary `self.tables` is empty"
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        self.set_table(df, overwrite=True)
        module_logger.info("Completed `concatenate_tables`")

    def dictionary_tables(self, key=None):
        """
        Where the tables are in a DataFrame format put them in a dictionary,
        using the values in the key column as the new dictionary keys
        """
        module_logger.info("Starting `dictionary_tables`")
        if type(self.tables).__name__ != "DataFrame":
            var_msg = ("For the function `dictionary_tables` the `tables` "
                       "should be in DataFrame format.")
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        df = self.tables
        dict_dfs = dict()

        if key is not None:
            var_cycle = key
        else:
            var_cycle = "level_0"
        if var_cycle not in self.tables.columns.tolist():
            var_msg = f"There is no {var_cycle} column present in the table"
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        for val in df[var_cycle].unique().tolist():
            dict_dfs[val] = df.loc[df[var_cycle] == val].copy()
        self.set_table(dict_dfs)

        module_logger.info("Completed `dictionary_tables`")

    def read_in_headers(self, function=None, path=None, script_name=None,
                        func_name="read_headers", **kwargs):
        # TODO Need to see if we can isolate just a set of new tables? Maybe
        #  have a list of dictionary keys that have had their headers done
        #  already?
        module_logger.info("Starting `read_in_headers`")

        if function is not None:
            if type(function).__name__ != "function":
                var_msg = ("The function passed to `self.read_in_headers` is "
                           "not a function.")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif script_name is not None:
            function = self.__import_attr(path, script_name, func_name)
        else:
            var_msg = ("One of the `function` or `script_name` arguments needs "
                       "to be completed. And if `script name is then `path` "
                       "needs to be too.")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        try:
            dfs = function(**kwargs)
            self.headers = dfs.copy()
        except AttributeError:
            if len([x for x in kwargs.keys()]) > 0:
                var_msg = (
                    f"Function read_in_headers, kwargs may have been passed "
                    f"when the function {func_name} in the script {script_name}"
                    f" does not take kwargs")
            else:
                var_msg = (f"Function read_in_headers: The {func_name} function"
                           f" does not exist in the {script_name} script.")
            module_logger.error(var_msg)
            raise AttributeError(var_msg)

        if len([x for x in dfs.keys() if x == "IdealHeaders"]) == 0:
            var_msg = "There is no IdealHeaders present"
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        module_logger.info("Completed `read_in_headers`")

    def link_headers(self, function=None, path=None, script_name=None,
                     func_name="link_headers", **kwargs):
        # TODO Need to see if we can isolate just a set of new tables? Maybe
        #  have a list of dictionary keys that have had their headers
        #  done already?
        module_logger.info("Starting `link_headers`")

        if function is not None:
            if type(function).__name__ != "function":
                var_msg = ("The function passed to `self.link_headers` is "
                           "not a function.")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif script_name is not None:
            function = self.__import_attr(path, script_name, func_name)
        else:
            var_msg = ("One of the `function` or `script_name` arguments needs "
                       "to be completed. And if `script name is then `path` "
                       "needs to be too.")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        try:
            dfs = function(self.tables, self.headers, **kwargs)
            self.__link_headers = dfs.copy()
        except AttributeError:
            if len([x for x in kwargs.keys()]) > 0:
                var_msg = (
                    f"Function link_headers, kwargs may have been passed when "
                    f"the function {func_name} in the script {script_name} does"
                    f" not take kwargs")
            else:
                var_msg = (f"Function link_headers: The {func_name} function "
                           f"does not exist in the {script_name} script.")
            module_logger.error(var_msg)
            raise AttributeError(var_msg)

        list_unallocated_keys = set(self.tables.keys()) - set(dfs.keys())
        if len(list_unallocated_keys) != 0:
            var_msg = (f"Not all the headers are linked, the unlinked tables "
                       f"are: {list_unallocated_keys}")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        module_logger.info("Completed `link_headers`")

    def assert_linked_headers(self, reset_index=False):
        module_logger.info("Starting `assert_linked_headers`")

        list_ideal_headers = self.headers[
            "IdealHeaders"].loc[0].values.tolist()
        list_keys = [
            key for key in self.__link_headers.keys() if key != "IdealHeaders"
        ]

        for key in list_keys:
            df_new_headers = self.headers[self.__link_headers[key]].copy()

            df_new_name = df_new_headers.loc[
                df_new_headers[0] == "New name"].copy()
            df_new_name.drop([0], axis=1, inplace=True)
            list_new_names = df_new_name.iloc[0].values.tolist()

            df_remove = df_new_headers.loc[
                df_new_headers[0] == "Remove"].iloc[0].copy()
            list_remove = df_remove.loc[df_remove.notnull()].index.tolist()
            list_remove.pop(list_remove.index(0))
            list_remove_names = [list_new_names[idx - 1] for idx in list_remove]
            self.tables[key].columns = list_new_names
            self.tables[key].drop(list_remove_names, axis=1, inplace=True)

            var_drop_len = df_new_headers.loc[
                df_new_headers[0] == 'Header'].shape[0]
            self.tables[key].drop(
                self.tables[key].iloc[:var_drop_len].index.tolist(),
                inplace=True
            )
            if reset_index:
                self.tables[key].reset_index(drop=True, inplace=True)

            for col in [
                col for col in list_ideal_headers if
                col not in self.tables[key].columns.tolist()
            ]:
                self.tables[key][col] = np.nan

            self.tables[key] = self.tables[key][list_ideal_headers].copy()

        module_logger.info("Completed `assert_linked_headers`")

    def set_headers(self, list_cols=None, function=None, ideal_headers=None,
                    required_headers=None):
        module_logger.info("Starting `set_headers`")
        if list_cols is not None:
            if type(list_cols).__name__ != "list":
                var_msg = ("The argument `list_cols` of function `set_headers` "
                           "needs to be a list")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif function is not None:
            if type(function).__name__ != "function":
                var_msg = ("The argument `function` of function `set_headers` "
                           "needs to be a function")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif ideal_headers is not None:
            if type(ideal_headers).__name__ != 'list':
                var_msg = ("The argument `ideal_headers` of function "
                           "`set_headers` needs to be a list")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif required_headers is not None:
            if type(required_headers).__name__ != 'list':
                var_msg = ("The argument `required_headers` of function "
                           "`set_headers` needs to be a list")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        var_type = type(self.tables).__name__
        if var_type == "dict":
            dict_dfs = self.tables.copy()
            var_cond = len(
                set([dict_dfs[key].shape[1] for key in dict_dfs.keys()]))
            var_cond = var_cond != 1
            if var_cond:
                var_msg = ("There are an inconsistent number of columns "
                           "present in the dictionary of tables")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            if list_cols is not None:
                if (len(list_cols) !=
                        dict_dfs[[x for x in dict_dfs.keys()][0]].shape[1]):
                    var_msg = ("The length of `list_cols` is different to the "
                               "number of columns present in the table")
                    module_logger.error(var_msg)
                    raise ValueError(var_msg)
            elif function is not None:
                list_cols_org = dict_dfs[
                    [x for x in dict_dfs.keys()][0]
                ].columns.tolist()
                list_cols = [function(x) for x in list_cols_org]
            for key in dict_dfs.keys():
                if list_cols is not None:
                    dict_dfs[key].columns = list_cols
                elif function is not None:
                    dict_dfs[key].columns = list_cols
                elif ideal_headers is not None:
                    for col in [
                        col for col in ideal_headers if
                        col not in dict_dfs[key].columns.tolist()
                    ]:
                        dict_dfs[key][col] = np.nan
                    dict_dfs[key] = dict_dfs[key][ideal_headers].copy()
                elif required_headers is not None:
                    for col in [
                        col for col in required_headers if
                        col not in dict_dfs[key].columns.tolist()
                    ]:
                        dict_dfs[key][col] = np.nan
            self.set_table(dict_dfs, overwrite=True)
        elif var_type == "DataFrame":
            if len(list_cols) != self.tables.shape[1]:
                var_msg = ("The length of `list_cols` is different to the "
                           "number of columns present in the table")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            df = self.tables.copy()
            if list_cols is not None:
                df.columns = list_cols
            elif function is not None:
                df.columns = [function(x) for x in df.columns.tolist()]
            elif ideal_headers is not None:
                for col in [
                    col for col in ideal_headers if
                    col not in df.columns.tolist()
                ]:
                    df[col] = np.nan
                df = df[ideal_headers].copy()
            elif required_headers is not None:
                for col in [
                    col for col in required_headers if
                    col not in df.columns.tolist()
                ]:
                    df[col] = np.nan
            self.set_table(df, overwrite=True)
        else:
            var_msg = ("Somehow the tables are not a dictionary or a DataFrame "
                       "for function `set_headers`")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        module_logger.info("Completed `set_headers`")

    def alter_tables(self, script_name=None, path=None,
                     object_name="dict_alter", dictionary=None, **kwargs):
        """
        Use this functionality to make alterations to the table(s)
        """
        module_logger.info("Starting `alter_tables`")
        # TODO move this check to own function (applies to convert_columns too)
        if (script_name is not None) & (object_name is not None):
            dict_alter = self.__import_attr(path, script_name, object_name)
        elif dictionary is not None:
            if type(dictionary).__name__ != "dict":
                var_msg = "The `dictionary` argument is not a dictionary"
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            dict_alter = dictionary
        else:
            var_msg = ("Either `dictionary` or both of `script_name` and "
                       "`path` need to be none null")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        if type(self.tables).__name__ == "DataFrame":
            df = self.tables.copy()
            df_new = self.__alter_cols(
                df, dict_alter, [self.__key_1, self.__key_2, self.__key_3],
                np.nan, **kwargs)
            self.set_table(df_new)
        elif type(self.tables).__name__ == "dict":
            dfs = self.tables
            for key in self.tables.keys():
                df = dfs[key].copy()
                df_new = self.__alter_cols(
                    df, dict_alter, [self.__key_1, self.__key_2, self.__key_3],
                    key, **kwargs)
                self.set_table(df_new, key)
        else:
            var_msg = ("The tables are in neither a DataFrame or dictionary "
                       "format, which means something is seriously wrong...")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        module_logger.info("Completed `alter_tables`")

    def __alter_cols(self, df, dict_alter, keys, dict_key, **kwargs):
        module_logger.info("Starting `__alter_cols`")
        if pd.isnull(dict_key):
            var_file = np.nan
            var_subfile = np.nan
        else:
            var_file = dict_key.split(self.__key_separator)[0]
            var_subfile = (dict_key.split(self.__key_separator)[1] if
                           self.__key_separator in dict_key else np.nan)
        for alter_key in dict_alter.keys():
            var_type = dict_alter[alter_key]["type"]
            function = dict_alter[alter_key]["function"]
            if var_type == "new_col":
                var_col_name = dict_alter[alter_key]["col_name"]
                if var_col_name in df.columns.tolist():
                    var_msg = (
                        f"The column {var_col_name} is present in the "
                        f"table so should not be overwritten")
                    module_logger.error(var_msg)
                    self.error_handling(var_file, var_subfile, "", var_msg,
                                        var_col_name, np.nan, np.nan)
                    continue
                try:
                    s = function(df, keys, **kwargs)
                    df[var_col_name] = s
                except KeyError:
                    var_msg = (
                        f"For type new_col the function for alter_key "
                        f"{alter_key} has not worked with a KeyError")
                    module_logger.error(var_msg)
                    self.error_handling(var_file, var_subfile, "", var_msg,
                                        var_col_name, np.nan, np.nan)
                    continue
                except:
                    var_msg = (f"For type new_col the function for "
                               f"alter_key {alter_key} has not worked")
                    module_logger.error(var_msg)

                    var_idx = np.nan
                    var_issue_count = np.nan
                    if "idx_function" in dict_alter[alter_key]:
                        func_idx = dict_alter[alter_key]['idx_function']
                        if type(func_idx).__name__ != 'function':
                            var_msg = ''
                            module_logger.error(var_msg)
                        s_idx = func_idx(df, keys, **kwargs)
                        var_idx = ', '.join(
                            [
                                str(item) for item in
                                s_idx.loc[s_idx].index.tolist()
                            ]
                        )
                        var_issue_count = s_idx.sum()
                    self.error_handling(var_file, var_subfile, "", var_msg,
                                        var_col_name, var_issue_count, var_idx)
                    continue
            elif var_type == "map_df":
                try:
                    df = function(df, keys, **kwargs)
                except:
                    var_msg = (f"For type map_df the function for "
                               f"alter_key {alter_key} has not worked")
                    module_logger.error(var_msg)

                    var_idx = np.nan
                    var_issue_count = np.nan
                    if "idx_function" in dict_alter[alter_key]:
                        func_idx = dict_alter[alter_key]['idx_function']
                        if type(func_idx).__name__ != 'function':
                            var_msg = ''
                            module_logger.error(var_msg)
                        s_idx = func_idx(df, keys, **kwargs)
                        var_idx = ', '.join(
                            [
                                str(item) for item in
                                s_idx.loc[s_idx].index.tolist()
                            ]
                        )
                        var_issue_count = s_idx.sum()
                    self.error_handling(var_file, var_subfile, "", var_msg,
                                        np.nan, var_issue_count, var_idx)
                    continue

        module_logger.info("Completed `__alter_cols`")
        return df

    def convert_columns(self, script_name=None, path=None,
                        object_name="dict_convert", dictionary=None, **kwargs):
        module_logger.info("Starting `convert_columns`")
        if (script_name is not None) & (object_name is not None):
            dict_convert = self.__import_attr(path, script_name, object_name)
        elif dictionary is not None:
            if type(dictionary).__name__ != "dict":
                var_msg = "The `dictionary` argument is not a dictionary"
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            dict_convert = dictionary
        else:
            var_msg = ("Either `dictionary` or both of `script_name` and "
                       "`path` need to be none null")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        if type(self.tables).__name__ == "DataFrame":
            df = self.tables.copy()
            df_new = self.__convert_col(df, dict_convert, "", **kwargs)
            self.set_table(df_new, overwrite=True)
        elif type(self.tables).__name__ == "dict":
            dfs = self.tables
            for key in self.tables.keys():
                df = dfs[key].copy()
                df_new = self.__convert_col(df, dict_convert, key, **kwargs)
                dfs[key] = df_new.copy()
            self.set_table(dfs, overwrite=True)
        else:
            var_msg = ("The tables are in neither a DataFrame or dictionary "
                       "format, which means something is seriously wrong...")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        module_logger.info("Completed `convert_columns`")

    def __convert_col(self, df, dict_convert, dict_key, **kwargs):
        module_logger.info("Starting `__convert_col`")
        for convert_key in dict_convert.keys():
            list_cols = dict_convert[convert_key]["columns"]
            list_stops = dict_convert[convert_key]["dtypes"]
            dict_functions = dict_convert[convert_key]["functions"]
            for col in list_cols:
                if col not in df.columns.tolist():
                    var_msg = f"The column {col} is not present"
                    module_logger.error(var_msg)
                    raise ValueError(var_msg)
                dtype_flag = 0
                var_dtype = df[col].dtype.name
                for dtype in list_stops:
                    if dtype in var_dtype:
                        dtype_flag = 1
                        break
                if dtype_flag == 1:
                    continue
                converted_flag = 0
                for key in dict_functions.keys():
                    func_use = dict_functions[key]
                    if type(func_use).__name__ != "function":
                        var_msg = (f"The function for converting is not a "
                                   f"function! For keys {convert_key}, {key}")
                        module_logger.error(var_msg)
                        raise ValueError(var_msg)
                    try:
                        s = func_use(df, col, **kwargs)
                        df[col] = s.copy()
                        converted_flag = 1
                        break
                    except:
                        var_msg = (f"The conversion failed for keys "
                                   f"{convert_key}, {key}, trying next")
                        module_logger.warning(var_msg)
                        continue
                if converted_flag == 0:
                    var_idx = np.nan
                    var_issue_count = np.nan
                    if "idx_function" in dict_convert[convert_key]:
                        func_idx = dict_convert[convert_key]['idx_function']
                        if type(func_idx).__name__ != 'function':
                            var_msg = (
                                f'The `idx_function` argument is not a function'
                                f' it is a {type(func_idx).__name__}')
                            module_logger.error(var_msg)
                            raise ValueError(var_msg)
                        s_idx = func_idx(df, col, **kwargs)
                        var_idx = ', '.join(
                            [
                                str(item) for item in
                                s_idx.loc[s_idx].index.tolist()
                            ]
                        )
                        var_issue_count = s_idx.sum()
                    var_msg = (f"The conversion for column {col} for "
                               f"convert_key {convert_key} failed.")
                    module_logger.error(var_msg)
                    self.error_handling(
                        dict_key.split(self.__key_separator)[0],
                        (dict_key.split(self.__key_separator)[1] if
                         self.__key_separator in dict_key else np.nan),
                        "",
                        f"The conversion failed to format {convert_key}",
                        col,
                        var_issue_count,
                        var_idx
                    )

        module_logger.info("Completed `__convert_col`")
        return df

    def assert_nulls(self, list_nulls=None, list_exclude_cols=None):
        module_logger.info("Starting `assert_nulls`")
        if list_nulls is None:
            list_nulls_use = ["nan", ""]
        else:
            list_nulls_use = list_nulls
        if list_exclude_cols is None:
            list_exclude_cols_use = []
        else:
            list_exclude_cols_use = list_exclude_cols
        module_logger.info(f"The nulls being used are: {list_nulls_use}")
        module_logger.info(
            f"The columns being excluded are: {list_exclude_cols_use}")
        df = self.tables.copy()
        if type(df).__name__ == "dict":
            list_keys = [x for x in df.keys()]
            for key in list_keys:
                for null in list_nulls_use:
                    if len(list_exclude_cols_use) == 0:
                        df[key] = df[key].replace(null, np.nan)
                    else:
                        for col in [
                            col for col in df[key].columns.tolist() if
                            col not in list_exclude_cols_use
                        ]:
                            df[key][col] = df[key][col].replace(null, np.nan)
        else:
            for null in list_nulls_use:
                if len(list_exclude_cols_use) == 0:
                    df = df.replace(null, np.nan)
                else:
                    for col in [
                        col for col in df.columns.tolist() if
                        col not in list_exclude_cols_use
                    ]:
                        df[col] = df[col].replace(null, np.nan)
        self.set_table(df, overwrite=True)
        module_logger.info("Completed `assert_nulls`")

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

    def form_summary_tables(self, function=None, path=None, script_name=None,
                            func_name="form_tables", **kwargs):
        """
        Use a function to create summaries off the main table set.

        The function is passed the arguments:
            self.tables, self.formed_tables, self.__grouping, self.__key_1,
            self.__key_2, self.__key_3, self.__key_separator, **kwargs
        """
        module_logger.info("Starting `form_summary_tables`")

        if function is not None:
            if type(function).__name__ != "function":
                var_msg = ("The function passed to `self.form_summary_tables` "
                           "is not a function.")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif script_name is not None:
            function = self.__import_attr(path, script_name, func_name)
        else:
            var_msg = ("One of the `function` or `script_name` arguments needs "
                       "to be completed. And if `script name is then `path` "
                       "needs to be too.")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        dict_formed_tables = function(
            self.tables, self.formed_tables, self.__grouping, self.__key_1,
            self.__key_2, self.__key_3, self.__key_separator, **kwargs)
        if type(dict_formed_tables).__name__ != 'dict':
            var_msg = ('The output of the function for `form_summary_table` '
                       'is not a dictionary and it needs to be')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        self.formed_tables = dict_formed_tables

        module_logger.info("Completed `form_summary_tables`")

    def get_step_no(self):
        module_logger.info("Starting `get_step_no`")
        module_logger.info("Completed `get_step_no`")
        return self.__step_no

    def _repr_html_(self):
        module_logger.info("Starting `_repr__html_`")
        var_key_3 = "" if self.__key_3 == "None" else self.__key_3
        var_out_keys = f"""
<table style="width:30%">
  <tr>
    <th>Grouping</th>
    <td>{self.__grouping}</td>
  </tr>
  <tr>
    <th>Key 1</th>
    <td>{self.__key_1}</td>
  </tr>
  <tr>
    <th>Key 2</th>
    <td>{self.__key_2}</td>
  </tr>
  <tr>
    <th>Key 3</th>
    <td>{var_key_3}</td>
  </tr>
</table>
        """
        if type(self.tables).__name__ == 'dict':
            var_out_tbl_info = """
<table style="width:100%">
  <tr>
    <th>Dictionary key</th>
    <th>Dataframe shape</th>
    <th>Count numeric columns</th>
    <th>Count date columns</th>
    <th>Count object columns</th>
  </tr>
  {}
</table>
            """
            for key in [key for key in self.tables.keys()]:
                var_out_tbl_info = var_out_tbl_info.replace(
                    '{}',
                    f"""
  <tr>
    <td>{key}</td>
    <td>{self.tables[key].shape}</td>
    <td>{self.tables[key].select_dtypes(include=[np.number]).shape[1]}</td>
    <td>{self.tables[key].select_dtypes(include=[np.datetime64, np.timedelta64]).shape[1]}</td>
    <td>{self.tables[key].select_dtypes(exclude=[np.number, np.datetime64, np.timedelta64]).shape[1]}</td>
  </tr>
  {{}}
                    """
                )
            var_out_tbl_info = var_out_tbl_info.replace('{}', '')
        else:
            var_out_tbl_info = f"""
<table style="width:100%">
  <tr>
    <th>Dataframe shape</th>
    <th>Count numeric columns</th>
    <th>Count date columns</th>
    <th>Count object columns</th>
  </tr>
  <tr>
    <td>{self.tables.shape}</td>
    <td>{self.tables.select_dtypes(include=[np.number]).shape[1]}</td>
    <td>{self.tables.select_dtypes(include=[np.datetime64, np.timedelta64]).shape[1]}</td>
    <td>{self.tables.select_dtypes(exclude=[np.number, np.datetime64, np.timedelta64]).shape[1]}</td>
  </tr>
</table>
            """
        var_out_issues = """
        """
        var_out = f"{var_out_keys}<br><br>{var_out_tbl_info}<br><br>{var_out_issues}"
        module_logger.info("Completed `_repr_html_`")
        return var_out
