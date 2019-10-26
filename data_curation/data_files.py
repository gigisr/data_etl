# Here we are defining a class that will deal with all the data storage and
# manipulations
import os
import logging
import importlib

import pandas as pd
import numpy as np

module_logger = logging.getLogger(__name__)

# TODO unit tests
# TODO Where self.df_tables = X etc with self.df_headers... Then we need to
#  use the inbuilt functions rather than asserting it directly
# TODO Change "".format to f"" throughout
# TODO Change returns for specific functions to not be the default self.step_no
# TODO replace ' with "
# TODO clean up the code


class DataCuration:
    # TODO remove the tables attribute from the object, it is appearing and I
    #  don't know why
    __step_no = 0
    df_issues = None
    df_headers = None
    __key_1 = None
    __key_2 = None
    __key_3 = None
    df_tables = dict()
    # TODO make this only set-able in the initialisation step
    list_files = list()
    __key_separator = ' -:- '
    __link_headers = dict()

    def __init__(self, key_1, key_2=None, key_3=None):
        """
        All data actions are taken on all tables, the aim is to process data to
        end up with a uniform data set that can be utilised and is consistent.

        The three arguments are individual identifiers for the data.

        The end form would be a pipeline that has regular data ingests.
        """
        module_logger.info('Initialising `DataCuration` object')
        # Three keys, all good things come in threes
        self.__key_1 = str(key_1)
        self.__key_2 = str(key_2)
        self.__key_3 = str(key_3)
        # sub_file, e.g. sheet for a spreadsheet, may not always be applicable
        df_issues = pd.DataFrame(
            columns=[
                'key_1', 'key_2', 'key_3', 'file', 'sub_file', 'step_number',
                'issue_short_desc', 'issue_long_desc', 'column', 'issue_count',
                'issue_idx'
            ]
        )
        df_issues['step_number'] = df_issues['step_number'].astype(int)
        self.df_issues = df_issues
        module_logger.info('Initialising `DataCuration` object complete')

    def error_handling(self, file, subfile, issue_short_desc, issue_long_desc,
                       column, issue_count, issue_idx):
        """
        If an error is handled, as they all should be, we need to specify what
        happens with the error. By putting it into a single function it will
        hopefully make the code briefer.
        """
        module_logger.info('Logging an error with `error_handling`')
        df = self.df_issues.copy()
        list_vals = [
            self.__key_1, self.__key_2, self.__key_3, file, subfile,
            self.__step_no, issue_short_desc, issue_long_desc, column,
            issue_count, issue_idx
        ]
        try:
            df.loc[df.shape[0]] = list_vals
            self.df_issues = df.copy()
        except:
            module_logger.error(
                'Logging the issue failed, values: {}'.format(list_vals))
            raise ValueError('Logging an issue has failed, can not continue')
        module_logger.info('Error logged: {}'.format(list_vals))

    def set_step_no(self, step_no):
        """
        Set the step number, this allows errors to be recorded against a
        specific step which in turn can help with issue tracking and checking
        once issues are recorded.

        The argument step_no needs to be convertible to integer format.
        """
        module_logger.info('Starting `set_step_no`')
        try:
            self.__step_no = int(step_no)
        except ValueError:
            var_msg = ('Function set_step_no: The value {} can not be converted'
                       ' to int.'.format(step_no))
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        module_logger.info(
            'Completed `set_step_no`, the step number is {}'.format(
                self.__step_no))

    def set_key_separator(self, separator):
        module_logger.info('Starting `set_key_separator`')
        if (type(separator).__name__ != 'str') | (len(separator) == 0):
            var_msg = ('The argument `separator` for function '
                       '`set_key_separator` should be a string of length '
                       'greater than 0')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        self.__key_separator = separator
        module_logger.info(
            'Completed `set_key_separator`, the key separator is: {}'.format(
                self.__key_separator))

    def set_file_list(self, list_files):
        """
        If there is a know list of files then define them here rather than
        setting a function to find the files.
        """
        module_logger.info('Starting `set_file_list`')
        var_type = type(list_files).__name__
        if (var_type != 'list') & (var_type != 'str'):
            var_msg = ('The type of the `list_files` argument is not a list or '
                       'a string.')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        elif var_type == 'str':
            if len(list_files) == 0:
                var_msg = ('The length of the `list_files` argument is 0, it '
                           'needs to be a valid value.')
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            list_files = [list_files]
        else:
            var_msg = 'Unhandled type for function `set_file_list`: {}'.format(
                var_type)
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        self.list_files = list_files
        module_logger.info(
            'Completed `set_file_list`, the list of files is: {}'.format(
                self.list_files))

    def find_files(self, path, script_name=None, function=None, append=False,
                   func_name='list_the_files', **kwargs):
        """
        Using an externally defined function, as specified in the module
        argument script, acquire a list of files to be read in.

        In the case that we want to accumulate a list of files from different
        main paths there is an append option.
        """
        module_logger.info('Starting `find_files`')
        if script_name is not None:
            mod = importlib.import_module(script_name)
            try:
                function = getattr(mod, func_name)
            except AttributeError:
                if len([x for x in kwargs.keys()]) > 0:
                    var_msg = ('Function find_files, kwargs may have been '
                               'passed when the function {} in the script {} '
                               'does not take kwargs').format(
                        func_name, script_name)
                else:
                    var_msg = ('Function find_files: the function {} is not '
                               'present in the script {}.'.format(
                                   func_name, script_name))
                module_logger.error(var_msg)
                raise AttributeError(var_msg)
        elif function is not None:
            if type(function).__name__ != 'function':
                var_msg = 'The `function` argument needs to be a function'
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        else:
            var_msg = ('One of `script_name` or `function` needs to be not '
                       'None in the function `find_files`')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        list_files = function(path, **kwargs)
        # TODO move these to be calls on the self.set_file_list function instead
        #  of setting the value here
        if append:
            self.list_files += list_files
        else:
            self.list_files = list_files
        module_logger.info(
            'Completed `find_files`, the list of files is: {}'.format(
                self.list_files))

    def reading_in(self, function=None, path=None, script_name=None,
                   func_name='read_files', overwrite=False, **kwargs):
        """
        Using an externally defined reading in function, and the internally
        defined list of files, read in each of the tables required.

        `path` being the relative script file path
        """
        module_logger.info('Starting `reading_in`')
        if type(self.df_tables).__name__ != 'dict':
            var_msg = ('The tables need to be in dictionary format for this '
                       '`self.reading_in` step')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        if function is not None:
            if type(function).__name__ != 'function':
                var_msg = ('The function passed to `self.reading_in` is not a '
                           'function.')
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif (script_name is not None) & (path is not None):
            if not os.path.exists(
                    os.path.join(path, '{}.py'.format(script_name))):
                raise ValueError('The script does not exist')
            mod = importlib.import_module(script_name)
            function = getattr(mod, func_name)
        else:
            var_msg = ('One of the `function` or `script_name` arguments needs '
                       'to be completed. And if `script name is then `path` '
                       'needs to be too.')
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        try:
            dfs = function(self.list_files, **kwargs)
            if overwrite is False:
                df_org = self.df_tables.copy()
                df_org.update(dfs)
            elif overwrite is True:
                pass
            else:
                var_msg = ('The attribute `overwrite` in the function '
                           '`reading_in` needs to be `True` or `False`')
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            self.set_table(dfs, overwrite=overwrite)
            if type(dfs).__name__ == 'DataFrame':
                module_logger.info('The table has shape "{}"'.format(dfs.shape))
            for key in dfs:
                module_logger.info(
                    'The table with key "{}" has shape "{}"'.format(
                        key, dfs[key].shape))
        except AttributeError:
            if len([x for x in kwargs.keys()]) > 0:
                var_msg = ('Function reading_in, kwargs may have been passed '
                           'when the function {} in the script {} does not take'
                           ' kwargs').format(func_name, script_name)
            else:
                var_msg = ('Function reading in: The {} function does '
                           'not exist in the {} script.'
                           ).format(func_name, script_name)
            module_logger.error(var_msg)
            raise AttributeError(var_msg)

        module_logger.info('Completed `reading_in`')

    def set_table(self, df, dict_key=None, overwrite=False):
        """
        If self.df_tables is a dictionary set df to key else overwrite existing
        table if argument is True
        """
        module_logger.info('Starting `set_table`')
        if self.df_tables == dict():
            self.df_tables = df
        elif (type(df).__name__ == 'dict') & (overwrite is True):
            self.df_tables = df
        elif ((type(self.df_tables).__name__ == 'dict') &
                (dict_key is not None)):
            # TODO make sure that the columns are the same as any existing
            #  tables if a new table is being added
            self.df_tables[dict_key] = df
        elif ((type(self.df_tables).__name__ == 'DataFrame') &
              (overwrite is True)):
            self.df_tables = df
        else:
            var_msg = ('The combinations provided are not compatible: `df` - {}'
                       ', `self.df_tables` - {}, `overwrite` - {}').format(
                type(df).__name__, type(self.df_tables).__name__, overwrite)
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        module_logger.info('Completed `set_table`')

    def concatenate_tables(self):
        """
        Where the tables are in a dictionary format put them into a DataFrame
        """
        module_logger.info('Starting `concatenate_tables`')
        if type(self.df_tables).__name__ != 'dict':
            var_msg = ('For the function `concatenate_tables` the `tables` '
                       'should be in dictionary format')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        df = pd.concat(self.df_tables, axis=1)
        self.df_tables = df
        module_logger.info('Completed `concatenate_tables`')

    def dictionary_tables(self, key=None):
        """
        Where the tables are in a DataFrame format put them in a dictionary,
        using the values in the key column as the new dictionary keys
        """
        module_logger.info('Starting `dictionary_tables`')
        if type(self.df_tables).__name__ != 'DataFrame':
            var_msg = ('For the function `dictionary_tables` the `tables` '
                       'should be in DataFrame format.')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        df = self.df_tables
        dict_dfs = dict()
        if key is not None:
            var_cycle = key
        else:
            var_cycle = 'level_0'
        if var_cycle not in self.df_tables.columns.tolist():
            raise ValueError(
                'There is no {} column present in the table'.format(var_cycle))
        for val in df[var_cycle].unique().tolist():
            dict_dfs[val] = df.loc[df[var_cycle] == val].copy()
        self.df_tables = dict_dfs
        module_logger.info('Completed `dictionary_tables`')

    def read_in_headers(self, function=None, path=None, script_name=None,
                        func_name='read_headers', **kwargs):
        # TODO Need to see if we can isolate just a set of new tables? Maybe
        #  have a list of dictionary keys that have had their headers done
        #  already?
        module_logger.info('Starting `read_in_headers`')

        if function is not None:
            if type(function).__name__ != 'function':
                var_msg = ('The function passed to `self.read_in_headers` is '
                           'not a function.')
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif (script_name is not None) & (path is not None):
            if not os.path.exists(
                    os.path.join(path, '{}.py'.format(script_name))):
                raise ValueError('The script does not exist')
            mod = importlib.import_module(script_name)
            function = getattr(mod, func_name)
        else:
            var_msg = ('One of the `function` or `script_name` arguments needs '
                       'to be completed. And if `script name is then `path` '
                       'needs to be too.')
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        try:
            dfs = function(**kwargs)
            self.df_headers = dfs.copy()
        except AttributeError:
            if len([x for x in kwargs.keys()]) > 0:
                var_msg = ('Function read_in_headers, kwargs may have been '
                           'passed when the function {} in the script {} does '
                           'not take kwargs').format(func_name, script_name)
            else:
                var_msg = ('Function read_in_headers: The {} function does '
                           'not exist in the {} script.'
                           ).format(func_name, script_name)
            module_logger.error(var_msg)
            raise AttributeError(var_msg)

        if len([x for x in dfs.keys() if x == 'IdealHeaders']) == 0:
            var_msg = 'There is no IdealHeaders present'
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        module_logger.info('Completed `read_in_headers`')
        return self.__step_no

    def link_headers(self, function=None, path=None, script_name=None,
                     func_name='link_headers', **kwargs):
        # TODO Make sure kwargs is included
        # TODO Need to see if we can isolate just a set of new tables? Maybe
        #  have a list of dictionary keys that have had their headers
        #  done already?
        module_logger.info('Starting `link_headers`')

        if function is not None:
            if type(function).__name__ != 'function':
                var_msg = ('The function passed to `self.link_headers` is '
                           'not a function.')
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif (script_name is not None) & (path is not None):
            if not os.path.exists(
                    os.path.join(path, '{}.py'.format(script_name))):
                raise ValueError('The script does not exist')
            mod = importlib.import_module(script_name)
            function = getattr(mod, func_name)
        else:
            var_msg = ('One of the `function` or `script_name` arguments needs '
                       'to be completed. And if `script name is then `path` '
                       'needs to be too.')
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        try:
            dfs = function(self.df_tables, self.df_headers, **kwargs)
            self.__link_headers = dfs.copy()
        except AttributeError:
            if len([x for x in kwargs.keys()]) > 0:
                var_msg = ('Function link_headers, kwargs may have been '
                           'passed when the function {} in the script {} does '
                           'not take kwargs').format(func_name, script_name)
            else:
                var_msg = ('Function link_headers: The {} function does '
                           'not exist in the {} script.'
                           ).format(func_name, script_name)
            module_logger.error(var_msg)
            raise AttributeError(var_msg)

        list_unallocated_keys = set(self.df_tables.keys()) - set(dfs.keys())
        if len(list_unallocated_keys ) != 0:
            var_msg = (
                f"Not all the headers are linked, the unlinked tables are: "
                f"{list_unallocated_keys}"
            )
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        module_logger.info('Completed `link_headers`')
        return self.__step_no

    def assert_linked_headers(self, **kwargs):
        # TODO Make sure kwargs is included
        # TODO Need to see if we can isolate just a set of new tables? Maybe
        #  have a list of dictionary keys that have had their headers
        #  done already?
        module_logger.info('Starting `assert_linked_headers`')

        list_ideal_headers = self.df_headers[
            'IdealHeaders'].loc[0].values.tolist()
        list_keys = [
            key for key in self.__link_headers.keys() if key != 'IdealHeaders'
        ]

        for key in list_keys:
            df_new_headers = self.df_headers[self.__link_headers[key]].copy()

            df_new_name = df_new_headers.loc[
                df_new_headers[0] == 'New name'].copy()
            df_new_name.drop([0], axis=1, inplace=True)
            list_new_names = df_new_name.iloc[0].values.tolist()

            df_remove = df_new_headers.loc[
                df_new_headers[0] == 'Remove'].iloc[0].copy()
            list_remove = df_remove.loc[df_remove.notnull()].index.tolist()
            list_remove.pop(list_remove.index(0))
            list_remove_names = [list_new_names[idx - 1] for idx in list_remove]

            self.df_tables[key].columns = list_new_names
            self.df_tables[key].drop(list_remove_names, axis=1, inplace=True)

            

        module_logger.info('Completed `assert_linked_headers`')
        return self.__step_no

    def set_headers(self, list_cols=None, function=None):
        module_logger.info('Starting `set_headers`')
        if list_cols is not None:
            if type(list_cols).__name__ != 'list':
                var_msg = ('The argument `list_cols` of function `set_headers` '
                           'needs to be a list')
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif function is not None:
            if type(function).__name__ != 'function':
                var_msg = ('The argument `function` of function `set_headers` '
                           'needs to be a function')
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        var_type = type(self.df_tables).__name__
        if var_type == 'dict':
            dict_dfs = self.df_tables.copy()
            var_cond = len(
                set([dict_dfs[key].shape[1] for key in dict_dfs.keys()]))
            var_cond = var_cond != 1
            if var_cond:
                var_msg = ('There are an inconsistent number of columns '
                           'present in the dictionary of tables')
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            if list_cols is not None:
                if (len(list_cols) !=
                        dict_dfs[[x for x in dict_dfs.keys()][0]].shape[1]):
                    var_msg = ('The length of `list_cols` is different to the '
                               'number of columns present in the table')
                    module_logger.error(var_msg)
                    raise ValueError(var_msg)
            dict_dfs = self.df_tables.copy()
            if function is not None:
                list_cols_org = dict_dfs[
                    [x for x in dict_dfs.keys()][0]
                ].columns.tolist()
                list_cols_org = [function(x) for x in list_cols_org]
            for key in dict_dfs.keys():
                if list_cols is not None:
                    dict_dfs[key].columns = list_cols
                elif function is not None:
                    dict_dfs[key].columns = list_cols_org
            self.set_table(dict_dfs, overwrite=True)
        elif var_type == 'DataFrame':
            if len(list_cols) != self.df_tables.shape[1]:
                var_msg = ('The length of `list_cols` is different to the '
                           'number of columns present in the table')
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            df = self.df_tables.copy()
            if list_cols is not None:
                df.columns = list_cols
            elif function is not None:
                df.columns = [function(x) for x in df.columns.tolist()]
            self.set_table(df, overwrite=True)
        else:
            var_msg = ('Somehow the tables are not a dictionary or a DataFrame '
                       'for function `set_headers`')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        module_logger.info('Completed `set_headers`')

    def alter_tables(self, script_name=None, path=None,
                     object_name='dict_alter', dictionary=None, **kwargs):
        """
        Use this functionality to make alterations to the table(s)
        """
        module_logger.info('Starting `alter_tables`')
        # TODO move this check to own function (applies to convert_columns too
        if (script_name is not None) & (object_name is not None):
            if not os.path.exists(
                    os.path.join(path, '{}.py'.format(script_name))):
                raise ValueError('The script does not exist')
            mod = importlib.import_module(script_name)
            dict_alter = getattr(mod, object_name)
        elif dictionary is not None:
            if type(dictionary).__name__ != 'dict':
                var_msg = ''
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            dict_alter = dictionary
        else:
            var_msg = ('Either `dictionary` or both of `script_name` and '
                       '`path` need to be none null')
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        if type(self.df_tables).__name__ == 'DataFrame':
            df = self.df_tables.copy()
            self.__alter_cols(
                df, dict_alter, [self.__key_1, self.__key_2, self.__key_3],
                **kwargs
            )
        elif type(self.df_tables).__name__ == 'dict':
            dfs = self.df_tables
            for key in self.df_tables.keys():
                df = dfs[key].copy()
                self.__alter_cols(
                    df, dict_alter, [self.__key_1, self.__key_2, self.__key_3],
                    **kwargs
                )
        else:
            var_msg = ('The tables are in neither a DataFrame or dictionary '
                       'format, which means something is seriously wrong...')
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        module_logger.info('Completed `alter_tables`')

    def __alter_cols(self, df, dict_alter, keys, **kwargs):
        module_logger.info('Starting `__alter_cols`')
        var_tbl_type = type(self.df_tables).__name__
        dfs = self.df_tables
        for alter_key in dict_alter.keys():
            var_type = dict_alter[alter_key]['type']
            var_col_name = dict_alter[alter_key]['col_name']
            function = dict_alter[alter_key]['function']
            if var_type == 'new_col':
                if var_tbl_type == 'dict':
                    for tbl_key in [x for x in self.df_tables.keys()]:
                        if var_col_name in dfs[tbl_key].columns.tolist():
                            var_msg = ''
                            module_logger.error(var_msg)
                            # TODO add in error_handling call here
                            continue
                        try:
                            s = function(df, keys, **kwargs)
                            dfs[tbl_key][var_col_name] = s
                        except:
                            var_msg = ''
                            module_logger.error(var_msg)
                            # TODO add in error_handling call here
                elif var_tbl_type == 'DataFrame':
                    if var_col_name in dfs.columns.tolist():
                        var_msg = ''
                        module_logger.error(var_msg)
                        # TODO add in error_handling call here
                        continue
                    try:
                        s = function(df, keys, **kwargs)
                        dfs[var_col_name] = s
                    except:
                        var_msg = ''
                        module_logger.error(var_msg)
                        # TODO add in error_handling call here
            elif var_type == 'map_df':
                if var_tbl_type == 'dict':
                    for tbl_key in [x for x in self.df_tables.keys()]:
                        try:
                            df = function(dfs[tbl_key], keys, **kwargs)
                            dfs[tbl_key] = df.copy()
                        except:
                            var_msg = ''
                            module_logger.error(var_msg)
                            # TODO add in error_handling call here
                elif var_tbl_type == 'DataFrame':
                    try:
                        df = function(df, keys, **kwargs)
                        dfs = df.copy()
                    except:
                        var_msg = ''
                        module_logger.error(var_msg)
                        # TODO add in error_handling call here

        module_logger.info('Completed `__alter_cols`')

    def convert_columns(self, script_name=None, path=None,
                        object_name='dict_convert', dictionary=None, **kwargs):
        module_logger.info('Starting `convert_columns`')
        if (script_name is not None) & (object_name is not None):
            if not os.path.exists(
                    os.path.join(path, '{}.py'.format(script_name))):
                raise ValueError('The script does not exist')
            mod = importlib.import_module(script_name)
            dict_convert = getattr(mod, object_name)
        elif dictionary is not None:
            if type(dictionary).__name__ != 'dict':
                var_msg = ''
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            dict_convert = dictionary
        else:
            var_msg = ('Either `dictionary` or both of `script_name` and '
                       '`path` need to be none null')
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        if type(self.df_tables).__name__ == 'DataFrame':
            df = self.df_tables.copy()
            df_new = self.__convert_col(df, dict_convert, '', **kwargs)
            self.set_table(df_new, overwrite=True)
        elif type(self.df_tables).__name__ == 'dict':
            dfs = self.df_tables
            for key in self.df_tables.keys():
                df = dfs[key].copy()
                df_new = self.__convert_col(df, dict_convert, key, **kwargs)
                dfs[key] = df_new.copy()
            self.set_table(dfs, overwrite=True)
        else:
            var_msg = ('The tables are in neither a DataFrame or dictionary '
                       'format, which means something is seriously wrong...')
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        module_logger.info('Completed `convert_columns`')

    def __convert_col(self, df, dict_convert, dict_key, **kwargs):
        module_logger.info('Starting `__convert_col`')
        for convert_key in dict_convert.keys():
            list_cols = dict_convert[convert_key]['columns']
            list_stops = dict_convert[convert_key]['dtypes']
            dict_functions = dict_convert[convert_key]['functions']
            for col in list_cols:
                dtype_flag = 0
                var_dtype = df[col].dtype.name
                for dtype in list_stops:
                    if dtype in var_dtype:
                        dtype_flag = 1
                        break
                if dtype_flag == 1:
                    continue
                converted_flag = 0
                for i in range(1, len([x for x in dict_functions.keys()]) + 1):
                    try:
                        func_use = dict_functions[i]
                    except:
                        var_msg = ('The functions should be keyed in integers '
                                   'from 1 to number of keys, this happened in '
                                   'convert_key {} and function key {}').format(
                            convert_key, i)
                        module_logger.error(var_msg)
                        raise ValueError(var_msg)
                    if type(func_use).__name__ != 'function':
                        var_msg = ('The function for converting is not a '
                                   'function! For keys {}, {}').format(
                            convert_key, i)
                        module_logger.error(var_msg)
                        raise ValueError(var_msg)
                    try:
                        s = func_use(df, col, **kwargs)
                        df[col] = s.copy()
                        converted_flag = 1
                        break
                    except:
                        var_msg = ('The conversion failed for keys {}, {}, '
                                   'trying next').format(convert_key, i)
                        module_logger.warning(var_msg)
                        continue
                if converted_flag == 0:
                    var_msg = ('The conversion for column {} for convert_key '
                               '{} failed.').format(
                        col, convert_key, i)
                    module_logger.error(var_msg)
                    self.error_handling(
                        dict_key.split(self.__key_separator)[0],
                        dict_key.split(self.__key_separator)[1],
                        'The conversion failed to format {}'.format(
                            convert_key),
                        '',
                        col,
                        pd.np.nan,
                        pd.np.nan
                    )

        module_logger.info('Completed `__convert_col`')
        return df

    def assert_nulls(self, list_nulls=None):
        # TODO: have excluded columns from the nulls application
        module_logger.info('Starting `assert_nulls`')
        if list_nulls is None:
            list_nulls = ['nan', '']
        module_logger.info('The nulls being used are: {}'.format(list_nulls))
        df = self.df_tables.copy()
        if type(df).__name__ == 'dict':
            list_keys = [x for x in df.keys()]
            for key in list_keys:
                for null in list_nulls:
                    df[key] = df[key].replace(null, np.nan)
        else:
            for null in list_nulls:
                df = df.reaplce(null, np.nan)
        self.set_table(df, overwrite=True)
        module_logger.info('Completed `assert_nulls`')

    def get_issue_count(self, issue_number_min=None, issue_number_max=None):
        module_logger.info('Starting `get_issue_count`')
        df = self.df_issues.copy()
        if issue_number_min is not None:
            df = df.loc[df['step_number'] >= issue_number_min].copy()
        if issue_number_max is not None:
            df = df.loc[df['step_number'] <= issue_number_max].copy()
        var_count = df.shape[0]
        module_logger.info('Completed `get_issue_count`')
        return var_count

    def get_step_no(self):
        return self.__step_no
