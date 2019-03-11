# Here we are defining a class that will deal with all the data storage and
# manipulations
import os
import logging
import importlib

import pandas as pd
import numpy as np

module_logger = logging.getLogger(__name__)


class DataCuration:
    __step_no = None
    df_issues = None
    __key_1 = None
    __key_2 = None
    __key_3 = None
    tables = dict()
    list_files = []

    def __init__(self, key_1, key_2=None, key_3=None):
        """
        All data actions are taken on all tables, the aim is to process data to
        end up with a uniform data set that can be utilised and is consistent.

        The three arguments are individual identifiers for the data.

        The end form would be a pipeline that has regular data injests.
        """
        # Three keys, all good things come in threes
        self.__key_1 = key_1
        self.__key_2 = key_2
        self.__key_3 = key_3
        # sub_file, e.g. sheet for a spreadsheet, may not always be applicable
        df_issues = pd.DataFrame(columns=[
            'key_1', 'key_2', 'key_3', 'file', 'sub_file', 'step_number',
            'issue_short_desc', 'issue_long_desc', 'issue_count', 'issue_idx'
        ])
        self.df_issues = df_issues

    def error_handling(self, file, subfile, issue_short_desc, issue_long_desc,
                       issue_count, issue_idx):
        """
        If an error is handled, as they all should be, we need to specify what
        happens with the error. By putting it into a single function it will
        hopefully make the code briefer.
        """
        df = self.df_issues
        list_vals = [
            self.__key_1, self.__key_2, self.__key_3, file, subfile,
            self.__step_no, issue_short_desc, issue_long_desc, issue_count,
            issue_idx
        ]
        try:
            df.iloc[df.shape[0]] = list_vals
            self.df_issues = df.copy()
        except:
            logging.error(
                'Logging the issue failed, values: {}'.format(list_vals))

    def set_step_no(self, step_no):
        """
        Set the step number, this allows errors to be recorded against a
        specific step which in turn can help with issue tracking and checking
        once issues are recorded.

        The argument step_no needs to be convertible to integer format.
        """
        try:
            self.__step_no = int(step_no)
        except ValueError:
            var_msg = ('Function set_step_no: The value {} can not be converted'
                       ' to int.'.format(step_no))
            module_logger.error(var_msg)
            raise ValueError(var_msg)

    def set_file_list(self, list_files):
        """
        If there is a know list of files then define them here rather than
        setting a function to find the files.
        """
        var_type = type(list_files).__name__
        if (var_type != 'list') & (var_type != 'str'):
            var_msg = ('The type of the `list_files` argument is not a list or '
                       'a string.')
            logging.error(var_msg)
            raise ValueError(var_msg)
        if var_type == 'str':
            if len(list_files) == 0:
                var_msg = ('The length of the `list_files` argument is 0, it '
                           'needs to be a valid value.')
                logging.error(var_msg)
                raise ValueError(var_msg)
            list_files = [list_files]
        self.list_files = list_files

    def find_files(self, path, script_name=None, function=None, append=False,
                   func_name='list_the_files', **kwargs):
        """
        Using an externally defined function, as specified in the module
        argument script, acquire a list of files to be read in.

        In the case that we want to accumulate a list of files from different
        main paths there is an append option.
        """
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
                logging.error(var_msg)
                raise ValueError(var_msg)
        else:
            var_msg = ('One of `script_name` or `function` needs to be not '
                       'None in the function `fnd_files`')
            logging.error(var_msg)
            raise ValueError(var_msg)
        list_files = function(path, **kwargs)
        if append:
            self.list_files = self.list_files + list_files
        else:
            self.list_files = list_files
        module_logger.info('The list of files is: {}'.format(self.list_files))

    def reading_in(self, function=None, path=None, script_name=None,
                   func_name='read_files', **kwargs):
        """
        Using an externally defined reading in function, and the internally
        defined list of files, read in each of the tables required.

        `path` being the relative script file path
        """
        if type(self.tables).__name__ != 'dict':
            var_msg = ('The tables need to be in dictionary format for this '
                       '`self.reading_in` step')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        if function is not None:
            if type(function).__name__ != 'function':
                var_msg = ('The function passed to `self.reading_in` is not a '
                           'function.')
                logging.error(var_msg)
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
            logging.error(var_msg)
            raise ValueError(var_msg)

        try:
            dfs = function(self.list_files, **kwargs)
            df_org = self.tables
            df_org.update(dfs)
            self.set_table(df_org, overwrite=True)
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

    def set_table(self, df, dict_key=None, overwrite=False):
        """
        If self.tables is a dictionary set df to key else overwrite existing
        table if argument is True
        """
        if ((type(self.tables).__name__ == 'dict') &
                (dict_key is not None)):
            # TODO make sure that the columns are the same as any existing
            #  tables if a new table is being added
            self.tables[dict_key] = df
        elif (type(df).__name__ == 'dict') & (overwrite is True):
            self.tables = df
        elif ((type(self.tables).__name__ == 'DataFrame') &
              (overwrite is True)):
            self.tables = df
        else:
            var_msg = 'The combinations provided are not compatible'
            logging.error(var_msg)
            raise ValueError(var_msg)

    def concatenate_tables(self):
        """
        Where the tables are in a dictionary format put them into a DataFrame
        """
        if type(self.tables).__name__ != 'dict':
            var_msg = ('For the function `concatenate_tables` the `tables` '
                       'should be in dictionary format')
            logging.error(var_msg)
            raise ValueError(var_msg)
        df = pd.concat(self.tables, axis=1)
        self.tables = df

    def dictionary_tables(self, key=None):
        """
        Where the tables are in a DataFrame format put them in a dictionary,
        using the values in the key column as the new dictionary keys
        """
        if type(self.tables).__name__ != 'DataFrame':
            var_msg = ('For the function `dictionary_tables` the `tables` '
                       'should be in DataFrame format.')
            logging.error(var_msg)
            raise ValueError(var_msg)
        df = self.tables
        dict_dfs = dict()
        if key is not None:
            var_cycle = key
        else:
            var_cycle = 'level_0'
        if var_cycle not in self.tables.columns.tolist():
            raise ValueError(
                'There is no {} column present in the table'.format(var_cycle))
        for val in df[var_cycle].unique().tolist():
            dict_dfs[val] = df.loc[df[var_cycle] == val].copy()
        self.tables = dict_dfs

    def read_in_headers(self, filepath, **kwargs):
        # TODO Need to see if we can isolate just a set of new tables? Maybe
        #  have a list of dictionary keys that have had their headers done
        #  already?
        return self.__step_no

    def link_headers(self, **kwargs):
        # Make sure kwargs are included
        # Need to see if we can isolate just a set of new tables? Maybe have
        # a list of dictionary keys that have had their headers done already?
        return self.__step_no

    def assert_linked_headers(self, **kwargs):
        # Make sure kwargs are included
        # Need to see if we can isolate just a set of new tables? Maybe have
        # a list of dictionary keys that have had their headers done already?
        return self.__step_no

    def set_headers(self, list_cols):
        if type(list_cols).__name__ != 'list':
            var_msg = ('The argument `list_cols` of function `set_headers` '
                       'needs to be a list')
            logging.error(var_msg)
            raise ValueError(var_msg)
        var_type = type(self.tables).__name__
        if var_type == 'dict':
            dict_dfs = self.tables.copy()
            var_cond = len(
                set([dict_dfs[key].shape[1] for key in dict_dfs.keys()]))
            var_cond = var_cond != 1
            if var_cond:
                var_msg = ('There are an inconsistent number of columns '
                           'present in the dictonary of tables')
                logging.error(var_msg)
                raise ValueError(var_msg)
            elif (len(list_cols) !=
                  dict_dfs[[x for x in dict_dfs.keys()][0]].shape[1]):
                var_msg = ('The length of `list_cols` is different to the '
                           'number of columns present in the table')
                logging.error(var_msg)
                raise ValueError(var_msg)
            dict_dfs = self.tables.copy()
            for key in dict_dfs.keys():
                dict_dfs[key].columns = list_cols
            self.set_table(dict_dfs, overwrite=True)
        elif var_type == 'DataFrame':
            if len(list_cols) != self.tables.shape[1]:
                var_msg = ('The length of `list_cols` is different to the '
                           'number of columns present in the table')
                logging.error(var_msg)
                raise ValueError(var_msg)
            df = self.tables.copy()
            df.columns = list_cols
            self.set_table(df, overwrite=True)
        else:
            var_msg = ('Somehow the tables are not a dictionary or a DataFrame '
                       'for function `set_headers`')
            logging.error(var_msg)
            raise ValueError(var_msg)

    def alter_table(self, **kwargs):
        # Make sure kwargs are included
        # Have a dictionary_name attribute so that it works with different named
        # dictionaries
        return self.__step_no

    def convert_columns(self, **kwargs):
        # Make sure kwargs are included
        return self.__step_no

    def assert_nulls(self, list_nulls=None):
        if list_nulls is None:
            list_nulls = ['nan', '']
        df = self.tables.copy()
        if type(df).__name__ == 'dict':
            list_keys = [x for x in df.keys()]
            for key in list_keys:
                for null in list_nulls:
                    df[key] = df[key].replace(null, np.nan)
        else:
            for null in list_nulls:
                df = df.reaplce(null, np.nan)
        self.set_table(df, overwrite=True)
