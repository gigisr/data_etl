# Here we are defining a class that will deal with all the data storage and
# manipulations
import os
import logging
import importlib

import pandas as pd

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
        """
        # Three keys, all good things come in threes
        # sub_file, e.g. sheet for a spreadsheet, may not always be applicable
        df_issues = pd.DataFrame(columns=[
            'key_1', 'key_2', 'key_3', 'file', 'sub_file', 'step_number',
            'issue_short_desc', 'issue_long_desc', 'issue_count', 'issue_idx'
        ])
        self.df_issues = df_issues

        self.__key_1 = key_1
        self.__key_2 = key_2
        self.__key_3 = key_3

    def error_handling(self):
        """
        If an error is handled, as they all should be, we need to specify what
        happens with the error. By putting it into a single function it will
        hopefully make the code briefer.
        """
        # TODO fill this in
        return self.df_issues

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

    def find_files(self, path, script_name, append=False,
                   func_name='list_the_files', **kwargs):
        """
        Using an externally defined function, as specified in the module
        argument script, acquire a list of files to be read in.

        In the case that we want to accumulate a list of files from different
        main paths there is an append option.
        """
        mod = importlib.import_module(script_name)
        try:
            list_files = getattr(mod, func_name)(path, **kwargs)
        except AttributeError:
            if len([x for x in kwargs.keys()]) > 0:
                var_msg = ('Function find_files, kwargs may have been passed '
                           'when the function {} in the script {} does not take'
                           ' kwargs').format(func_name, script_name)
            else:
                var_msg = ('Function find_files: the function {} is not present'
                           ' in the script {}.'.format(func_name, script_name))
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        if append:
            self.list_files = self.list_files + list_files
        else:
            self.list_files = list_files
        module_logger.info('The list of files is: {}'.format(self.list_files))

    def reading_in(self, path, script_name, func_name='read_files', **kwargs):
        """
        Using an externally defined reading in function, and the internally
        defined list of files, read in each of the tables required.

        `path` being the relative script file path
        """
        if not os.path.exists(os.path.join(path, '{}.py'.format(script_name))):
            raise ValueError('The script does not exist')
        if type(self.tables).__name__ != 'dict':
            var_msg = ('The tables need to be in dictionary format for this '
                       'reading_in step')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        mod = importlib.import_module(script_name)
        try:
            dfs = getattr(mod, func_name)(self.list_files, **kwargs)
            self.tables.update(dfs)
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
        elif ((type(self.tables).__name__ == 'DataFrame') &
              (overwrite is True)):
            self.tables = df
        else:
            var_msg = 'The combinations provided are not compatible'
            raise ValueError(var_msg)

    def concatenate_tables(self):
        """
        Where the tables are in a dictionary format put them into a DataFrame
        """
        if type(self.tables).__name__ != 'dict':
            return None
        df = pd.concat(self.tables, axis=1)
        self.tables = df

    def dictionary_tables(self, key=None):
        """
        Where the tables are in a DataFrame format put them in a dictionary,
        using the values in the key column as the new dictionary keys
        """
        if type(self.tables).__name__ != 'DataFrame':
            return None
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

    def read_in_headers(self, **kwargs):
        # TODO Need to see if we can isolate just a set of new tables? Maybe
        #  have a list of dictionary keys that have had their headers done
        #  already?
        return self.__step_no

    def link_headers(self):
        # Make sure kwargs are included
        # Need to see if we can isolate just a set of new tables? Maybe have
        # a list of dictionary keys that have had their headers done already?
        return self.__step_no

    def assert_linked_headers(self):
        # Make sure kwargs are included
        # Need to see if we can isolate just a set of new tables? Maybe have
        # a list of dictionary keys that have had their headers done already?
        return self.__step_no

    def set_headers(self):
        # Make sure kwargs are included
        return self.__step_no

    def alter_table(self):
        # Make sure kwargs are included
        # Have a dictionary_name attribute so that it works with different named
        # dictionaries
        return self.__step_no

    def convert_columns(self):
        # Make sure kwargs are included
        return self.__step_no

    def assert_nulls(self):
        return self.__step_no
