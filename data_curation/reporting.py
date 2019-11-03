# Here we are going to be creating outputs of various kinds from the data we
# have checked
import logging
import os
import importlib

import pandas as pd

module_logger = logging.getLogger(__name__)

# TODO move the reporting to be either taking a full function or different
#  key arguments so it auto generates as required
#  Will want to not import everything as default, want to import only when
#  required, such as with feather in pandas
# TODO add reporting type in for :
#  # Excel
#  # # From scratch
#  # # From template
#  # HTML
#  # Email
#  # Jupyter notebook
#  # # From template add information in
#  # # Run an existing one
#  # Word document [?]
#  # PowerPoint [?]


class Reporting:
    __step_no = 0
    __file_path = None
    __grouping = None
    __key_1 = None
    __key_2 = None
    __key_3 = None
    df_issues = None
    tables = dict()

    def __init__(self, grouping, key_1, key_2=None, key_3=None):
        module_logger.info("Initialising `Reporting` object")
        # Three keys, all good things come in threes
        self.__key_1 = str(key_1)
        self.__key_2 = str(key_2)
        self.__key_3 = str(key_3)
        self.__grouping = grouping
        # Initialise the `df_issues` table
        df_issues = pd.DataFrame(
            columns=[
                "key_1", "key_2", "key_3", "file", "sub_file", "step_number",
                "issue_short_desc", "issue_long_desc", "column", "issue_count",
                "issue_idx", "grouping"
            ]
        )
        df_issues["step_number"] = df_issues["step_number"].astype(int)
        self.df_issues = df_issues
        module_logger.info("Initialising `Reporting` object complete")

    def error_handling(self, file, subfile, issue_short_desc, issue_long_desc,
                       column, issue_count, issue_idx):
        """
        If an error is handled, as they all should be, we need to specify what
        happens with the error. By putting it into a single function it will
        hopefully make the code briefer.
        """
        # TODO work out how to add in `file` and `subfile` where data is a
        #  dictionary / using the dictionary key for the subset
        module_logger.info("Logging an error with `error_handling`")
        df = self.df_issues.copy()
        list_vals = [
            self.__key_1, self.__key_2, self.__key_3, file, subfile,
            self.__step_no, issue_short_desc, issue_long_desc, column,
            issue_count, issue_idx, self.__grouping
        ]
        try:
            df.loc[df.shape[0]] = list_vals
            self.df_issues = df.copy()
        except:
            module_logger.error(
                f"Logging the issue failed, values: {list_vals}")
            raise ValueError("Logging an issue has failed, can not continue")
        module_logger.info(f"Error logged: {list_vals}")

    def set_file_path(self, file_path):
        module_logger.info("Starting `set_file_path`")
        if type(file_path).__name__ != 'str':
            var_msg = (f'The type of argument `file_path` is not `str` as '
                       f'expected it is type `{type(file_path).__name__}`')
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        if os.path.exists(file_path) is False:
            var_msg = f'The file path provided does not exist: `{file_path}`'
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        self.__file_path = file_path
        module_logger.info("Completed `set_file_path`")

    def form_tables(self, tables, function=None, path=None, script_name=None,
                    func_name="form_tables", **kwargs):
        module_logger.info("Starting `form_tables`")

        if function is not None:
            if type(function).__name__ != "function":
                var_msg = (f"The function passed to `self.form_tables` is not a"
                           f" function it is a {type(function).__name__}")
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif (script_name is not None) & (path is not None):
            if not os.path.exists(os.path.join(path, f"{script_name}.py")):
                raise ValueError("The script does not exist")
            mod = importlib.import_module(script_name)
            function = getattr(mod, func_name)
        else:
            var_msg = ("One of the `function` or `script_name` arguments needs "
                       "to be completed. And if `script name is then `path` "
                       "needs to be too.")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        try:
            dict_data = function(tables, **kwargs)
        except AttributeError:
            if len([x for x in kwargs.keys()]) > 0:
                var_msg = (f"Function form_tables, kwargs may have been passed "
                           f"when the function {func_name} in the script "
                           f"{script_name} does not take kwargs")
            else:
                var_msg = (f"Function form_tables: The {func_name} function "
                           f"does not exist in the {script_name} script.")
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        if type(dict_data).__name__ != 'dict':
            var_msg = 'The `dict_data` object is not a dictionary'
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        module_logger.info(f'The keys for the data are: {dict_data.keys()}')
        for key in dict_data:
            module_logger.info(f"The table with key `{key}` has shape "
                               f"{dict_data[key].shape}")
        self.tables = dict_data

        module_logger.info("Completed `form_tables`")

    def apply_reporting(
            self, script_name=None, path=None, object_name="dict_reporting",
            dictionary=None, **kwargs):
        module_logger.info(
            f"Starting `apply_reporting` for script {script_name}")

        if (script_name is not None) & (object_name is not None):
            if not os.path.exists(os.path.join(path, f"{script_name}.py")):
                raise ValueError("The script does not exist")
            mod = importlib.import_module(script_name)
            dict_report = getattr(mod, object_name)
        elif dictionary is not None:
            if type(dictionary).__name__ != "dict":
                var_msg = ""
                module_logger.error(var_msg)
                raise ValueError(var_msg)
            dict_report = dictionary
        else:
            var_msg = ("Either `dictionary` or both of `script_name` and "
                       "`path` need to be none null")
            module_logger.error(var_msg)
            raise ValueError(var_msg)

        for report_key in dict_report.keys():
            module_logger.info(f"Starting report `{report_key}`")
            var_report_type = dict_report[report_key]['type']
            if 'file_name' not in dict_report[report_key]:
                var_msg = (
                    'The key `file_name` is not present when it should be')
                module_logger.error(var_msg)
                raise AttributeError(var_msg)
            # TODO join the full file path together here and pass to the report
            #  functions
            if var_report_type == 'chart':
                if 'chart' not in dict_report[report_key]:
                    var_msg = 'The key `chart` is not present when it should be'
                    module_logger.error(var_msg)
                    raise AttributeError(var_msg)
                self.__report_chart(dict_report[report_key], **kwargs)
            elif var_report_type == 'map':
                if 'map' not in dict_report[report_key]:
                    var_msg = 'The key `map` is not present when it should be'
                    module_logger.error(var_msg)
                    raise AttributeError(var_msg)
                self.__report_map(dict_report[report_key], **kwargs)
            else:
                var_msg = (f'The passed type is not currently handled: '
                           f'`{var_report_type}`')
                module_logger.error(var_msg)
                raise AttributeError(var_msg)

            module_logger.info(f"Completed report `{report_key}`")

        module_logger.info("Completed `apply_reporting`")

    def __report_chart(self, dict_report, **kwargs):
        module_logger.info("Starting `__report_chart`")
        var_file_name = dict_report['file_name'](
            self.tables, self.__file_path, self.__grouping, self.__key_1,
            self.__key_2, self.__key_3, **kwargs)
        if type(var_file_name).__name__ != 'str':
            var_msg = (f'The type of `file_name` is not `str` it is '
                       f'{type(var_file_name).__name__}')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        if 'chart' in dict_report:
            dict_report['chart'](
                self.tables, self.__file_path, self.__grouping, self.__key_1,
                self.__key_2, self.__key_3, var_file_name, **kwargs)
        else:
            var_msg = ('The combination of keys is not compatible with the '
                       '`__report_chart` call')
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        module_logger.info("Completed `__report_chart`")

    def __report_map(self, dict_report, **kwargs):
        module_logger.info("Starting `__report_map`")
        var_file_name = dict_report['file_name'](
            self.tables, self.__file_path, self.__grouping, self.__key_1,
            self.__key_2, self.__key_3, **kwargs)
        if type(var_file_name).__name__ != 'str':
            var_msg = (f'The type of `file_name` is not `str` it is '
                       f'{type(var_file_name).__name__}')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        dict_report['map'](
            self.tables, self.__file_path, self.__grouping, self.__key_1,
            self.__key_2, self.__key_3, var_file_name, **kwargs)
        module_logger.info("Completed `__report_map`")

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

    def set_step_no(self, step_no):
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
