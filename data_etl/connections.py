# Here we are defining a class that will deal with the various connections
# required by the pipeline
import logging
import sqlite3
import os
import configparser

import pandas as pd
import pyodbc

from .general_functions import func_to_sql

module_logger = logging.getLogger(__name__)
# TODO account for tables not existing and existing when writing to the cnx,
#  ideally any tables used should have been pre-emptively setup in the required
#  databases
# TODO add MSSQL connection handling


class Connections:
    __step_no = 0
    __df_issues = None
    __dict_cnx = {
        'blank': {'cnx_type': 'blank'}
    }

    def __init__(self, step_no=None):
        module_logger.info("Initialising `Connections` object")
        if step_no is not None:
            self.set_step_no(step_no)
        module_logger.info("Initialising `Connections` object complete")

    def set_step_no(self, step_no):
        module_logger.info(f"Starting `set_step_no`")
        self.__step_no = step_no
        module_logger.info(f"Completed `set_step_no`")

    def get_step_no(self):
        module_logger.info("Starting `get_step_no`")
        module_logger.info("Completed `get_step_no`")
        return self.__step_no

    def add_cnx(self, cnx_key, cnx_type, table_name, cnx_string=None,
                file_path=None, config_section=None, overwrite=False,
                timestamp_format='%Y-%m-%d', **kwargs):
        module_logger.info(f"Starting `add_cnx` for cnx key `{cnx_key}`")
        # TODO query is the file existing, if not then error out
        if (cnx_key in self.__dict_cnx) & (overwrite is False):
            var_msg = ('This connection string is already set, use the '
                       'argument `overwrite=True` to overwrite')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        if cnx_type not in ['sqlite3', 'db']:
            var_msg = 'The `cnx_type` argument only takes values `sqlite3`'
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        if table_name is None:
            var_msg = 'The argument `table_name` is required'
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        if (file_path is None) & (cnx_type in ['sqlite3', 'db']):
            var_msg = 'The argument `file_path` is required'
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        if (not os.path.exists(file_path)) & (cnx_type in ['db']):
            var_msg = f'The `file_path` {file_path} is not valid'
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        if ((not os.path.exists(os.path.dirname(file_path))) &
            (cnx_type in ['sqlite3'])):
            var_msg = (
                f'The fodler path {os.path.dirname(file_path)} is not valid')
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        if (not os.path.exists(file_path)) & (cnx_type in ['sqlite3']):
            var_msg = (f'The `file_path` {file_path} is not valid so this '
                       f'file will be created')
            module_logger.warning(var_msg)
        if cnx_type == 'sqlite3':
            module_logger.info(
                f'The information is: {cnx_type}, {file_path}, {table_name}')
            self.__dict_cnx[cnx_key] = {
                'cnx_type': cnx_type,
                'file_path': file_path,
                'table_name': table_name
            }
        elif cnx_type == 'db':
            if (config_section is None) & (cnx_string is None):
                var_msg = ('The argument `config_section` or `cnx_string` is '
                           'required for `cnx_type=db`')
                module_logger.error(var_msg)
                raise AttributeError(var_msg)
            if config_section is not None:
                dict_config = configparser.ConfigParser()
                dict_config.read(file_path)
                var_cnx_string = ''.join(
                    [
                        f"{key}={dict_config[config_section][key]};" for
                        key in dict_config[config_section]
                    ]
                )
                self.__dict_cnx[cnx_key] = {
                    'cnx_type': cnx_type,
                    'file_path': file_path,
                    'cnx_string': var_cnx_string ,
                    'table_name': table_name,
                    'timestamp_format': timestamp_format
                }
            elif cnx_string is not None:
                self.__dict_cnx[cnx_key] = {
                    'cnx_type': cnx_type,
                    'file_path': file_path,
                    'cnx_string': cnx_string,
                    'table_name': table_name,
                    'timestamp_format': timestamp_format
                }
        self.test_cnx(cnx_key, **kwargs)
        module_logger.info("Completed `add_cnx`")

    def test_cnx(self, cnx_key, **kwargs):
        module_logger.info(f"Starting `test_cnx` for cnx key `{cnx_key}`")
        if cnx_key not in self.__dict_cnx:
            var_msg = f'The key {cnx_key} is not present'
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        dict_cnx = self.__dict_cnx[cnx_key]
        var_cnx_type = dict_cnx['cnx_type']
        if var_cnx_type == 'sqlite3':
            cnx = sqlite3.connect(dict_cnx['file_path'])
            if kwargs.get('sqlite_df_issues_create') is True:
                var_create_table_sql = """
                CREATE TABLE IF NOT EXISTS {} (
                    key_1 text,
                    key_2 text,
                    key_3 text,
                    file text,
                    sub_file text,
                    step_number integer,
                    category text,
                    issue_short_desc text,
                    issue_long_desc text,
                    column text,
                    issue_count integer,
                    issue_idx text,
                    grouping text
                );
                """.format(dict_cnx['table_name'])
                cnx.execute(var_create_table_sql)
            try:
                pd.read_sql(
                    f"SELECT * FROM {dict_cnx['table_name']} LIMIT 0;",
                    cnx
                )
                cnx.close()
            except:
                cnx.close()
                var_msg = 'Reading in from the table has not worked'
                module_logger.error(var_msg)
                raise AttributeError(var_msg)
        elif var_cnx_type == 'db':
            cnx = pyodbc.connect(dict_cnx['cnx_string'])
            try:
                pd.read_sql(
                    f"SELECT TOP (0) * FROM {dict_cnx['table_name']};",
                    cnx
                )
                cnx.close()
            except:
                cnx.close()
        module_logger.info("Completed `test_cnx`")

    def read_from_db(self, cnx_key, sql_stmt):
        module_logger.info("Starting `read_from_db`")
        module_logger.info(f'Sql statement: {sql_stmt}')
        dict_cnx = self.__dict_cnx[cnx_key]
        var_cnx_type = dict_cnx['cnx_type']
        df = pd.DataFrame()
        if var_cnx_type == 'blank':
            var_msg = 'Trying to use `read_from_db` using a blank connection'
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        elif var_cnx_type == 'sqlite3':
            cnx = sqlite3.connect(dict_cnx['file_path'])
            try:
                df = pd.read_sql(sql_stmt, cnx)
                cnx.close()
            except:
                cnx.close()
                var_msg = 'Reading in using a `sqlite3` connection has failed'
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif var_cnx_type == 'db':
            cnx = pyodbc.connect(dict_cnx['cnx_string'])
            try:
                df = pd.read_sql(sql_stmt, cnx)
                cnx.close()
            except:
                cnx.close()
                var_msg = 'Reading in using a `db` connection has failed'
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        module_logger.info("Completed `read_from_db`")
        return df

    def write_to_db(self, cnx_key, table, batch_size=None,
                    flag_sql_logging=False):
        module_logger.info("Starting `write_to_db`")
        dict_cnx = self.__dict_cnx[cnx_key]
        var_cnx_type = dict_cnx['cnx_type']
        # Temp table first
        var_write_works = 0
        if var_cnx_type == 'blank':
            var_write_works += 1
        elif var_cnx_type == 'sqlite3':
            cnx = sqlite3.connect(dict_cnx['file_path'])
            cursor = cnx.cursor()
            var_sql = (f"CREATE TEMP TABLE temp.{dict_cnx['table_name']} AS "
                       f"SELECT * FROM {dict_cnx['table_name']} LIMIT 0;")
            module_logger.info(var_sql)
            cursor.execute(var_sql)
            cnx.commit()
            for idx in table.index.tolist():
                var_sql = "INSERT INTO temp.{} VALUES ({});".format(
                    dict_cnx['table_name'],
                    ', '.join(
                        table.loc[idx].map(
                            lambda value: 'NULL' if pd.isnull(value) else
                            f"'{str(value)}'"
                        ).astype(str).values.tolist()
                    )
                )
                if flag_sql_logging:
                    module_logger.info(var_sql)
                cursor.execute(var_sql)
                cnx.commit()

            df_test = pd.read_sql(
                f"SELECT * FROM temp.{dict_cnx['table_name']}", cnx)

            if df_test.shape[0] == table.shape[0]:
                var_write_works += 1

            cnx.close()
        elif var_cnx_type == 'db':
            cnx = pyodbc.connect(dict_cnx['cnx_string'])
            cursor = cnx.cursor()

            var_sql = (f"DROP TABLE IF EXISTS #Temp "
                       f"SELECT TOP(0) * INTO #Temp "
                       f"FROM {dict_cnx['table_name']}")
            module_logger.info(var_sql)
            cursor.execute(var_sql)
            cnx.commit()

            var_sql_template = "INSERT INTO #Temp ([{}]) VALUES {}".format(
                "], [".join(table.columns.tolist()),
                '{}'
            )
            module_logger.info(var_sql_template)
            s_sql_values = table.apply(
                lambda s: s.map(
                    lambda x: func_to_sql(x, dict_cnx['timestamp_format']))
            ).apply(
                lambda r: f"({', '.join(r)})", axis=1)
            var_iloc_min = 0
            for i in range(1, int(s_sql_values.shape[0] / batch_size) + 2):
                s_filtered = s_sql_values.iloc[
                             var_iloc_min:(i * batch_size)]
                var_sql = var_sql_template.format(
                    ", ".join(s_filtered.values.tolist()))
                if flag_sql_logging:
                    module_logger.info(var_sql)
                cursor.execute(var_sql)
                cnx.commit()
                var_iloc_min = i * batch_size

            df_test = pd.read_sql("SELECT * FROM #Temp", cnx)

            if df_test.shape[0] == table.shape[0]:
                var_write_works += 1

            cnx.close()

        if var_write_works == 0:
            var_msg = ('The writing to a temporary table has not worked, '
                       'will not try writing to main table')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        if var_write_works > 1:
            var_msg = ('The writing to a temporary table has happened '
                       'multiple times, will not try writing to main table')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        # Then move to the main table only if the temporary table worked
        if var_cnx_type == 'blank':
            pass
        elif var_cnx_type == 'sqlite3':
            cnx = sqlite3.connect(dict_cnx['file_path'])
            try:
                table.to_sql(dict_cnx['table_name'], cnx,
                             index=False, if_exists='append')
                cnx.close()
            except:
                cnx.close()
                var_msg = 'Writing to the table has not worked'
                module_logger.error(var_msg)
                raise ValueError(var_msg)
        elif var_cnx_type == 'db':
            cnx = pyodbc.connect(dict_cnx['cnx_string'])
            cursor = cnx.cursor()
            try:
                var_sql_template = "INSERT INTO {} ({}) VALUES {}".format(
                    dict_cnx['table_name'],
                    ", ".join(table.columns.tolist()),
                    '{}'
                )
                s_sql_values = table.apply(
                    lambda s: s.map(
                        lambda x: func_to_sql(x, dict_cnx['timestamp_format']))
                ).apply(
                    lambda r: f"({', '.join(r)})", axis=1)
                var_iloc_min = 0
                for i in range(1, int(s_sql_values.shape[0] / batch_size) + 2):
                    s_filtered = s_sql_values.iloc[
                        var_iloc_min:(i * batch_size)]
                    var_sql = var_sql_template.format(
                        ", ".join(s_filtered.values.tolist()))
                    cursor.execute(var_sql)
                    cnx.commit()
                    var_iloc_min = i * batch_size
                cnx.close()
            except:
                cnx.close()
                var_msg = 'Writing to the table has not worked'
                module_logger.error(var_msg)
                raise ValueError(var_msg)

        module_logger.info("Completed `write_to_db`")

    def get_cnx_keys(self):
        module_logger.info("Starting `get_cnx_keys`")
        module_logger.info("Completed `get_cnx_keys`")
        return [x for x in self.__dict_cnx.keys()]
