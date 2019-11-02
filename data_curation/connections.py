# Here we are defining a class that will deal with the various connections
# required by the pipeline
import logging
import sqlite3

import pandas as pd
import pyodbc

module_logger = logging.getLogger(__name__)
# TODO account for tables not existing and existing when writing to the cnx,
#  ideally any tables used should have been pre-emptively setup in the required
#  databases


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
                file_path=None, config_section=None, overwrite=False):
        module_logger.info(f"Starting `add_cnx` for cnx key `{cnx_key}`")
        # TODO query is the file existing, if not then error out
        if (cnx_key in self.__dict_cnx) & (overwrite is False):
            var_msg = ('This connection string is already set, use the '
                       'argument `overwrite=True` to overwrite')
            module_logger.error(var_msg)
            raise ValueError(var_msg)
        if cnx_type not in ['sqlite3']:
            var_msg = 'The `cnx_type` argument only takes values `sqlite3`'
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        if cnx_type == 'sqlite3':
            if table_name is None:
                var_msg = ('The argument `table_name` is required for '
                           '`cnx_type=sqlite3`')
                module_logger.error(var_msg)
                raise AttributeError(var_msg)
            if file_path is None:
                var_msg = ('The argument `file_path` is required for '
                           '`cnx_type=sqlite3`')
                module_logger.error(var_msg)
                raise AttributeError(var_msg)
            self.__dict_cnx[cnx_key] = {
                'cnx_type': cnx_type,
                'file_path': file_path,
                'table_name': table_name
            }
        self.test_cnx(cnx_key)
        module_logger.info("Completed `add_cnx`")

    def test_cnx(self, cnx_key):
        module_logger.info(f"Starting `test_cnx` for cnx key `{cnx_key}`")
        if cnx_key not in self.__dict_cnx:
            var_msg = f'The key {cnx_key} is not present'
            module_logger.error(var_msg)
            raise AttributeError(var_msg)
        var_cnx_type = self.__dict_cnx[cnx_key]['cnx_type']
        if var_cnx_type == 'sqlite3':
            cnx = sqlite3.connect(self.__dict_cnx[cnx_key]['file_path'])
            try:
                pd.read_sql(
                    (f"SELECT * FROM "
                     f"{self.__dict_cnx[cnx_key]['table_name']} LIMIT 0;"),
                    cnx
                )
                cnx.close()
            except:
                cnx.close()
                var_msg = 'Reading in from the table has not worked'
                module_logger.error(var_msg)
                raise AttributeError(var_msg)
        module_logger.info("Completed `test_cnx`")

    def write_to_db(self, cnx_key, table):
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
                cursor.execute(var_sql)
                cnx.commit()

            df_test = pd.read_sql(
                f"SELECT * FROM temp.{dict_cnx['table_name']}", cnx)

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
            cnx = sqlite3.connect(self.__dict_cnx[cnx_key]['file_path'])
            try:
                table.to_sql(self.__dict_cnx[cnx_key]['table_name'], cnx,
                             index=False, if_exists='append')
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
