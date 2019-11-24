# Here functions that are typically used when using these scripts or writing
# these data curation scripts are predefined here
import logging
import os
from datetime import datetime

import pandas as pd

module_logger = logging.getLogger(__name__)


def func_initialise_logging(script_name, log_folder_path, var_key_1, var_key_2,
                            var_key_3, var_start_time):
    var_log_name = os.path.abspath(
        os.path.join(
            log_folder_path,
            (f"{script_name}_{var_key_1}_{var_key_2}_{var_key_3}_"
             f"{var_start_time.strftime('%Y%m%d_%H%M%S')}.log")
        )
    )
    logging.basicConfig(
        filename=var_log_name, filemode="a", datefmt="%H:%M:%S",
        level=logging.DEBUG,
        format="%(asctime)s|%(name)s|%(levelname)s|%(message)s")

    logging.info(f"Starting the process at "
                 f"{var_start_time.strftime('%Y-%m-%d %H:%M:%S')}")


def func_check_for_issues(issue_count, cnx, cnx_key, table, step_no,
                          override=False, start_time=None):
    if (issue_count > 0) & (override is not True):
        cnx.write_to_db(cnx_key, table)
        var_msg = f'There were {issue_count} issues found at step {step_no}'
        module_logger.error(var_msg)
        if start_time is not None:
            module_logger.info("Script time taken: {}".format(
                str(datetime.now() - start_time)))
        raise ValueError(var_msg)


def func_to_sql(x, datetime_format='%Y-%m-%d'):
    if pd.isnull(x):
        return "NULL"
    elif type(x).__name__ == 'Timestamp':
        return f"'{x.strftime(datetime_format)}'"
    else:
        return f"'{str(x)}'"
