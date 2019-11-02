# Here functions that are typically used when using these scripts or writing
# these data curation scripts are predefined here
import logging
import os

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


def func_check_for_issues(
        issue_count, cnx, cnx_key, table, step_no, override=False):
    if (issue_count > 0) & (override is not True):
        cnx.write_to_db(cnx_key, table)
        var_msg = f'There were {issue_count} issues found at step {step_no}'
        module_logger.error(var_msg)
        raise ValueError(var_msg)
