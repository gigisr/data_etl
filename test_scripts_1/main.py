# This is the section where we put all the classes together in combinations
# that are required for specific data sets

import os
import logging
from datetime import datetime
import pickle

import pandas as pd

import connections
from data_curation import DataCuration, Checks

# TODO add in logging
# TODO data_files class working
# TODO checks class working
# TODO connections class working
# TODO reporting class working
# TODO some sort of checking class working
# TODO set up a dedicated environment for this

if __name__ == "__main__":
    var_key_1 = "A"
    var_key_2 = "1"
    var_key_3 = "1"
    var_start_time = datetime.now()

    logname = os.path.abspath(
        (f"../logs/pipeline_test_1_{var_key_1}_{var_key_2}_{var_key_3}"
         f"_{var_start_time.strftime('%Y%m%d_%H%M%S')}.log"))
    logging.basicConfig(
        filename=logname, filemode="a", datefmt="%H:%M:%S", level=logging.DEBUG,
        format="%(asctime)s|%(name)s|%(levelname)s|%(message)s")

    logging.info(
        f"Starting the process at {var_start_time.strftime('%Y%m%d_%H%M%S')}")

    # Testing shortcut
    cnxs = connections.Connections()
    data = DataCuration(var_start_time, "A")
    check = Checks(var_start_time, "A")

    # Data curation testing

    # Read the files in
    data.find_files("../data/input/test_scripts_1", "test_reading_in")
    data.reading_in(path=".", script_name="test_reading_in")

    # Set the step number
    data.set_step_no(1)

    # Read in the headers STYLE 1
    # data.set_headers(["number", "date_1", "date_2", "string"])

    # Read in the headers STYLE 2
    data.read_in_headers(
        path=".",
        script_name="test_reading_in",
        filepath="../data/input/test_scripts_1/headers.xlsx"
    )
    data.link_headers(path=".", script_name="test_reading_in")
    data.assert_linked_headers()

    data.set_step_no(2)
    data.assert_nulls([""])
    data.convert_columns("convert_columns", ".")
    if data.get_issue_count(2, 2) > 0:
        # TODO once connections is in place add a call here
        pickle.dump(data, open("../pickles/df.pkl", "wb"))
        var_msg = (f'There were {data.get_issue_count()} issues found at step '
                   f'{data.get_step_no()}')
        logging.error(var_msg)
        raise ValueError(var_msg)

    data.set_step_no(3)
    data.alter_tables("alter_cols", ".")
    if data.get_issue_count(3, 3) > 0:
        # TODO once connections is in place add a call here
        pickle.dump(data, open("../pickles/df.pkl", "wb"))
        var_msg = (f'There were {data.get_issue_count()} issues found at step '
                   f'{data.get_step_no()}')
        logging.error(var_msg)
        raise ValueError(var_msg)

    data.set_step_no(4)
    data.concatenate_tables()
    pickle.dump(data, open("../pickles/df.pkl", "wb"))
    data.append_table(data.tables)

    check.set_step_no(5)
    check.apply_checks(data.tables, "checks_1", ".")
    check.table_look(data.tables, 0)

    pickle.dump(
        {'data': data, 'checks': check},
        open("../pickles/dict_dc.pkl", "wb"))

    logging.info("Script time taken: {}".format(
        str(datetime.now() - var_start_time)))
