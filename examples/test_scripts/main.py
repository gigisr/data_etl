# This is the section where we put all the classes together in combinations
# that are required for specific data sets
import logging
from datetime import datetime
import pickle

from data_etl import DataCuration, Checks, Connections, Reporting, \
    func_check_for_issues, func_initialise_logging

if __name__ == "__main__":
    var_key_1 = "A"
    var_key_2 = "1"
    var_key_3 = "1"
    var_start_time = datetime.now()

    var_checks_1_pass = True

    func_initialise_logging('pipeline_test_1', '../../logs/', var_key_1,
                            var_key_2, var_key_3, var_start_time)

    # Initialise objects required
    cnxs = Connections()
    data = DataCuration(var_start_time, "A")
    check = Checks(var_start_time, "A")
    reporting = Reporting(var_start_time, "A")

    # Set up connections
    cnxs.add_cnx(
        cnx_key='df_issues', cnx_type='sqlite3', table_name='df_issues',
        file_path='../../data/processed/pipeline.db')

    # Data etl testing

    # Read the files in
    data.find_files(files_path="../../data/input/test_scripts_1",
                    script_name="test_reading_in", path='.')
    data.reading_in(path=".", script_name="test_reading_in")

    # Set the step number
    data.set_step_no(1)

    # Read in the headers STYLE 1
    # data.set_headers(["number", "date_1", "date_2", "string"])

    # Read in the headers STYLE 2
    data.read_in_headers(
        path=".",
        script_name="test_reading_in",
        filepath="../../data/input/test_scripts_1/headers.xlsx")
    data.link_headers(path=".", script_name="test_reading_in")
    data.assert_linked_headers()

    data.set_step_no(2)
    data.assert_nulls([""])
    data.convert_columns(".", "convert_columns")
    func_check_for_issues(
        data.get_issue_count(2, 2), cnxs, 'df_issues', data.df_issues,
        data.get_step_no(), start_time=var_start_time)

    data.set_step_no(3)
    data.alter_tables(".", "alter_cols")
    func_check_for_issues(
        data.get_issue_count(3, 3), cnxs, 'df_issues', data.df_issues,
        data.get_step_no(), start_time=var_start_time)

    data.set_step_no(4)
    data.concatenate_tables()

    check.set_step_no(5)
    check.set_defaults(idx_flag=True)
    check.apply_checks(data.tables, ".", "checks_1")
    func_check_for_issues(
        check.get_issue_count(5, 5), cnxs, 'df_issues', check.df_issues,
        check.get_step_no(), var_checks_1_pass, var_start_time)

    # Now the data is cleansed do the reporting, this would ideally be post
    # writing to DB
    data.set_step_no(6)
    data.form_summary_tables(path='.', script_name='reporting_1')
    reporting.set_file_path('../../data/deliverables/pipeline_test_1/')
    reporting.apply_reporting(
        data.formed_tables, path='.', script_name='reporting_1')

    # Temporary snapshot for testing
    pickle.dump(
        {'data': data, 'checks': check, 'report': reporting},
        open("../../data/pickles/dict_dc.pkl", "wb"))

    # Log issues found
    cnxs.write_to_db('df_issues', data.df_issues)
    cnxs.write_to_db('df_issues', check.df_issues)

    logging.info("Script time taken: {}".format(
        str(datetime.now() - var_start_time)))
