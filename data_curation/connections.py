# Here we are defining a class that will deal with the various connections
# required by the pipeline
import os
import win32com.client
import sqlite3
import sys

import pandas as pd


class Connections:
    __step_no = 0
    __df_issues = None
    __cnx_dict = dict()

    def __init__(self):
        df_issues = pd.DataFrame(columns=[
            'key_1', 'key_2', 'key_3', 'file', 'sub_file', 'step_number',
            'issue_short_desc', 'issue_long_desc', 'issue_count', 'issue_idx'
        ])
        self.__df_issues = df_issues

    def set_step_no(self, step_no):
        self.__step_no = step_no

    @staticmethod
    def follow_shortcut(path):
        shell = win32com.client.Dispatch("WScript.Shell")
        shortcut = shell.CreateShortCut(path)
        return shortcut.targetpath

    def add_cnx(self, cnx_id, cnx_type, path=None):
        # TODO check id doesn't already exist
        if path is not None:
            if not os.path.exists(path):
                raise ValueError('Path provided does not exist...')
        if cnx_type.lower() == 'shortcut':
            dict_out = {
                'type': 'shortcut',
                'path': os.path.abspath(self.follow_shortcut(path))
            }
        elif cnx_type.lower() == 'sqlite':
            dict_out = {
                'type': 'sqlite',
                'path': os.path.abspath(path)
            }
        elif cnx_type.lower() == 'script':
            dict_out = {
                'type': 'script',
                'path': os.path.abspath(path)
            }
            sys.path.insert(0, dict_out['path'])
        else:
            raise ValueError('The provided type is not provided for...')
        self.__cnx_dict[cnx_id] = dict_out

    def open_cnx(self, cnx_id):
        dict_use = self.__cnx_dict[cnx_id]
        var_type = dict_use['type']
        if var_type == 'shortcut':
            return dict_use['path']
        elif var_type == 'sqlite':
            return sqlite3.connect(dict_use['path'])
        else:
            raise ValueError('Not a valid cnx_id:', cnx_id)

    def close_cnx(self, cnx_id, cnx=None):
        var_type = self.__cnx_dict[cnx_id]['type']
        if var_type == 'sqlite':
            cnx.close()

    def get_cnx_ids(self):
        return [x for x in self.__cnx_dict.keys()]
