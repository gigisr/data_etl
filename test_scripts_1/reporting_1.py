import os

import matplotlib.pyplot as plt


def form_tables(tables, **kwargs):
    dict_data = dict()
    dict_data['main_data'] = tables.copy()
    return dict_data


dict_reporting = dict()


def func_chart_1(tables, file_path, file_name):
    df = tables['main_data']
    plt.figure()
    g = df['number_2'].hist(bins=50)
    plt.title('Histogram')
    plt.savefig(os.path.join(file_path, file_name))
    return None


dict_reporting['Histogram 1'] = {
    'type': 'chart',
    'file_name': lambda tables, file_path, grouping, key_1, key_2, key_3,
        **kwargs: 'chart_1.png',
    'chart': lambda tables, file_path, grouping, key_1, key_2, key_3, file_name,
        **kwargs: func_chart_1(tables, file_path, file_name)
}
dict_reporting['Histogram 2'] = {
    'type': 'chart',
    'file_name': lambda tables, file_path, grouping, key_1, key_2, key_3,
        **kwargs: 'sub_folder_test/chart_1.png',
    'chart': lambda tables, file_path, grouping, key_1, key_2, key_3, file_name,
        **kwargs: func_chart_1(tables, file_path, file_name)
}
