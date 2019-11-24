import os

import matplotlib.pyplot as plt
import folium


def form_tables(tables, formed_tables, grouping, key_1, key_2, key_3,
                key_separator, **kwargs):
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
    'file_name': lambda tables, file_path, grouping, key_1, key_2, key_3,
                        **kwargs: 'chart_1.png',
    'function': lambda tables, file_path, file_name, grouping, key_1, key_2,
                       key_3,**kwargs:
        func_chart_1(tables, file_path, file_name)
}
dict_reporting['Histogram 2'] = {
    'file_name': lambda tables, file_path, grouping, key_1, key_2, key_3,
                        **kwargs: 'sub_folder_test/chart_1.png',
    'function': lambda tables, file_path, file_name, grouping, key_1, key_2,
                       key_3, **kwargs:
        func_chart_1(tables, file_path, file_name)
}


def func_map_1(tables, file_path, file_name):
    df = tables['main_data']
    m = folium.Map([51.5074, 0.1278], zoom_start=12)
    for idx in df.index.tolist():
        folium.Marker([df.loc[idx, 'lat'], df.loc[idx, 'lng']]).add_to(m)
    m.save(os.path.join(file_path, file_name))
    return df


dict_reporting['Map 1'] = {
    'file_name': lambda tables, file_path, grouping, key_1, key_2,
                        key_3, **kwargs: 'map_1.html',
    'function': lambda tables, file_path, file_name, grouping, key_1, key_2,
                       key_3, **kwargs: func_map_1(tables, file_path, file_name)
}
