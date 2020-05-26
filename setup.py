from distutils.core import setup

setup(
    name='data_etl',
    version='0.1.0dev',
    packages=['data_etl',],
    license='MIT',
    url="https://github.com/gigisr/data_etl",
    
    author='GigiSR', requires=['pandas', 'numpy', 'pyodbc']
)
