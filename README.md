# Data ETL

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A package for dealing with data curation, transformation and checks.

This can be reading in and converting to the correct dtypes, making suitable alterations to bring data into a uniform format. Or just taking an existing data set and performing some checks on it.

The aim is to help with regular data sources provided by others, or by systems. This means it could be in a flat file format, it could mean that you are given data that isn't logically correct, it could mean missing data, there could be any number of problems. But hopefully having an issue report with a good amount of information on how something is wrong and where will give us capacity to provide this back to the data creators. Thus checks can be done in bulk, quickly, and issue reports put the responsibility on the data creator to make the corrections.

The checks are not just considering single columns or single values they can consider the whole data set or even in conjunction with extra data sets, because that's how data often behaves. 

With models if certain assumptions are made then these can be tested.

There is also benefit in performing checks in bulk, even if they produce issues, so it stops the stop start process.

To use this package you should already have a good understanding of how the `pandas` package works.

## How to use this repoistory

### Setup environment

There is a YML file for the main requirements.

```
conda env create --file condaenv.yml
```

Then you can use `pip` to install the `data_etl` module, navigate to the same directory as contains the `setup.py` file then:

```
pip install -e .
```

This now means you can import `data_etl` from the environment. 

## Examples

There are multiple examples present in the repository in the `examples` files. 

Use the `00_create_data.py` file to create the data to run the examples on and the sqlitedb file that will contain any errors or written out data.

The other files, both `*.ipynb` and `*.py`, are the examples files.   

A brief code example of how to use:

```python
from data_etl import Checks
import pandas as pd

data = pd.DataFrame([1, -3, 2], columns=['number'])

# Initialise the Checks class
ch_simple = Checks('grouping_label', 'key_1', 'key_2', 'key_3')

# Define a simple check
dict_checks = {
    'Number should be greater than 0': {
        'calc_condition': lambda df, col, **kwargs: df['number'] <= 0
    }
}
# Apply the checks to the tables
ch_simple.apply_checks(data, dictionary=dict_checks)

# If any issues are found then they are stored internal to the class as a Pandas DataFrame
ch_simple.df_issues
```
