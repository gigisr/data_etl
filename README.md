# Data Curation

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A package for dealing with data curation and checks.

This can be reading in and converting to the correct dtypes, making suitable alterations to bring data into a uniform format. Or just taking an existing data set and performing some checks on it. 

The aim is to help with regular data sources provided by others. This means it could be in a different format, it could mean that you are given data that isn't logically correct, it could mean any number of problems. But hopefully having an issue report with a good amount of information on how something is wrong and where will give us capacity to provide this back to the data creatrors. Thus checks can be done in bulk and issue reports put the responsibility on the data creator to make the corrections.

The checks are not just considering single columns or single values they can consider the whole data set or even in conjunction with extra data sets, because that's how data often behaves. 

With models if certain assumptions are made then these can be tested.

There is also benefit in performing checks in bulk, even if they produce issues, so it stops the stop start process.  

## How to use this repoistory

### Setup environment

There is a YML file for the main requirements.

```
conda craete --file condaenv.yml
```

Then you can use `pip` to install the `data_curation` module, navigate to the same file as contains `setup.py` then 

```
pip install -e .
```

## Examples

There are multiple examples present in the repository in the `examples` files. 

Use the `00_create_data.py` file to create the data to run the examples on and the sqlitedb file that will contain any errors or written out data.

The other files, both `*.ipynb` and `*.py`, are the examples files.   
