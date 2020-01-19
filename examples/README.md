# Examples

A collection of examples for potential uses of my package!

A lot of the functionality is easy to code in yourself and is dependant on the data set in use. But I have found it useful to be able to try to apply all conversions at once and then check that there were no errors rather than stop each time there is an error. For example, knowing exactly which columns failed to convert to integer means you can investigate all of them at once.

The main use I have for using this package at work is so I can feed back to the data creators where there are errors in their manually entered data sets so they can make corrections to it before I have to use it. And if there are values that break my assumptions but are actually valid values I get the feedback from the domain experts that can help me modify my assumptions, or keep the check as is because it's a highly unlikely occurance and it's good to know when it's cropped up. So, although the problems are labelled as being in a `issues log` they could just be flags for unusual or specific values of particular interest.

# The structure

`data/` will contain any generated data we need, some of the tables may be pre-existing hard coded ones
`test_scripts/` contains an example in scripts rather than notebooks, from this form which runs well locally you can easily convert it into an Airflow compatible form, the `main.py` script accesses all the other scripts so you only need to run one
`00_create_data.ipynb` creates the data and dbs that are used in the examples
`01_example.ipnb` a look at some basic functionality: finding files, reading in the data, setting new headers, asserting nulls, then converting to the correct dtypes
`02_example.ipynb` a concentrated look at individual bits of functionality available and a look at the issue output produced when there are problems
`02_example.py` some externally defined information to use in the `02_example.ipynb` notebook for one of the sections
