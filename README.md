# etl_transform
transform files data based on given schemas and create .ndjson files as result

## Assumptions
* All data files will be on .txt extension and follow the naming convention of: `<name of schema>_<yyyy>-<mm>-<dd>.txt`
* All specification file will be created on `csv` format and will include the following schema: `column name, width, datatype`
* Not to process same data files if already processed in previous runs

## Design Notes
* Data files will be processed in a loop. Batch processing.
* Process will exclude files that were already processed
* Process will no stop for basic errors (schema not found, data file empty, etc) but it will log the error and continue with the next file. We don't want to create a bootle neck, just because one file can not be processed.
* Created a simple txt file where to keep the name of the files that were already processed. However, it can be done in many different ways depending on the requirements of the business. For example, File names processed can be kept in a database (tracking table), it could be something simple as a SQLite file, accompanying the scrip. Another, way could be just moving the files to a different folder (processed folder) once the data file is processed (method implemented in the code). This last approach can speed up processing time since it will not have to check for files already processed.

## Set up project
* Clone source code `[address](https://github.com/RicardoASP/etl_transform.git)`
* Set up a virtual environment for project `virtualenv venv`
* Install project requirements file `pip install -r requirements.txt`. 
