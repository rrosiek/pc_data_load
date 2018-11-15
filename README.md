# PC Data Load

## Configuration file

**data_load/DATA_LOAD_CONFIG.py**

*You can configure the elasticsearch url and local directory to where the data source files are downloaded.*

## Setup & Usage

**Use Python 2.7**

```pip install -r requirements.txt```

```python -u -m data_load.pubmed2018.pubmed_load_manager -n <number of files to process>```

