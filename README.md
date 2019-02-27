# PC Data Load

## Configuration file

**data_load/DATA_LOAD_CONFIG.py**

*You can configure the elasticsearch url and local directory to where the data source files are downloaded.*

## Setup & Usage

**Use Python 2.7**

```pip install -r requirements.txt```

### PubMed

```python -u -m data_load.pubmed2018.pubmed_load_manager -n <number of files to process>```

### PubMed 2019 - Baseline + Relationships

```python -u -m data_load.pubmed2019.pubmed_load_manager -baseline```

### Proposal Central

```python -u -m data_load.proposal_central.pc_load_manager -path <path to csv file>```

### Grants.gov

```python -u -m data_load.grants.grants_load_manager -auto```

### Clinical Trials

```python -m data_load.clinical_trials.ct_load_manager -auto```

### USPTO

```python -m data_load.uspto.uspto_load_manager -auto``` - Downloads and starts loading files from 2001 onwards.

**Specific year**

```python -m data_load.uspto.uspto_load_manager -auto 2019```

### Crossref

```python -m data_load.crossref.crossref_load_manager -auto``` - Loads Works for "Nature" & "ScienceOpen"

```python -m data_load.crossref.events_load_manager -auto``` - Loads Events for "Nature" & "ScienceOpen"


