# Solution Overview

This solution consists of two python modules **task1.py** and **task2.py**. The solution developed using Apache Beam to process the CSV file and caliculate the total amounts by date and write the output as csv(compressed file).

## Assumptions

1. Python 3.7 and above has installed
2. You have a google cloud account account and you can access to the 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'

### task1.py module

This is solution developed using Apache Beam without Composite Transform approach.

### task2.py module

This is solution developed using Apache Beam with Composite Transform approach.

## Set up

```
python -m venv .venv
source .venv/bin/activate
pip install apache-beam
pip install apache-beam[gcp]
```
if you are facing any issue  please follow this link to setup the apache beam https://beam.apache.org/get-started/quickstart-py/

## Run guide
```
python -m task1 --input_file gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv
```