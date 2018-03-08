
# Automated Ingest Report

It is important to closely monitor the state of ingest related data
stores, especially Solr.  This repo holds code that daily gathers 
the list of canonical bibcodes and current bibcodes in Solr to
compute what is missing, what is new, what is deleted, etc.  

## Running
To gather all the needed data and compute state:
`python run.py --gather --compute`

## Maintainer
Steve McDonald