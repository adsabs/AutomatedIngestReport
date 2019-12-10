
# Automated Ingest Report

It is important to closely monitor the state of ingest related data
stores, especially Solr.  This repo holds code that daily gathers 
the list of canonical bibcodes and current bibcodes in Solr to
compute what is missing, what is new, what is deleted, etc.  

## Running
To gather all the needed data and compute state:
`python run.py --gather --compute`

### Fulltext Section
- Errors are defined in the config file
  - new errors can be added to this list
- Results will only change if the pipeline has processed all.links since the last AIR
  - we assume the location of all.links to be /proj/ads/abstracts/config/links/fulltext/all.links 
  - There is a date in the report indicating the date of the last fulltext extraction 

- This directory structure needs to exist for files to be stored:

  ```bash
  data
  └── ft
      ├── Errno_2_No_such_file_or_directory
      ├── extraction_failed_for_bibcode
      ├── format_not_currently_supported_for_extraction
      ├── is_linked_to_a_non_existent_file
      └── is_linked_to_a_zero_byte_size_file
  ```

## Maintainer
Steve McDonald
