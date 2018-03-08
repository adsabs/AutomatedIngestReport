
#!/bin/bash

# create files with .good extension for testing
# the unit test code compares generated results to .good files

comm -2 -3 20000102Canonical.txt 20000101Canonical.txt > 20000102newCanonical.good
comm -1 -3 20000102Canonical.txt 20000101Canonical.txt > 20000102deletedCanonical.good

comm -2 -3 20000102Canonical.txt 20000102Solr.txt > 20000102missingSolr.good
comm -2 -3 20000102Solr.txt 20000101Solr.txt > 20000102newSolr.good
comm -1 -3 20000102Solr.txt 20000101Solr.txt > 20000102deletedSolr.good