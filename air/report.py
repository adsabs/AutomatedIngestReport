from __future__ import print_function
from __future__ import absolute_import

from builtins import str
from builtins import object

from string import Template
from .utils import Date, conf, logger


class Report(object):
    """create ingest report based on data that has been gathered and computed

    Reports (eventually) are google docs, they are created using google's python api
    """

    def __init__(self, gather, compute, date=Date.TODAY):
        self.gather = gather
        self.compute = compute

    def _text(self):
        """return text for report, including links"""
        d = {}
        d.update(self.gather.values)
        d.update(self.compute.values)
        t = Template(self._text_template).safe_substitute(d)
        return t

    def create(self):
        pass

    _text_template = '''
<html>
<body>
<p><h1>Ingest Report</h1></p>

<p></p>

<p><h2>Status</h2></p>

<p>Number of myADS emails sent today: $myads_email_count</p>

<p></p>

<p>Canonical bibcodes since yesterday: <a href="$new_canonical_file">$new_canonical</a> created, <a href="$deleted_canonical_file">$deleted_canonical</a> deleted.</p>

<p></p>

<p>Solr bibcodes since yesterday: <a href="$new_solr_file">$new_solr</a> new, <a href="$deleted_solr_file">$deleted_solr</a> deleted.  <a href="$missing_solr_file">$missing_solr</a> missing.</p>

<p>Number of canonical bibcodes: $canonical, $canonical_delta</p>
<p></p>
<p>Number of solr bibcodes: $solr, $solr_delta</p>

<p></p>

<p>Errors</p>
<p></p>
<p>Error counts from kibana:<br>
backoffice_prod_master_pipeline: $master_piperr <br>
backoffice_prod_import_pipeline: $import_piperr <br>
backoffice_prod_data_pipeline: $data_piperr <br>
backoffice_prod_fulltext_pipeline: $fulltext_piperr <br>
backoffice_prod_orcid_pipeline: $orcid_piperr <br>
backoffice_prod_citation_capture_pipeline: $citation_capture_piperr <br>
backoffice_prod_augment_pipeline: $augment_piperr <br>
backoffice_prod_myads_pipeline: $myads_piperr <br></p>

<p></p>

<p>Fulltext Error Counts: <br>
<p></p>
'extraction failed for bibcode' errors since yesterday: $new_ft_extraction_failed_for_bibcode created, $deleted_ft_extraction_failed_for_bibcode deleted. <br>
'format not currently supported for extraction' errors since yesterday: $new_ft_format_not_currently_supported_for_extraction created, $deleted_ft_format_not_currently_supported_for_extraction deleted. <br>
'is linked to a non-existent file' errors since yesterday: $new_ft_is_linked_to_a_non_existent_file created, $deleted_ft_is_linked_to_a_non_existent_file deleted. <br>
'is linked to a zero byte size file' errors since yesterday: $new_ft_is_linked_to_a_zero_byte_size_file created, $deleted_ft_is_linked_to_a_zero_byte_size_file deleted. <br>
'No such file or directory' errors since yesterday: $new_ft_No_such_file_or_directory created, $deleted_ft_No_such_file_or_directory deleted. <br></p>

<p></p>

<p>Metrics info: <br>
Number of null records = $metrics_null_count <br>
Number of updates since yesterday = $metrics_updated_count <br></p>

<p>Master/resolver Errors: $resolver_err_count</p>

<p></p>

<p><h1>Solr Report</h1></p>

<p></p>

<p>Solr index generation: $solr_indexgen <br>
Solr index size: $solr_indexsize <br></p>

<p></p>


<p>Solr on adsnest has $solr_bibcodes bibcodes <br>
Solr has $solr_cumulative_adds cumulative adds, and $solr_deleted deletions. <br>
Solr has $solr_errors errors, and $solr_cumulative_errors cumulative_errors. <br></p>

<p></p>

<p>Cause Of Solr Changes <br>
Total number of records changed: $master_total_changed <br>
Changes sent to solr: $master_solr_changed <br>
Changes from bib: $master_bib_changed <br>
Changes from fulltext: $master_fulltext_changed <br>
Changes from orcid: $master_orcid_changed <br>
Changes from nonbib: $master_nonbib_changed <br>

</body>
</html>
'''
