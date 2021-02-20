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
        # service = build('drive', 'v3', developerKey='')
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
Number of myADS emails sent today: $myads_email_count

Canonical bibcodes since yesterday: $new_canonical created, $deleted_canonical deleted.

Solr bibcodes since yesterday: $new_solr new, $deleted_solr deleted.  $missing_solr missing.

Error counts from kibana:
backoffice_prod_master_pipeline: $master_piperr
backoffice_prod_import_pipeline: $import_piperr
backoffice_prod_data_pipeline: $data_piperr
backoffice_prod_fulltext_pipeline: $fulltext_piperr
backoffice_prod_orcid_pipeline: $orcid_piperr
backoffice_prod_citation_capture_pipeline: $citation_capture_piperr
backoffice_prod_augment_pipeline: $augment_piperr
backoffice_prod_myads_pipeline: $myads_piperr

Fulltext Error Counts:
'extraction failed for bibcode' errors since yesterday: $new_ft_extraction_failed_for_bibcode created, $deleted_ft_extraction_failed_for_bibcode deleted.
'format not currently supported for extraction' errors since yesterday: $new_ft_format_not_currently_supported_for_extraction created, $deleted_ft_format_not_currently_supported_for_extraction deleted.
'is linked to a non-existent file' errors since yesterday: $new_ft_is_linked_to_a_non_existent_file created, $deleted_ft_is_linked_to_a_non_existent_file deleted.
'is linked to a zero byte size file' errors since yesterday: $new_ft_is_linked_to_a_zero_byte_size_file created, $deleted_ft_is_linked_to_a_zero_byte_size_file deleted.
'No such file or directory' errors since yesterday: $new_ft_No_such_file_or_directory created, $deleted_ft_No_such_file_or_directory deleted.

Metrics info:
Number of null records = $metrics_null_count
Number of updates since yesterday = $metrics_updated_count

Master/resolver Errors: $resolver_err_count

Solr index generation: $solr_indexgen
Solr index size: $solr_indexsize

Solr on adsnest has $solr_bibcodes bibcodes
Solr has $solr_cumulative_adds cumulative adds,
         $solr_deleted deletions,
         $solr_errors errors,
     and $solr_cumulative_errors cumulative_errors.

Cause Of Solr Changes
Total number of records changed: $master_total_changed
Changes sent to solr: $master_solr_changed
Changes from bib: $master_bib_changed
Changes from fulltext: $master_fulltext_changed
Changes from orcid: $master_orcid_changed
Changes from nonbib: $master_nonbib_changed
    '''

    # https://developers.google.com/drive/v3/web/quickstart/python
    # https://developers.google.com/drive/v3/web/simple-upload
    # https://developers.google.com/api-client-library/python/apis/script/v1
    # https://developers.google.com/resources/api-libraries/documentation/drive/v3/python/latest/
