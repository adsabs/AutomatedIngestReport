
from utils import Filename, FileType, FileAdjective, Date, comm, lines_in_file


class Compute:
    """compute missing bibcodes and other values"""

    def __init__(self, start=Date.YESTERDAY, end=Date.TODAY):
        self.start = start
        self.end = end
        self.values = {}

    def canonical(self):
        """compute new, deleted"""
        canonical_start = Filename.get(self.start, FileType.CANONICAL)
        canonical_end = Filename.get(self.end, FileType.CANONICAL)
        canonical_new = Filename.get(self.end, FileType.CANONICAL, FileAdjective.NEW)
        self.values['new_canonical'] = comm(canonical_end, canonical_start, canonical_new)

        canonical_deleted = Filename.get(self.end, FileType.CANONICAL, FileAdjective.DELETED)
        self.values['deleted_canonical'] = comm(canonical_start, canonical_end, canonical_deleted)

        self.values['canonical'] = lines_in_file(canonical_end)

    def solr(self):
        """compute missing, deleted, new, extra"""
        solr_end = Filename.get(self.end, FileType.SOLR)
        canonical_end = Filename.get(self.end, FileType.CANONICAL)
        solr_missing = Filename.get(self.end, FileType.SOLR, FileAdjective.MISSING)
        self.values['missing_solr'] = comm(canonical_end, solr_end, solr_missing)

        solr_start = Filename.get(self.start, FileType.SOLR)
        solr_new = Filename.get(self.end, FileType.SOLR, FileAdjective.NEW)
        self.values['new_solr'] = comm(solr_end, solr_start, solr_new)

        solr_deleted = Filename.get(self.end, FileType.SOLR, FileAdjective.DELETED)
        self.values['deleted_solr'] = comm(solr_start, solr_end, solr_deleted)

        solr_extra = Filename.get(self.end, FileType.SOLR, FileAdjective.EXTRA)
        self.values['extra_solr'] = comm(solr_end, canonical_end, solr_extra)

        self.values['solr'] = lines_in_file(solr_end)

    def fulltext(self):
        """compute new, deleted"""
        for e in conf['ERRORS'].keys():

            err_msg = "_" + "_".join(e.split())

            ft_start = Filename.get(self.start, FileType.FULLTEXT, adjective=None, msg=err_msg + "_")
            ft_end = Filename.get(self.end, FileType.FULLTEXT, adjective=None, msg=err_msg + "_")
            ft_new = Filename.get(self.end, FileType.FULLTEXT, adjective=FileAdjective.NEW, msg=err_msg + "_")
            self.values['new_ft' + err_msg] = comm(ft_end, ft_start, ft_new)

            ft_deleted = Filename.get(self.end, FileType.FULLTEXT, FileAdjective.DELETED, msg=err_msg + "_")
            self.values['deleted_ft' + err_msg] = comm(ft_start, ft_end, ft_deleted)

            self.values['ft' + err_msg] = lines_in_file(ft_end)
