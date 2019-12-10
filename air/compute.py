
from utils import Filename, FileType, FileAdjective, Date, comm, lines_in_file, conf, sorter, sort, remove_duplicates
import glob

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
        """Compute the new and deleted bibcodes for each type of error from
        most recent list of bibcodes compared with previous most recent list. Results stored
        in variables that are then used in report.py."""
        for err in conf['FULLTEXT_ERRORS']:

            err_msg = "_".join(err.split('"')[1].split()).replace('-', '_').replace(']', '').replace('[', '')
            dir = "data/ft/" + err_msg + '/'

            # get 2 most recent files
            files = sorted(glob.glob(dir + '*.txt'), key=sorter, reverse=True)

            sort(files[0])
            sort(files[1])          

            remove_duplicates(files[0])
            remove_duplicates(files[1])

            ft_start = files[1]
            ft_end = files[0]
            ft_new = dir + "new.tsv"
            self.values['new_ft_' + err_msg] = comm(ft_end, ft_start, ft_new)

            ft_fixed = dir + "fixed.tsv"
            self.values['fixed_ft_' + err_msg] = comm(ft_start, ft_end, ft_fixed)
