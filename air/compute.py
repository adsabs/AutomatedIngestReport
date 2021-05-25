from __future__ import absolute_import

import os.path
from builtins import object
from .utils import Filename, FileType, FileAdjective, Date, comm, lines_in_file, conf, GoogleUploader


class Compute(object):
    """compute missing bibcodes and other values"""

    def __init__(self, start=Date.YESTERDAY, end=Date.TODAY):
        self.start = start
        self.end = end
        self.values = {}

    def canonical(self):
        up = GoogleUploader()
        url_string = conf.get('GOOGLE_URL_BASE', '')
        fold_id = conf.get('GOOGLE_DATA_FOLDER', '')

        """compute new, deleted"""
        canonical_start = Filename.get(self.start, FileType.CANONICAL)
        canonical_end = Filename.get(self.end, FileType.CANONICAL)
        canonical_new = Filename.get(self.end, FileType.CANONICAL, FileAdjective.NEW)
        self.values['new_canonical'] = comm(canonical_end, canonical_start, canonical_new)
        if os.path.exists(canonical_new):
            try:
                fileid = up.upload_file(infile=canonical_new, folderID=fold_id)
                new_canonical_file = url_string + fileid
                self.values['new_canonical_file'] = new_canonical_file
            except Exception as err:
                print("Err in compute: %s" % err)

        canonical_deleted = Filename.get(self.end, FileType.CANONICAL, FileAdjective.DELETED)
        self.values['deleted_canonical'] = comm(canonical_start, canonical_end, canonical_deleted)
        if os.path.exists(canonical_deleted):
            try:
                fileid = up.upload_file(infile=canonical_deleted, folderID=fold_id)
                deleted_canonical_file = url_string + fileid
                self.values['deleted_canonical_file'] = deleted_canonical_file
            except Exception as err:
                print("Err in compute: %s" % err)

        self.values['canonical'] = lines_in_file(canonical_end)
        delta = self.values['canonical'] - lines_in_file(canonical_start)
        if delta < 0:
            self.values['canonical_delta'] = str(abs(delta)) + ' fewer.'
        else:
            self.values['canonical_delta'] = str(abs(delta)) + ' more.'

    def solr(self):
        """compute missing, deleted, new, extra"""

        up = GoogleUploader()
        url_string = conf.get('GOOGLE_URL_BASE', '')
        fold_id = conf.get('GOOGLE_DATA_FOLDER', '')

        solr_end = Filename.get(self.end, FileType.SOLR)
        canonical_end = Filename.get(self.end, FileType.CANONICAL)
        solr_missing = Filename.get(self.end, FileType.SOLR, FileAdjective.MISSING)
        self.values['missing_solr'] = comm(canonical_end, solr_end, solr_missing)
        if os.path.exists(solr_missing):
            try:
                fileid = up.upload_file(infile=solr_missing, folderID=fold_id)
                missing_solr_file = url_string + fileid
                self.values['missing_solr_file'] = missing_solr_file
            except Exception as err:
                print("Err in compute: %s" % err)

        solr_start = Filename.get(self.start, FileType.SOLR)
        solr_new = Filename.get(self.end, FileType.SOLR, FileAdjective.NEW)
        self.values['new_solr'] = comm(solr_end, solr_start, solr_new)
        if os.path.exists(solr_new):
            try:
                fileid = up.upload_file(infile=solr_new, folderID=fold_id)
                new_solr_file = url_string + fileid
                self.values['new_solr_file'] = new_solr_file
            except Exception as err:
                print("Err in compute: %s" % err)

        solr_deleted = Filename.get(self.end, FileType.SOLR, FileAdjective.DELETED)
        self.values['deleted_solr'] = comm(solr_start, solr_end, solr_deleted)
        if os.path.exists(solr_deleted):
            try:
                fileid = up.upload_file(infile=solr_deleted, folderID=fold_id)
                deleted_solr_file = url_string + fileid
                self.values['deleted_solr_file'] = deleted_solr_file
            except Exception as err:
                print("Err in compute: %s" % err)

        solr_extra = Filename.get(self.end, FileType.SOLR, FileAdjective.EXTRA)
        self.values['extra_solr'] = comm(solr_end, canonical_end, solr_extra)
        if os.path.exists(solr_extra):
            try:
                fileid = up.upload_file(infile=solr_extra, folderID=fold_id)
                extra_solr_file = url_string + fileid
                self.values['extra_solr_file'] = extra_solr_file
            except Exception as err:
                print("Err in compute: %s" % err)
        with open(solr_extra, 'r') as fe:
            zndo = 0
            for l in fe.readlines():
                if 'zndo' in l:
                    zndo += 1
        non_zndo = self.values['extra_solr'] - zndo
        self.values['extra_solr_zndo'] = str(zndo) + ' Zenodo, ' + str(non_zndo) + ' other'

        self.values['solr'] = lines_in_file(solr_end)
        delta = self.values['solr'] - lines_in_file(solr_start)
        if delta < 0:
            self.values['solr_delta'] = str(abs(delta)) + ' fewer.'
        else:
            self.values['solr_delta'] = str(abs(delta)) + ' more.'
        

    def fulltext(self):
        """Compute the new and deleted bibcodes for each type of error from
        todays list of bibcodes compared with yesterdays list. Results stored
        in variables that are then used in report.py."""
        for e in list(conf['FULLTEXT_ERRORS'].keys()):

            err_msg = "_" + ("_".join(e.split())).replace('-', '_')

            ft_start = Filename.get(self.start, FileType.FULLTEXT, adjective=None, msg=err_msg + "_")
            ft_end = Filename.get(self.end, FileType.FULLTEXT, adjective=None, msg=err_msg + "_")
            ft_new = Filename.get(self.end, FileType.FULLTEXT, adjective=FileAdjective.NEW, msg=err_msg + "_")
            self.values['new_ft' + err_msg] = comm(ft_end, ft_start, ft_new)

            ft_deleted = Filename.get(self.end, FileType.FULLTEXT, FileAdjective.DELETED, msg=err_msg + "_")
            self.values['deleted_ft' + err_msg] = comm(ft_start, ft_end, ft_deleted)

            self.values['ft' + err_msg] = lines_in_file(ft_end)
