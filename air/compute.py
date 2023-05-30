from __future__ import absolute_import

import os.path
from builtins import object
from .utils import Filename, FileType, FileAdjective, Date, comm, lines_in_file, conf
from adsgcon.gmanager import GoogleManager


class Compute(object):
    """compute missing bibcodes and other values"""

    def __init__(self, start=Date.YESTERDAY, end=Date.TODAY):
        self.start = start
        self.end = end
        self.values = {}

    def canonical(self):
        try:
            folderId = conf.get("GOOGLE_DATA_FOLDER", None)
            secretsPath = conf.get("GOOGLE_SECRETS_FILENAME", None)
            scopesList = [conf.get("GOOGLE_API_SCOPE", None)]
            up = GoogleManager(authtype="service",
                               folderId=folderId,
                               secretsFile=secretsPath,
                               scopes=scopesList)
            url_string = conf.get("GOOGLE_URL_BASE", None)
        except Exception as err:
            print('Instantiating GoogleManager failed: %s' % err)

        """compute new, deleted"""
        try:
            canonical_start = Filename.get(self.start, FileType.CANONICAL)
            canonical_end = Filename.get(self.end, FileType.CANONICAL)
            canonical_new = Filename.get(self.end, FileType.CANONICAL, FileAdjective.NEW)
            self.values['new_canonical'] = comm(canonical_end, canonical_start, canonical_new)
        except Exception as err:
            print("Err in compute: %s" % err)

        if os.path.exists(canonical_new):
            try:
                kwargs = {"infile": canonical_new,
                          "folderId": folderId}
                fileid = up.upload_file(**kwargs)
                new_canonical_file = url_string + fileid
                self.values['new_canonical_file'] = new_canonical_file
            except Exception as err:
                self.values['new_canonical_file'] = '(Not Uploaded)'
                print("Err in compute: %s" % err)

        try:
            canonical_deleted = Filename.get(self.end, FileType.CANONICAL, FileAdjective.DELETED)
            self.values['deleted_canonical'] = comm(canonical_start, canonical_end, canonical_deleted)
        except Exception as err:
            print("Err in compute: %s" % err)

        if os.path.exists(canonical_deleted):
            try:
                kwargs = {"infile": canonical_deleted,
                          "folderId": folderId}
                fileid = up.upload_file(**kwargs)
                deleted_canonical_file = url_string + fileid
                self.values['deleted_canonical_file'] = deleted_canonical_file
            except Exception as err:
                self.values['deleted_canonical_file'] = '(Not Uploaded)'
                print("Err in compute: %s" % err)

        try:
            self.values['canonical'] = lines_in_file(canonical_end)
            delta = self.values['canonical'] - lines_in_file(canonical_start)
            if delta < 0:
                self.values['canonical_delta'] = str(abs(delta)) + ' fewer.'
            else:
                self.values['canonical_delta'] = str(abs(delta)) + ' more.'
        except Exception as err:
            print("Err in compute: %s" % err)

    def solr(self):
        """compute missing, deleted, new, extra"""

        try:
            folderId = conf.get("GOOGLE_DATA_FOLDER", None)
            secretsPath = conf.get("GOOGLE_SECRETS_FILENAME", None)
            scopesList = [conf.get("GOOGLE_API_SCOPE", None)]
            url_string = conf.get("GOOGLE_URL_BASE", None)
            up = GoogleManager(authtype="service",
                               folderId=folderId,
                               secretsFile=secretsPath,
                               scopes=scopesList)
        except Exception as err:
            print('Instantiating GoogleManager failed: %s' % err)

        try:
            solr_end = Filename.get(self.end, FileType.SOLR)
            canonical_end = Filename.get(self.end, FileType.CANONICAL)
            solr_missing = Filename.get(self.end, FileType.SOLR, FileAdjective.MISSING)
            self.values['missing_solr'] = comm(canonical_end, solr_end, solr_missing)
        except Exception as err:
            print("Err in compute: %s" % err)
        if os.path.exists(solr_missing):
            try:
                kwargs = {"infile": solr_missing,
                          "folderId": folderId}
                fileid = up.upload_file(**kwargs)
                missing_solr_file = url_string + fileid
                self.values['missing_solr_file'] = missing_solr_file
            except Exception as err:
                self.values['missing_solr_file'] = '(Not Uploaded)'
                print("Err in compute: %s" % err)

        try:
            solr_start = Filename.get(self.start, FileType.SOLR)
            solr_new = Filename.get(self.end, FileType.SOLR, FileAdjective.NEW)
            self.values['new_solr'] = comm(solr_end, solr_start, solr_new)
        except Exception as err:
            print("Err in compute: %s" % err)

        if os.path.exists(solr_new):
            try:
                kwargs = {"infile": solr_new,
                          "folderId": folderId}
                fileid = up.upload_file(**kwargs)
                new_solr_file = url_string + fileid
                self.values['new_solr_file'] = new_solr_file
            except Exception as err:
                self.values['new_solr_file'] = '(Not Uploaded)'
                print("Err in compute: %s" % err)

        try:
            solr_deleted = Filename.get(self.end, FileType.SOLR, FileAdjective.DELETED)
            self.values['deleted_solr'] = comm(solr_start, solr_end, solr_deleted)
        except Exception as err:
            print("Err in compute: %s" % err)
        if os.path.exists(solr_deleted):
            try:
                kwargs = {"infile": solr_deleted,
                          "folderId": folderId}
                fileid = up.upload_file(**kwargs)
                deleted_solr_file = url_string + fileid
                self.values['deleted_solr_file'] = deleted_solr_file
            except Exception as err:
                self.values['deleted_solr_file'] = '(Not Uploaded)'
                print("Err in compute: %s" % err)

        try:
            solr_extra = Filename.get(self.end, FileType.SOLR, FileAdjective.EXTRA)
            self.values['extra_solr'] = comm(solr_end, canonical_end, solr_extra)
        except Exception as err:
            print("Err in compute: %s" % err)

        if os.path.exists(solr_extra):
            try:
                kwargs = {"infile": solr_extra,
                          "folderId": folderId}
                fileid = up.upload_file(**kwargs)
                extra_solr_file = url_string + fileid
                self.values['extra_solr_file'] = extra_solr_file
            except Exception as err:
                self.values['extra_solr_file'] = '(Not Uploaded)'
                print("Err in compute: %s" % err)
            try:
                with open(solr_extra, 'r') as fe:
                    zndo = 0
                    for l in fe.readlines():
                        if 'zndo' in l:
                            zndo += 1
                non_zndo = self.values['extra_solr'] - zndo
                self.values['extra_solr_zndo'] = str(zndo) + ' Zenodo, ' + str(non_zndo) + ' other'
            except Exception as err:
                print("Err in compute: %s" % err)

        try:
            self.values['solr'] = lines_in_file(solr_end)
            delta = self.values['solr'] - lines_in_file(solr_start)
            if delta < 0:
                self.values['solr_delta'] = str(abs(delta)) + ' fewer.'
            else:
                self.values['solr_delta'] = str(abs(delta)) + ' more.'
        except Exception as err:
            print("Err in compute: %s" % err)


    def fulltext(self):
        """Compute the new and deleted bibcodes for each type of error from
        todays list of bibcodes compared with yesterdays list. Results stored
        in variables that are then used in report.py."""
        for e in list(conf.get('FULLTEXT_ERRORS', {}).keys()):

            err_msg = "_" + ("_".join(e.split())).replace('-', '_')

            ft_start = Filename.get(self.start, FileType.FULLTEXT, adjective=None, msg=err_msg + "_")
            ft_end = Filename.get(self.end, FileType.FULLTEXT, adjective=None, msg=err_msg + "_")
            ft_new = Filename.get(self.end, FileType.FULLTEXT, adjective=FileAdjective.NEW, msg=err_msg + "_")
            self.values['new_ft' + err_msg] = comm(ft_end, ft_start, ft_new)

            ft_deleted = Filename.get(self.end, FileType.FULLTEXT, FileAdjective.DELETED, msg=err_msg + "_")
            self.values['deleted_ft' + err_msg] = comm(ft_start, ft_end, ft_deleted)

            self.values['ft' + err_msg] = lines_in_file(ft_end)
