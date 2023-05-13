from __future__ import print_function
import argparse
from air.gather import Gather
from air.compute import Compute
from air.report import Report
from air.utils import SlackPublisher
from adsgcon.gmanager import GoogleManager
import datetime
from adsputils import load_config

conf = load_config(proj_home="./")

def getargs():

    parser = argparse.ArgumentParser(description="Process user input.")

    parser.add_argument("-g",
                        "--gather",
                        default=False,
                        dest="gather",
                        action="store_true",
                        help="gather solr and canonical data files")

    parser.add_argument("-c",
                        "--compute",
                        default=False,
                        dest="compute",
                        action="store_true",
                        help="compute based on gathered data files")

    return parser.parse_args()


def main():

    args = getargs()

    g = None
    c = None

    output_basedir = conf.get("AIR_DATA_DIRECTORY", "./data")
    now = datetime.datetime.now()
    output_file = output_basedir + "/" + now.strftime("%Y%m%d") + "System"

    with open(output_file,"w") as fout:

        if args.gather:
            g = Gather()
            try:
                g.all()
            except Exception as err:
                fout.write("Error in Gather.all(): %s\n" % err)

        if args.compute:
            c = Compute()
            try:
                c.canonical()
            except Exception as err:
                fout.write("Error in Compute.canonical(): %s\n" % err)
            try:
                c.solr()
            except Exception as err:
                fout.write("Error in Compute.solr(): %s\n" % err)
            try:
                c.fulltext()
            except Exception as err:
                fout.write("Error in Compute.fulltext(): %s\n" % err)

        if g and c:
            try:
                r = Report(g, c)
                fout.write(r._text())
            except Exception as err:
                fout.write("Exception in writing report: %s\n" % err)
        else:
            print("No report generated, skipping write.")

    try:
        folderId = conf.get("GOOGLE_SYSTEM_FOLDER", None)
        secretsPath = conf.get("GOOGLE_SECRETS_FILENAME", None)
        scopesList = [conf.get("GOOGLE_API_SCOPE", None)]
        up = GoogleManager(authtype="service",
                           folderId=folderId,
                           secretsFile=secretsPath,
                           scopes=scopesList)
        kwargs = {"infile": output_file,
                  "folderID": folderId,
                  "mtype": "text/html",
                  "meta_mtype": "application/vnd.google-apps.document"}
        out_id = up.upload_file(**kwargs)
        slack = SlackPublisher(out_id)
        slack.push()
    except Exception as err:
        print("Exception uploading report: %s\n" % err)

if __name__ == "__main__":
    main()
