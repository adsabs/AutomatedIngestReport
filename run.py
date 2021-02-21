from __future__ import print_function
import argparse
from air.gather import Gather
from air.compute import Compute
from air.report import Report
import datetime




def main():
    parser = argparse.ArgumentParser(description='Process user input.')
    parser.add_argument('-g', '--gather', default=False, dest='gather',
                        action='store_true',
                        help='gather solr and canonical data files')
    parser.add_argument('-c', '--compute', default=False, dest='compute',
                        action='store_true',
                        help='compute based on gathered data files')
    args = parser.parse_args()

    g = c = None

    now = datetime.datetime.now()
    output_file = now.strftime('%Y%m%d') + 'System'

    with open(output_file,'w') as fout:

        if args.gather:
            g = Gather()
            try:
                g.all()
            except Exception as err:
                fout.write('Error in Gather.all(): %s\n' % err)

        if args.compute:
            c = Compute()
            try:
                c.canonical()
            except Exception as err:
                fout.write('Error in Compute.canonical(): %s\n' % err)
            try:
                c.solr()
            except Exception as err:
                fout.write('Error in Compute.solr(): %s\n' % err)
            try:
                c.fulltext()
            except Exception as err:
                fout.write('Error in Compute.fulltext(): %s\n' % err)

        try:
            r = Report(g, c)
            fout.write(r._text())
        except Exception as err:
            fout.write('Exception in writing report: %s\n' % err)

if __name__ == '__main__':
    main()
