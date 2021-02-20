from __future__ import print_function
import argparse
from air.gather import Gather
from air.compute import Compute
from air.report import Report


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

    if args.gather:
        g = Gather()
        try:
            g.all()
        except Exception as err:
            print('Error in Gather.all(): %s' % err)

    if args.compute:
        c = Compute()
        try:
            c.canonical()
        except Exception as err:
            print('Error in Compute.canonical(): %s' % err)
        try:
            c.solr()
        except Exception as err:
            print('Error in Compute.solr(): %s' % err)
        try:
            c.fulltext()
        except Exception as err:
            print('Error in Compute.fulltext(): %s' % err)

    try:
        r = Report(g, c)
        print(r._text())
    except Exception as err:
        print('Exception in writing report: %s' % err)
        # print('No db actions requested.')

if __name__ == '__main__':
    main()
