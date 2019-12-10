
import argparse

from air.gather import Gather
from air.compute import Compute
from air.report import Report


def main():
    parser = argparse.ArgumentParser(description='Process user input.')
    parser.add_argument('-g', '--gather', default=False, dest='gather', action='store_true',
                        help='gather solr and canonical data files')
    parser.add_argument('-c', '--compute', default=False, dest='compute', action='store_true',
                        help='compute based on gathered data files')
    args = parser.parse_args()
    g = c = None
    if args.gather:
        g = Gather()
        g.fulltext()
        print 'gathered list of bibcodes in canonical and bibcodes in solr'

    if args.compute:
        c = Compute()
        #c.canonical()
        #c.solr()
        c.fulltext()
        print 'computed canonical and bibcodes'

    r = Report(g, c)
    print r._text()

if __name__ == '__main__':
    main()
