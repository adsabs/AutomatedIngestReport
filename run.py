
import argparse

from air.gather import Gather
from air.compute import Compute
from air.report import Report

import os


def main():
    parser = argparse.ArgumentParser(description='Process user input.')
    parser.add_argument('-k', '--kibana', default=False, dest='kibana', action='store_true',
                        help='request log data from kibana')
    parser.add_argument('-g', '--gather', default=False, dest='gather', action='store_true',
                        help='gather solr and canonical data files')
    parser.add_argument('-c', '--compute', default=False, dest='compute', action='store_true',
                        help='compute based on gathered data files')
    args = parser.parse_args()

    
    g = c = None
    # get Kibana output
    if args.kibana:
        k = Report(g,c)
        myads_query = query='"+@log_group:\\"backoffice-logs\\" +@log_stream:\\"fluent-bit-backoffice_prod_myads_pipeline_1\\" +@message:\\"Email sent to\\""'
        result = k.query_Kibana(query=myads_query, n_days=0, rows=5)
        try:
            count = result['responses'][0]['hits']['total']
        except:
            pass
        else:
            mesg = '\n\nNumber of myADS emails: %s\n\n' % (count)
            print(mesg)

    if args.gather:
        g = Gather()
        g.all()
        print('gathered list of bibcodes in canonical and bibcodes in solr')

    if args.compute:
        c = Compute()
        c.canonical()
        c.solr()
        try:
            c.fulltext()
            print("computed canonical and bibcodes")
        except Exception as err:
            print("RUN error on c.fulltext():",err)

    r = Report(g, c)
    try:
        print(r._text())
    except Exception as err:
        print('No db actions requested.')

if __name__ == '__main__':
    main()
