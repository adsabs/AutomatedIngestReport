from __future__ import print_function
import argparse
from air.gather import Gather
from air.compute import Compute
from air.report import Report


def main():
    parser = argparse.ArgumentParser(description='Process user input.')
    parser.add_argument('-k', '--kibana', default=False, dest='kibana',
                        action='store_true',
                        help='request log data from kibana')
    parser.add_argument('-g', '--gather', default=False, dest='gather',
                        action='store_true',
                        help='gather solr and canonical data files')
    parser.add_argument('-c', '--compute', default=False, dest='compute',
                        action='store_true',
                        help='compute based on gathered data files')
    args = parser.parse_args()

    g = c = None
    # get Kibana output
    if args.kibana:
        # query for the number of myADS emails sent
        k = Report(g, c)
        myads_query = '"+@log_group:\\"backoffice-logs\\" +@log_stream:\\"fluent-bit-backoffice_prod_myads_pipeline_1\\" +@message:\\"Email sent to\\""'
        result = k.query_Kibana(query=myads_query, n_days=0, rows=5)
        try:
            count = result['responses'][0]['hits']['total']
        except Exception as err:
            print('Kibana query for myADS emails failed: %s' % err)
        else:
            mesg = '\nNumber of myADS emails: %s\n' % (count)
            print(mesg)

        k = Report(g, c)
        mstr_rslv_query = '"+@log_group:\\"backoffice-logs\\" +@log_stream:\\"fluent-bit-backoffice_prod_master_pipeline_1\\" +@message:\\"error sending links\\""'
        result = k.query_Kibana(query=mstr_rslv_query, n_days=0, rows=5)
        try:
            count = result['responses'][0]['hits']['total']
        except Exception as err:
            print('Kibana query for Master/Resolver errors failed: %s' % err)
        else:
            mesg = 'Number of Master/Resolver errors: %s\n\n' % (count)
            print(mesg)

    if args.gather:
        g = Gather()
        try:
            g.all()
            print('gathered list of bibcodes in canonical and bibcodes in solr')
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
            print("computed canonical and bibcodes")
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
