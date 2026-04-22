import getopt
import sys

from helper.logger import Logger


def cmd_line_input_time_space_cluster(argv):

    help_string = '''This is the commandline interface for the Time/Space Clustering near realtime index update script\n
                     All input options are required as described below\n
                     \t-h: Print the doc string for the module\n
                     \t-c (--customer): Input the customer name string as listed in the production SQL database\n
                     \t-i (--index): Input the corresponding Elasticsearch index id string for EQSources, must match\n 
                     \t\t\t the appropriate index for the corresponding customer\n
                     \t-p (--indexpeak): Input the corresponding Elasticsearch index id string for EQPeaks, must match\n 
                     \t\t\tthe appropriate index for the corresponding customer\n
                     \t-l (--lookback): Input the number of seconds back in time to check for new data in the\n 
                     \t\t\tproduction database\n
                     '''

    customer_name_str = ''
    index_id_sources = ''
    index_id_peaks = ''
    lookback_window = 72 * 3600  # specify a default interval in seconds for the update window
    # ip_address = '20.80.30.92'  # specify a default ip address for the database connection

    try:
        opts, args = getopt.getopt(argv,
                                   "hc:i:p:l:",
                                   ["customer=", "index=", "indexpeak=", "lookback="])
    except getopt.GetoptError:
        print('Error: Invalid command line option(s) selected!')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(help_string)
            sys.exit()
        elif opt in ("-c", "--customer"):
            customer_name_str = arg
        elif opt in ("-i", "--index"):
            index_id_sources = arg
        elif opt in ("-p", "--indexpeaks"):
            index_id_peaks = arg
        elif opt in ("-l", "--lookback"):
            lookback_window = int(arg)
        # elif opt in ("-a", "--addr"):
        #     ip_address = arg

    print('Customer Name: %s' % customer_name_str)
    print('Elasticsearch Index Id EQSources: %s' % index_id_sources)
    print('Elasticsearch Index Id EQPEaks: %s' % index_id_peaks)
    print('Lookback Time Window: %d s' % lookback_window)
    # print('Connecting to ES Db Connection at: %s' % ip_address)

    return customer_name_str, index_id_sources, index_id_peaks, lookback_window


def cmd_line_input_calc_index_cdf(argv):

    help_string = '''This is the commandline interface for a module that calculates the Cumulative Distribution\n
                     Fucntion of a variable in an Elasticsearch index\n
                     All input options are required as described below\n
                     \t-h: Print the doc string for the module\n
                     \t-i (--index): Input the corresponding Elasticsearch index id string for EQSources, must match\n
                     \t\t\t the appropriate index for the corresponding customer\n
                     '''

    index_id_sources = ''
    # ip_address = '20.80.30.92'  # input the default elastic database connection address

    try:
        opts, args = getopt.getopt(argv, "hi:", ["index="])
    except getopt.GetoptError:
        print('Error: Invalid command line option(s) selected!')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(help_string)
            sys.exit()
        elif opt in ("-i", "--index"):
            index_id_sources = arg
        # elif opt in ("-a", "--addr"):
        #     ip_address = arg

    print('Elasticsearch Index Id EQSources: %s' % index_id_sources)
    # print('Connecting to ES Db Connection at: %s' % ip_address)

    # return index_id_sources, ip_address
    return index_id_sources


def cmd_line_input_calc_bdy_assoc(argv):

    help_string = '''This is the commandline interface for a module that calculates the association between points and 
                     a boundary. Documents in the target index are updated with the associated boundary id
                        "-i", "--bdyindex" boundary index
                        "-n", "--bdyid" boundary id field
                        "-s", "--bdyshp" boundary shape field
                        "-o", "--outindex" output index
                        "-p", "--outfield" output field
                        "-a", "--altbdyname" Alternative boundary field name
                        "-m", "--metafields" Boundary metadata field name csv string
                     '''

    user_opts = {'index_name_bdys': '',
                 'bdy_id_field': '',
                 'bdy_shape_field': '',
                 'index_name_assocs': '',
                 'assoc_geo_field': '',
                 'alt_bdy_field_name': '',
                 'bdy_metafields': '',
    }

    try:
        opts, args = getopt.getopt(argv,
                                   "hi:n:s:o:p:a:m:",
                                   ["bdyindex=", "bdyid=", "bdyshp=", "outindex=", "outfield=", "altbdyname=",
                                    "metafields=",])
    except getopt.GetoptError:
        print('Error: Invalid command line option(s) selected!')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(help_string)
            sys.exit()
        elif opt in ("-i", "--bdyindex"):
            user_opts['index_name_bdys'] = arg
        elif opt in ("-n", "--bdyid"):
            user_opts['bdy_id_field'] = arg
        elif opt in ("-s", "--bdyshp"):
            user_opts['bdy_shape_field'] = arg
        elif opt in ("-o", "--outindex"):
            user_opts['index_name_assocs'] = arg
        elif opt in ("-p", "--outfield"):
            user_opts['assoc_geo_field'] = arg
        elif opt in ("-a", "--altbdyname"):
            user_opts['alt_bdy_field_name'] = arg
        elif opt in ("-m", "--metafields"):
            user_opts['bdy_metafields'] = arg
        # elif opt in ("-d", "--addr"):
        #     user_opts['es_address'] = arg

    print('Boundary Index: %s' % user_opts['index_name_bdys'])
    print('Boundary unique ID field name: %s' % user_opts['bdy_id_field'])
    print('Boundary shape field name: %s' % user_opts['bdy_shape_field'])
    print('Index to update: %s' % user_opts['index_name_assocs'])
    print('Geofield (point coord) to use for association: %s' % user_opts['assoc_geo_field'])
    print('Alternative boundary id field name: %s' % user_opts['alt_bdy_field_name'])
    print('Adding boundary level metadata fields: %s' % user_opts['bdy_metafields'])
    # print('Connecting to ES Db at: %s' % user_opts['es_address'])

    return user_opts


def cmd_line_input_segments(argv):
    help_string = '''This is the commandline interface for the Segments data index update script\n
                         All input options are required as described below\n
                         \t-h: Print the doc string for the module\n
                         \t-c (--customer): Input the customer name string as listed in the production SQL database\n
                         \t-i (--index): Input the corresponding Elasticsearch index for segments, must match\n 
                         \t\t\t the appropriate index for the corresponding customer\n
                         \t-s (--indexshape): Input the corresponding Elasticsearch index id for customer boundaries,\n 
                         \t\t\tmust match the appropriate index for the corresponding customer\n
                         \t-d (--shapeid): Input the corresponding Elasticsearch field name for the boundary uuid\n 
                         \t-b (--shapebdy): Input the corresponding Elasticsearch index field name for the shape\n 
                         \t\t\tboundary object\n
                         \t-l (--lookback): Input the number of seconds back in time to check for new data in the\n 
                         \t\t\tproduction database\n
                         \t-p (--mainsindexname): Input the name of the ES index where the customer segmentized main\n 
                         \t\t\tlineshapes are stored. If blank number of passes calculation will be skipped\n'''

    customer_name = ''
    segment_index_name = ''
    # field names for customer boundaries
    shape_index_name = ''
    shape_id_field = ''
    shape_bdy_field = ''
    shape_main_len_field = ''
    shape_n_svc_field = ''
    lookback_window = 72 * 3600
    mains_index_name = ''

    try:
        opts, args = getopt.getopt(argv,
                                   "hc:i:s:d:b:m:n:l:p:",
                                   ["customer=", "index=", "indexshape=", "shapeid=", "shapebdy=",
                                    "mainlen=", "numsvc=", "lookback=", "mainsindexname="])
    except getopt.GetoptError:
        print('Error: Invalid command line option(s) selected!')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(help_string)
            sys.exit()
        elif opt in ("-c", "--customer"):
            customer_name = arg
        elif opt in ("-i", "--index"):
            segment_index_name = arg
        elif opt in ("-s", "--indexshape"):
            shape_index_name = arg
        elif opt in ("-d", "--shapeid"):
            shape_id_field = arg
        elif opt in ("-b", "--shapebdy"):
            shape_bdy_field = arg
        elif opt in ("-m", "--mainlen"):
            shape_main_len_field = arg
        elif opt in ("-n", "--numsvc"):
            shape_n_svc_field = arg
        elif opt in ("-l", "--lookback"):
            lookback_window = int(arg)
        elif opt in ("-p", "--mainsindexname"):
            mains_index_name = arg

    print('Customer Name: %s' % customer_name)
    print('Segment Index Name: %s' % segment_index_name)
    print('Customer Boundary Index Name: %s' % shape_index_name)
    print('Customer Boundary ID Field Name: %s' % shape_id_field)
    print('Customer Boundary Shape Field Name: %s' % shape_bdy_field)
    print('Main Length in Boundary Field: %s' % shape_main_len_field)
    print('Number of Services in Boundary Field: %s' % shape_n_svc_field)
    print('Lookback Time Window: %d s' % lookback_window)
    print('Using Main Pipeline Geometry in Index: %s' % mains_index_name)

    return customer_name, segment_index_name, shape_index_name, shape_id_field, shape_bdy_field, \
        shape_main_len_field, shape_n_svc_field, lookback_window, mains_index_name


def cmd_line_meas_2_s3(argv):
    help_string = '''This is the commandline interface for the Measurements data S3 export script\n
                         All input options are required as described below\n
                         \t-h Print the doc string for the module\n
                         \t-d (--dirpath) The local temp directory to store the intermediate data file for transfer\n
                         \t-c (--customer): Input the customer name string as listed in the production SQL database\n
                         \t-b (--bucket): The name of the S3 bucket\n
                         \t-p (--prefix): The name of the directory prefix on the S3 bucket\n 
                         \t-l (--lookback): Number of seconds to lookback in time to pull data from SQL/Dynamo\n 
                         '''

    export_file_path = ''
    customer = ''
    bucket_name = ''
    prefix_name = ''
    lookback_sec = 36.0 * 3600.0

    try:
        opts, args = getopt.getopt(argv,
                                   "hd:c:b:p:l:",
                                   ["dirpath=", "customer=", "bucket=", "prefix=", "lookback="])
    except getopt.GetoptError:
        print('Error: Invalid command line option(s) selected!')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(help_string)
            sys.exit()
        elif opt in ("-d", "--dirpath"):
            export_file_path = arg
        elif opt in ("-c", "--customer"):
            customer = arg
        elif opt in ("-b", "--bucket"):
            bucket_name = arg
        elif opt in ("-p", "--prefix"):
            prefix_name = arg
        elif opt in ("-l", "--lookback"):
            lookback_sec = float(arg)

    print('Local temp file path: %s' % export_file_path)
    print('Customer Name: %s' % customer)
    print('S3 Bucket Name: %s' % bucket_name)
    print('S3 Bucket Prefix Dir Name: %s' % prefix_name)
    print('Lookback Time Window: %d s' % lookback_sec)

    return export_file_path, customer, bucket_name, prefix_name, lookback_sec
