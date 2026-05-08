def read_cred_file(filename):
    with open(filename, 'r') as fh:
        creds = fh.readline()
    creds = creds.split(',')
    creds = [x.strip().strip('\n') for x in creds]

    return creds
