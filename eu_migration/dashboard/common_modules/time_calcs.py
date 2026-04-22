import time


def time_string_regularizer(in_time_string):
    in_time_string = in_time_string.strip('Z')
    # split the date and time strings
    date_time_strs = in_time_string.split()
    # fix any month or day values that are only one character
    ymd = date_time_strs[0].split('-')
    for ct, st in enumerate(ymd):
        if len(st) == 1:
            ymd[ct] = '0' + st
    ymd = '-'.join(ymd)
    in_time_string = '%s %s' % (ymd, date_time_strs[1])

    # grab decimal portion of seconds
    dt_tm_split = str(in_time_string).split('.')
    if len(dt_tm_split) == 1:
        return in_time_string + '.000'
    # fix the decimals to always contain 3 sigfigs
    elif len(dt_tm_split) == 2:
        n_dec = len(dt_tm_split[1])
        if n_dec == 1:
            return '%s.%s' % (dt_tm_split[0], dt_tm_split[1] + '00')
        elif n_dec == 2:
            return '%s.%s' % (dt_tm_split[0], dt_tm_split[1] + '0')
        elif n_dec == 3:
            return in_time_string
        elif n_dec == 4:
            return '%s.%s' % (dt_tm_split[0], str(round(float(dt_tm_split[1]) / 10.0)).zfill(3))
        elif n_dec == 5:
            return '%s.%s' % (dt_tm_split[0], str(round(float(dt_tm_split[1]) / 100.0)).zfill(3))
        elif n_dec == 6:
            return '%s.%s' % (dt_tm_split[0], str(round(float(dt_tm_split[1]) / 1000.0)).zfill(3))
        elif n_dec == 7:
            return '%s.%s' % (dt_tm_split[0], str(round(float(dt_tm_split[1]) / 10000.0)).zfill(3))
        else:
            raise ValueError('Too many digits in decimal, case not handled! %s' % in_time_string)

    else:
        raise ValueError('Input contains more than one decimal point! Time String: %s' % in_time_string)


def epoch_2_gmtstring(epoch_time):
    time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(epoch_time)) + '.'

    # calculate hundreths of seconds
    hunds_str = str(epoch_time).split('.')[-1]

    # add the hundreths onto the time string
    time_str += hunds_str[0:3]

    # add the zulu gmt identifier
    time_str += 'Z'

    return time_string_regularizer(time_str)


def epoch_2_gmtstring_basic(epoch_time):
    time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(epoch_time))

    return time_str


def gmtstring_2_epoch(gmt_string, pattern='%Y-%m-%d %H:%M:%S'):

    return time.mktime(time.strptime(gmt_string, pattern))
