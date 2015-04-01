#! /usr/bin/env python
#

from time import localtime,strftime
from datetime import timedelta,date,datetime
import re
import inspect

outfile = ''

def out(msg):
    global outfile

    line = strftime("%Y-%m-%d %H:%M:%S", localtime()) + " - " + msg
    print line
    if not outfile:
        outfile = 'sync_' + strftime("%Y%m%d%H%M%S", localtime()) + '.log'
    f = open(outfile, 'a')
    line += "\n"
    try:
        line = line.encode("utf8")
    except:
        pass
    f.write(line)

def ipv4_to_int(ip):
    # If this is not a valid IPv4 address return 0, otherwise return int version of ip address
    # I realize this isn't an especially rigorous IP regex, it's all we need to create ids for IP users
    pattern = re.compile("^(\d{0,3})\.(\d{0,3})\.(\d{0,3})\.(\d{0,3})$")
    m = pattern.match(ip)
    if m == None:
        return 0
    # IP to int via: (first octet * 256^3) + (second octet * 256^2) + (third octet * 256) + (fourth octet)
    return (int(m.group(1)) * 256**3) + (int(m.group(2)) * 256**2) + (int(m.group(3)) * 256) + int(m.group(4))

def ww_to_date(ww):
    """ Given a wikiweek, will return a date string in the form YYYYmmdd """
    epoch = date(2001, 1, 1)
    ww = timedelta(weeks=ww)
    nd = epoch + ww
    return nd.strftime("%Y%m%d")

def date_to_ww(s):
    """ Given a date of the form YYYYmmdd, will return the wikiweek """
    epoch = date(2001, 1, 1)
    s = datetime.strptime(s, '%Y%m%d').date()
    diff = s - epoch
    return diff.days / 7

def get_ww():
    epoch = date(2001, 1, 1)
    today = date.today()
    diff = today - epoch
    return diff.days / 7

