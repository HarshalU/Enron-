#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 22:00:09 2020

@author: ufac001
"""

from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta
import operator

def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday,
                      tms.tm_hour, tms.tm_min, tms.tm_sec,
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))

def extract_email_network(rdd):
    merge = lambda r: (str(r[0])+','+str(r[1])+','+ str(r[2])).split(',')
    val = lambda s, r, t: ((s, r[i], t) for i in range(len(r)))
    regex = '[^\s]+@[\w.!#$%&*+-/=?^_`{|}~+-]*(enron.com)$'
    valid = lambda s: re.match(regex, s) != None
    q1=rdd.map(lambda x: Parser().parsestr(x))\
          .map(lambda x: (x.get('From'),[x.get('To'),x.get('Cc'),x.get('Bcc')],date_to_dt(x.get('Date'))))\
          .map(lambda x: (x[0], merge(x[1]), x[2]))\
          .flatMap(lambda x: (val(x[0], x[1], x[2])))\
          .map(lambda x: (x[0], x[1].strip(), x[2]))\
          .filter(lambda x: valid(x[0])).filter(lambda x: valid(x[1]))\
          .filter(lambda x: x[0] != x[1]).distinct()
    return q1

def convert_to_weighted_network(rdd, drange=None):
    if(drange):
        if (drange[0]>drange[1]):
            q2 = rdd.filter(lambda x:x[2] >= drange[1]).map(lambda x:((x[0],x[1]),1)).reduceByKey(operator.add)
        else:
            q2 = rdd.filter(lambda x:x[2] >= drange[0]).map(lambda x:((x[0],x[1]),1)).reduceByKey(operator.add)
    else:
        q2 = rdd.map(lambda x:((x[0],x[1]),1)).reduceByKey(operator.add)
    return q2

def get_out_degrees(rdd):
    rdd31a = rdd.map(lambda x: (x[0][0],x[1])).reduceByKey(operator.add)
    rdd31b = rdd.map(lambda x: (x[0][1],0)).reduceByKey(operator.add)
    rdd31  = rdd31a + rdd31b
    rdd31  = rdd31.reduceByKey(operator.add).map(lambda x: (x[1],x[0])).sortByKey(ascending=False)
    return rdd31


def get_in_degrees(rdd):
    rdd32a = rdd.map(lambda x: (x[0][1],x[1])).reduceByKey(operator.add)
    rdd32b = rdd.map(lambda x: (x[0][0],0)).reduceByKey(operator.add)
    rdd32  = rdd32a+rdd32b
    rdd32  = rdd32.reduceByKey(operator.add).map(lambda x: (x[1],x[0])).sortByKey(ascending=False)
    return rdd32


def get_out_degree_dist(rdd):
    rdd41a = rdd.map(lambda x: (x[0][0],x[1])).reduceByKey(operator.add)
    rdd41b = rdd.map(lambda x: (x[0][1],0)).reduceByKey(operator.add)
    rdd41  = rdd41a + rdd41b
    rdd41  = rdd41.reduceByKey(operator.add).map(lambda x: (x[1],x[0]))
    rdd41  = rdd41.map(lambda x: (x[0],1)).reduceByKey(operator.add).sortByKey()
    return rdd41


def get_in_degree_dist(rdd):
    rdd42a = rdd.map(lambda x: (x[0][1],x[1])).reduceByKey(operator.add)
    rdd42b = rdd.map(lambda x: (x[0][0],0)).reduceByKey(operator.add)
    rdd42  = rdd42a+rdd42b
    rdd42  = rdd42.reduceByKey(operator.add).map(lambda x: (x[1],x[0]))
    rdd42  = rdd42.map(lambda x: (x[0],1)).reduceByKey(operator.add).sortByKey()
    return rdd42