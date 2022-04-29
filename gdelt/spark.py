#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author:
# Damian Sastre
# Email: damian.sastre@gmail.com

import datetime
import re
import time
import warnings
from io import BytesIO

import pandas as pd
import requests


# class NoDaemonProcess(multiprocessing.Process):
#     # make 'daemon' attribute always return False
#     def _get_daemon(self):
#         return False
#
#     def _set_daemon(self, value):
#         pass
#
#     daemon = property(_get_daemon, _set_daemon)
#
#
# # We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# # because the latter is only a wrapper function, not a proper class.
# class NoDaemonProcessPool(multiprocessing.pool.Pool):
#     Process = NoDaemonProcess


def get_parallel_data_frames(urls, table='gkg'):
    
    dataframes = urls.map(lambda url: _spark_worker(url, table=table))
    return dataframes
    
    
def _spark_worker(url, table=None, proxies=None):
    """Code to download the urls and blow away the buffer to keep memory usage
     down"""

    warnings.filterwarnings("ignore",
                            '.*have mixed types. Specify dtype.*')  # ignore
    time.sleep(0.001)


    r = requests.get(url, proxies=proxies, timeout=5)

    if r.status_code == 404:
        message = "GDELT does not have a url for date time " \
                  "{0}".format(re.search('[0-9]{4,18}', url).group())
        warnings.warn(message)
        return None
    
    try:
        buffer = BytesIO(r.content)
        if table == 'events':

            frame = pd.read_csv(buffer, compression='zip', sep='\t',
                                header=None, warn_bad_lines=False,
                                dtype={26: 'str', 27: 'str', 28: 'str'})  # ,
            # parse_dates=[1, 2])

        elif table == 'gkg':
            frame = pd.read_csv(buffer, compression='zip', sep='\t',
                                header=None, warn_bad_lines=False)
            # parse_dates=['DATE'], warn_bad_lines=False)

        else:  # pragma: no cover
            frame = pd.read_csv(buffer, compression='zip', sep='\t',
                                header=None, warn_bad_lines=False)

        buffer.flush()
        buffer.close()
        return frame

    except:
        try:
            message = "GDELT did not return data for date time " \
                      "{0}".format(re.search('[0-9]{4,18}', url).group())
            warnings.warn(message)
        except:  # pragma: no cover
            message = "No data returned for {0}".format(r.url)
            warnings.warn(message)
        return None
    