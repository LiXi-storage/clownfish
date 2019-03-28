# Copyright (c) 2016 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Wathed IO is an file IO which will call callbacks when reading/writing from/to
the file

DO NOT import any library that needs extra python package,
since this might cause failure of commands that uses this
library to install python packages.
"""

import io
import os
import logging
import traceback


WATCHEDIO_LOG = "log"
WATCHEDIO_HOSTNAME = "hostname"


def watched_io_open(fname, func, args):
    """Open watched IO file.
    Codes copied from io.py
    """
    if not isinstance(fname, (basestring, int)):
        raise TypeError("invalid file: %r" % fname)
    mode = "w"
    raw = io.FileIO(fname, mode)
    buffering = io.DEFAULT_BUFFER_SIZE
    try:
        blksize = os.fstat(raw.fileno()).st_blksize
    except (os.error, AttributeError):
        pass
    else:
        if blksize > 1:
            buffering = blksize
    buffer_writer = io.BufferedWriter(raw, buffering)
    text = WatchedIO(buffer_writer, fname, func, args)
    return text


class WatchedIO(io.TextIOWrapper):
    """
    WatchedIO object
    The func will be called when writting to the file
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, buffered_io, fname, func, args):
        super(WatchedIO, self).__init__(buffered_io)
        self.wi_func = func
        self.wi_args = args
        self.wi_fname = fname

    def write(self, data):
        """
        Need unicode() otherwise will hit problem:
        TypeError: can't write str to text stream
        And also, even the encoding should be utf-8
        there will be some error, so need to ignore it.
        """
        #pylint: disable=bare-except
        data = unicode(data, encoding='utf-8', errors='ignore')
        try:
            super(WatchedIO, self).write(data)
        except:
            logging.error("failed to write the file [%s]: %s",
                          self.wi_fname, traceback.format_exc())
        self.wi_func(self.wi_args, data)


def log_watcher_debug(args, new_log):
    """
    Watch log dump to clog.cl_debug
    """
    if len(new_log) == 0:
        return
    log = args[WATCHEDIO_LOG]
    log.cl_debug("log from host [%s]: [%s]",
                 args[WATCHEDIO_HOSTNAME], new_log)


def log_watcher_info(args, new_log):
    """
    Watch log dump to clog.cl_info
    """
    if len(new_log) == 0:
        return
    log = args[WATCHEDIO_LOG]
    log.cl_info("log from host [%s]: [%s]",
                args[WATCHEDIO_HOSTNAME], new_log)


def log_watcher_error(args, new_log):
    """
    Watch log dump to clog.cl_error
    """
    if len(new_log) == 0:
        return
    log = args[WATCHEDIO_LOG]
    log.cl_error("log from host [%s]: [%s]",
                 args[WATCHEDIO_HOSTNAME], new_log)


def log_watcher_info_simplified(args, new_log):
    """
    Watch log dump to clog.cl_info
    """
    if len(new_log) == 0:
        return
    log = args[WATCHEDIO_LOG]
    if new_log.endswith("\n"):
        new_log = new_log[:-1]
    log.cl_info(new_log)


def log_watcher_error_simplified(args, new_log):
    """
    Watch log dump to clog.cl_error
    """
    if len(new_log) == 0:
        return
    log = args[WATCHEDIO_LOG]
    if new_log.endswith("\n"):
        new_log = new_log[:-1]
    log.cl_error(new_log)
