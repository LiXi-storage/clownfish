# Copyright (c) 2020 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Utilities without connecting to server
Clownfish is an automatic management system for Lustre
"""
import traceback
import sys
import getopt
import logging
import socket
import yaml

# Local libs
from pylcommon import utils
from pylcommon import cmd_general
from pylcommon import constants
from pyclownfish import clownfish

KEY_MGS_ID = "mgs_id"
KEY_FSNAME = "fsname"
KEY_HOSTNAME = "hostname"
KEY_UUID = "service_uuid"

CLOWNFISH_LOCATION_KEYS = [KEY_MGS_ID, KEY_FSNAME, KEY_HOSTNAME,
                           KEY_UUID]

CLOWNFISH_LOCAL_LOG_DIR = "/var/log/clownfish/clownfish_local"


def _clownfish_local_main(log, workspace, config, config_fpath,
                          location_dict):
    """
    Routine
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    if KEY_HOSTNAME in location_dict:
        hostname = location_dict[KEY_HOSTNAME]
    else:
        hostname = socket.gethostname()

    if KEY_MGS_ID in location_dict:
        mgs_id = location_dict[KEY_MGS_ID]
    else:
        mgs_id = None

    if KEY_UUID in location_dict:
        service_uuid = location_dict[KEY_UUID]
    else:
        service_uuid = None

    if KEY_FSNAME in location_dict:
        fsname = location_dict[KEY_FSNAME]
    else:
        fsname = None

    if mgs_id is not None and service_uuid is not None:
        log.cl_error("key [%s] and [%s] cannot be used at the same time",
                     KEY_MGS_ID, KEY_UUID)
        return -1

    if mgs_id is not None and fsname is not None:
        log.cl_error("key [%s] and [%s] cannot be used at the same time",
                     KEY_MGS_ID, KEY_FSNAME)
        return -1

    if ((fsname is not None and service_uuid is None) or
            (fsname is None and service_uuid is not None)):
        log.cl_error("key [%s] and [%s] should be specified at the same time",
                     KEY_FSNAME, KEY_UUID)
        return -1

    if mgs_id is None and fsname is None:
        log.cl_error("either key [%s] or [%s] should be specified",
                     KEY_FSNAME, KEY_MGS_ID)
        return -1

    clownfish_instance = clownfish.init_instance(log, workspace, config,
                                                 config_fpath,
                                                 no_operation=True)
    if clownfish_instance is None:
        log.cl_error("failed to init Clownfish")
        return -1

    if mgs_id is not None:
        if mgs_id not in clownfish_instance.ci_mgs_dict:
            log.cl_error("mgs with id [%s] is not configured",
                         mgs_id)
            return -1
        service = clownfish_instance.ci_mgs_dict[mgs_id]
    else:
        if fsname not in clownfish_instance.ci_lustres:
            log.cl_error("Lustre file system with name [%s] is not configured",
                         fsname)
            return -1
        lustrefs = clownfish_instance.ci_lustres[fsname]

        fields = service_uuid.split("-")
        if len(fields) > 2 or len(fields) == 0:
            log.cl_error("invalid value [%s] for key [%s]",
                         service_uuid, KEY_UUID)
            return -1
        elif len(fields) == 1:
            service_name = fsname + "-" + fields[0]
        else:
            if fields[0] != fsname:
                log.cl_error("invalid fsname [%s] in the value [%s] for key [%s], expected [%s]",
                             fields[0], service_uuid, KEY_UUID, fsname)
                return -1
            service_name = service_uuid
        if service_name not in lustrefs.lf_service_dict:
            log.cl_error("service [%s] is not configured for Lustre [%s]",
                         service_name, fsname)
            return -1
        service = lustrefs.lf_service_dict[service_name]

    for instance in service.ls_instances.values():
        host = instance.lsi_host
        if host.sh_hostname == hostname:
            print instance.lsi_device, instance.lsi_mnt
            return 0
    log.cl_error("host [%s] doesnot provide the Lustre service [%s]",
                 hostname, service.ls_service_name)
    return -1


def clownfish_local_main(log, workspace, config_fpath, location_dict):
    """
    Start local holding the configure lock
    """
    # pylint: disable=bare-except
    config_fd = open(config_fpath)
    ret = 0
    try:
        config = yaml.load(config_fd)
    except:
        log.cl_error("not able to load [%s] as yaml file: %s", config_fpath,
                     traceback.format_exc())
        ret = -1
    config_fd.close()
    if ret:
        return -1

    try:
        ret = _clownfish_local_main(log, workspace, config, config_fpath,
                                    location_dict)
    except:
        ret = -1
        log.cl_error("exception: %s", traceback.format_exc())

    return ret


def usage(command):
    """
    Print usage string
    """
    utils.eprint("""Usage: %s [--config|-c <config>] [--logdir|-d <logdir>] [--help|-h] <key>=<value>
logdir: the dir path to save logs
config: config file path
key is one of the following:
    mgs_id: the ID of mgs in clownfish.conf
    fsname: the name of Lustre file system
    hostname: the hostname the service runs
    uuid: the uuid of the service, e.g. fsname-OST000a, OST000a, or MDT000a

Examples:
    %s mgs_id=lustre_mgs hostname=server0
    %s fsname=lustre0 uuid=MDT000a hostname=server1
    %s fsname=lustre0 uuid=OST000a hostname=server2
    %s uuid=lustre0-OST000a hostname=server2
    %s fsname=lustre0 uuid=lustre0-OST000a hostname=server2
    """ % (command, command, command, command, command, command))


def find_equal(target):
    """
    Return the index of the first = without excape, need to escape \\ and \=
    """
    equal_index = -1
    escaped = False
    for char in target:
        equal_index += 1
        if char == '\\':
            escaped = not escaped
        else:
            if char == '=':
                if not escaped:
                    return equal_index
            escaped = False
    return -1


def pair_parse(arg, location_dict):
    """
    Parse the key value pair

    arg is a pair of $name=$value. Both $name and $value is a string.
    $name should be one of CLOWNFISH_LOCATION_KEYS.
    $value can contain = as long as it is escaped by \=
    \ itself can be escaped too by \\.
    """
    equal_index = find_equal(arg)
    if equal_index == -1:
        logging.error("cannot find [=] in argument [%s] of -xattr", arg)
        return -1

    if equal_index == 0:
        logging.error("no name pattern before [=] in argument [%s] of -xattr", arg)
        return -1

    if equal_index == len(arg) - 1:
        logging.error("no value pattern after [=] in argument [%s] of -xattr", arg)
        return -1

    name = arg[0:equal_index]
    # Remove the escape \\ or \=
    name = name.replace("\\\\", "\\").replace("\\=", "=")
    if name not in CLOWNFISH_LOCATION_KEYS:
        logging.error("invalid key [%s], expected one of %s",
                      name, CLOWNFISH_LOCATION_KEYS)
        return -1

    value = arg[equal_index + 1:]
    # Remove the escape \\ or \=
    value = value.replace("\\\\", "\\").replace("\\=", "=")
    location_dict[name] = value
    return 0


def parse_arguments():
    """
    Parse the arguments
    """
    options, args = getopt.getopt(sys.argv[1:],
                                  "c:i:h",
                                  ["config=",
                                   "help",
                                   "logdir="])

    config_fpath = None
    workspace = None
    for opt, arg in options:
        if opt == '-c' or opt == "--config" or opt == "-config":
            config_fpath = arg
        elif opt == '-l' or opt == "--logdir" or opt == "-logdir":
            workspace = arg
        elif opt == '-h' or opt == "--help" or opt == "-help":
            usage(sys.argv[0])
            sys.exit(0)
        else:
            usage(sys.argv[0])
            sys.exit(1)

    location_dict = {}
    for arg in args:
        pair_parse(arg, location_dict)

    return workspace, config_fpath, location_dict


def main():
    """
    Start clownfish server
    """
    cmd_general.main(constants.CLOWNFISH_CONFIG, CLOWNFISH_LOCAL_LOG_DIR,
                     clownfish_local_main, usage_func=usage,
                     parse_func=parse_arguments,
                     console_level=logging.ERROR)
