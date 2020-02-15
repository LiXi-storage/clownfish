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
from pylcommon import cstr
from pyclownfish import clownfish
from pyclownfish import clownfish_console

KEY_HOSTNAME = "hostname"
KEY_SERVICE = "service"

CLOWNFISH_LOCATION_KEYS = [KEY_HOSTNAME, KEY_SERVICE]

CLOWNFISH_LOCAL_LOG_DIR = "/var/log/clownfish/clownfish_local"


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
    utils.eprint("""Usage: %s [--config|-c <config>] [--logdir|-d <logdir>] [--help|-h] <command> [arg...]
  config: configuration file path
  logdir: the dir path to save logs
  config: config file path
  command: one of the following commands:
    locate <service> [hostname]
    start <service>
    stop <service>
  hostname: the hostname the service runs
  service: the name of the service, e.g. fsname-OST000a, or MGS ID in clownfish.conf, e.g. lustre_mgs
Examples:
  %s locate lustre_mgs hostname=server1
  %s locate lustre0-OST000a hostname=server2
  %s locate lustre0-MDT000a hostname=server3
  %s locate lustre0-OST000a
  %s start lustre0-OST000a
  %s start lustre_mgs
  %s stop lustre0-OST000a
  %s stop lustre_mgs""" % (command, command, command, command, command,
                           command, command, command, command))


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

    if len(args) == 0:
        usage(sys.argv[0])
        sys.exit(1)

    return workspace, config_fpath, args


def _clownfish_local_main_locate(log, workspace, config, config_fpath, args):
    """
    Print the location of service
    """
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    if len(args) == 0:
        log.cl_error("service name is not specified")
        usage(sys.argv[0])
        return -1
    elif len(args) == 1:
        service_name = args[0]
        hostname = socket.gethostname()
    elif len(args) == 2:
        service_name = args[0]
        hostname = args[1]
    else:
        log.cl_error("too many arguments")
        usage(sys.argv[0])
        return -1

    clownfish_instance = clownfish.init_instance(log, workspace, config,
                                                 config_fpath,
                                                 no_operation=True)
    if clownfish_instance is None:
        log.cl_error("failed to init Clownfish")
        return -1

    if service_name in clownfish_instance.ci_mgs_dict:
        service = clownfish_instance.ci_mgs_dict[service_name]
    else:
        fields = service_name.split("-")
        if len(fields) != 2:
            log.cl_error("invalid value [%s] for key [%s]",
                         service_name, KEY_SERVICE)
            return -1

        fsname = fields[0]
        if fsname not in clownfish_instance.ci_lustres:
            log.cl_error("Lustre file system with name [%s] is not configured",
                         fsname)
            return -1
        lustrefs = clownfish_instance.ci_lustres[fsname]
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


def _clownfish_local_main_start(log, workspace, config, config_fpath, args):
    """
    Start the service
    """
    # pylint: disable=unused-argument,too-many-locals
    server_config = utils.config_value(config, cstr.CSTR_CLOWNFISH_SERVER)
    if server_config is None:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_CLOWNFISH_SERVER, config_fpath)
        return -1

    virtual_ip = utils.config_value(server_config, cstr.CSTR_VIRTUAL_IP)
    if not virtual_ip:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_VIRTUAL_IP, config_fpath)
        return -1

    port = utils.config_value(server_config, cstr.CSTR_PORT)
    if not port:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_PORT, config_fpath)
        return -1

    local_hostname = socket.gethostname()

    server_url = "tcp://%s:%s" % (virtual_ip, port)
    console_client = clownfish_console.ClownfishClient(log, workspace,
                                                       server_url)
    ret = console_client.cc_init()
    if ret:
        log.cl_error("failed to connect to Clownfish server [%s]", server_url)
        return -1

    result = log.cl_result
    for arg in args:
        command = ("service move %s %s" % (arg, local_hostname))
        console_client.cc_command(log, command)
        if result.cr_exit_status:
            log.cl_error("failed to run command [%s]", command)
            ret = result.cr_exit_status
            break
    console_client.cc_fini()
    return ret


def _clownfish_local_main(log, workspace, config, config_fpath,
                          arguments):
    """
    Main routine
    """
    command = arguments[0]
    if command == "locate":
        return _clownfish_local_main_locate(log, workspace, config,
                                            config_fpath, arguments[1:])
    elif command == "start":
        return _clownfish_local_main_start(log, workspace, config,
                                           config_fpath, arguments[1:])
    else:
        usage(sys.argv[0])
        sys.exit(1)


def main():
    """
    Start clownfish server
    """
    cmd_general.main(constants.CLOWNFISH_CONFIG, CLOWNFISH_LOCAL_LOG_DIR,
                     clownfish_local_main, usage_func=usage,
                     parse_func=parse_arguments,
                     console_level=logging.ERROR,
                     lock=False)
