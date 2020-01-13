# Copyright (c) 2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com

"""
Subsystem of filesystem
"""
from pyclownfish import clownfish_command_common

CLOWNFISH_FS_COMMNAD_FORMAT = "format"
CLOWNFISH_FS_COMMNAD_LIST = "list"
CLOWNFISH_FS_COMMNAD_MOUNT = "mount"
CLOWNFISH_FS_COMMNAD_UMOUNT = "umount"
CLOWNFISH_FS_COMMANDS = [CLOWNFISH_FS_COMMNAD_LIST, CLOWNFISH_FS_COMMNAD_MOUNT,
                         CLOWNFISH_FS_COMMNAD_UMOUNT]
CLOWNFISH_FS_COMMNAD_HELP = "h"

SUBSYSTEM_FS_NAME = "fs"
SUBSYSTEM_FS = clownfish_command_common.Subsystem(SUBSYSTEM_FS_NAME)


def fs_argument(connection, complete_status):
    """
    Return file system names
    """
    # pylint: disable=unused-argument
    instance = connection.cc_instance
    return instance.ci_lustres.keys()


def clownfish_command_filesystem_usage(log, command):
    """
    Run command on the filesystems
    """
    log.cl_stdout("""Usage: %s %s <filesystem>...
Run %s on Lustre filesystem(s)""" %
                  (SUBSYSTEM_FS_NAME, command, command))


def clownfish_command_filesystem(connection, args, command):
    """
    Run command on the filesystems
    """
    log = connection.cc_command_log
    if command != CLOWNFISH_FS_COMMNAD_MOUNT and command != CLOWNFISH_FS_COMMNAD_UMOUNT:
        log.cl_error("unknown command [%s] to file system(s)", command)
        return -1

    if ((clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP in args) or
            (clownfish_command_common.CLOWNFISH_OPTION_LONG_HELP in args)):
        clownfish_command_filesystem_usage(log, command)
        return 0

    if len(args) <= 0:
        log.cl_error("please specify the file system(s) to %s", command)
        clownfish_command_filesystem_usage(log, command)
        return -1

    instance = connection.cc_instance
    ret2 = 0
    for arg in args:
        if arg not in instance.ci_lustres:
            log.cl_error("filesystem [%s] is not configured", arg)
            ret2 = -1
            continue
        lustrefs = instance.ci_lustres[arg]
        if command == CLOWNFISH_FS_COMMNAD_MOUNT:
            ret = lustrefs.lf_mount(log)
            if ret:
                log.cl_stderr("failed to mount file system [%s]",
                              lustrefs.lf_fsname)
        else:
            ret = lustrefs.lf_umount(log)
            if ret:
                log.cl_stderr("failed to umount file system [%s]",
                              lustrefs.lf_fsname)
        if ret < 0:
            ret2 = ret
    return ret2


def clownfish_command_umount(connection, args):
    """
    Umount the filesystems
    """
    return clownfish_command_filesystem(connection, args,
                                        CLOWNFISH_FS_COMMNAD_UMOUNT)

COMMAND = clownfish_command_common.ClownfishCommand(CLOWNFISH_FS_COMMNAD_UMOUNT, clownfish_command_umount)
COMMAND.cc_add_argument(fs_argument)
SUBSYSTEM_FS.ss_command_dict[CLOWNFISH_FS_COMMNAD_UMOUNT] = COMMAND


def clownfish_command_mount(connection, args):
    """
    Mount the filesystems
    """
    return clownfish_command_filesystem(connection, args, CLOWNFISH_FS_COMMNAD_MOUNT)

COMMAND = clownfish_command_common.ClownfishCommand(CLOWNFISH_FS_COMMNAD_MOUNT,
                                                    clownfish_command_mount)
COMMAND.cc_add_argument(fs_argument)
SUBSYSTEM_FS.ss_command_dict[CLOWNFISH_FS_COMMNAD_MOUNT] = COMMAND


def clownfish_command_help(connection, args):
    """
    Print the help information
    """
    # pylint: disable=unused-argument
    command_string = ""
    for command in CLOWNFISH_FS_COMMANDS:
        if command_string == "":
            command_string += command
        else:
            command_string += "|" + command
    log = connection.cc_command_log
    log.cl_stdout("""Usage: %s [%s] <servicename>...
    list: list information about the service
    mount: mount the Lustre file system
    umount: umount the Lustre file system
        servicename: a fsname or a Lustre service name, e.g. fsname-OST000a""" %
                  (SUBSYSTEM_FS_NAME,
                   command_string))
    return 0

COMMAND = clownfish_command_common.ClownfishCommand(CLOWNFISH_FS_COMMNAD_HELP,
                                                    clownfish_command_help)
SUBSYSTEM_FS.ss_command_dict[CLOWNFISH_FS_COMMNAD_HELP] = COMMAND


def fs_service_argument(connection, complete_status):
    """
    Return argument that can be enabled
    """
    instance = connection.cc_instance
    fsnames = instance.ci_lustres.keys()
    being_completed = complete_status.ccs_being_completed
    if len(being_completed) <= 1:
        # Impossible to have '$FSNAME-.+' pattern
        return fsnames

    fields = being_completed.split("-")
    if len(fields) != 2:
        # No '-' or multiple '-', not able to know fsname
        return fsnames

    fsname = fields[0]
    if fsname not in fsnames:
        return []

    lustrefs = instance.ci_lustres[fsname]

    services = []
    # Now return all candidates of '$FSNAME-$SERVICE'
    services += lustrefs.lf_service_dict.keys()
    services += lustrefs.lf_clients.keys()
    return services


def fs_service_usage(log, command):
    """
    Run command on the filesystems
    """
    log.cl_stdout("""Usage: %s %s <servicename>...
        servicename: a fsname or a Lustre service name, e.g. fsname-OST000a""" %
                  (SUBSYSTEM_FS_NAME, command))


def fs_service_list(connection, args):
    """
    list the service(s) of a filesystem
    """
    # pylint: disable=too-many-branches
    log = connection.cc_command_log

    if ((clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP in args) or
            (clownfish_command_common.CLOWNFISH_OPTION_LONG_HELP in args)):
        fs_service_usage(log, CLOWNFISH_FS_COMMNAD_LIST)
        return 0

    instance = connection.cc_instance

    if len(args) <= 0:
        instance.ci_list_lustre(log)
        return 0

    ret2 = 0
    for arg in args:
        fields = arg.split("-")
        if len(fields) == 1:
            # Only fsname
            fsname = fields[0]
            service_name = None
        elif len(fields) == 2:
            fsname = fields[0]
            service_name = fields[0] + "-" + fields[1]
        else:
            log.cl_stderr("invalid filesystem/service name [%s]",
                          arg)
            ret2 = -1
            continue

        if fsname not in instance.ci_lustres:
            log.cl_error("filesystem [%s] doesnot exist", fsname)
            ret2 = -1
            continue

        lustrefs = instance.ci_lustres[fsname]
        if service_name is None:
            ret = lustrefs.lf_list(log)
            if ret:
                log.cl_stderr("failed to list filesystem [%s]", fsname)
                ret2 = ret
        else:
            if service_name in lustrefs.lf_service_dict:
                service = lustrefs.lf_service_dict[service_name]
                ret = service.ls_list(log, instance)
                if ret:
                    log.cl_stderr("failed to list service [%s]", service_name)
                    ret2 = ret
            elif service_name in lustrefs.lf_clients:
                client = lustrefs.lf_clients[service_name]
                ret = client.lc_list(log)
                if ret:
                    log.cl_stderr("failed to list service [%s]", service_name)
                    ret2 = ret
            else:
                log.cl_stderr("service [%s] doesnot exist in filesystem [%s]",
                              service_name, fsname)
                ret2 = -1
    return ret2

COMMAND = clownfish_command_common.ClownfishCommand(CLOWNFISH_FS_COMMNAD_LIST, fs_service_list)
COMMAND.cc_add_argument(fs_service_argument)
SUBSYSTEM_FS.ss_command_dict[CLOWNFISH_FS_COMMNAD_LIST] = COMMAND
