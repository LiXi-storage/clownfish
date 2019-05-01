# Copyright (c) 2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com

"""
Subsystem of filesystem
"""
from pyclownfish import clownfish_command_common

CLOWNFISH_COMMNAD_FORMAT = "format"
CLOWNFISH_COMMNAD_LIST = "ls"
CLOWNFISH_COMMNAD_MOUNT = "mount"
CLOWNFISH_COMMNAD_UMOUNT = "umount"

SUBSYSTEM_FS_NAME = "fs"
SUBSYSTEM_FS = clownfish_command_common.Subsystem(SUBSYSTEM_FS_NAME)
CLOWNFISH_COMMNAD_DISABLE = "disable"
CLOWNFISH_COMMNAD_ENABLE = "enable"


def clownfish_command_filesystem_argument(connection, complete_status):
    """
    Return argument that can be enabled
    """
    # pylint: disable=unused-argument
    instance = connection.cc_instance
    return instance.ci_lustres.keys()


def clownfish_command_filesystem_usage(log, command):
    """
    Run command on the filesystems
    """
    log.cl_stdout("""Usage: %s %s [-f] <filesystem>...
Run %s on Lustre filesystem(s)
  -f: force running the command without asking for confirmation""" %
                  (SUBSYSTEM_FS_NAME, command, command))


def clownfish_command_filesystem(connection, args, command):
    """
    Run command on the filesystems
    """
    log = connection.cc_command_log
    if command != CLOWNFISH_COMMNAD_MOUNT and command != CLOWNFISH_COMMNAD_UMOUNT:
        log.cl_error("unknown command [%s] to file system(s)", command)
        return -1

    log = connection.cc_command_log
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
            log.cl_error("file system(s) is not configured", arg)
        lustrefs = instance.ci_lustres[arg]
        if command == CLOWNFISH_COMMNAD_MOUNT:
            ret = lustrefs.lf_mount(log)
            if ret:
                log.cl_stderr("failed to mount file system [%s]",
                              lustrefs.lf_fsname)
        else:
            ret = lustrefs.lf_mount(log)
            if ret:
                log.cl_stderr("failed to mount file system [%s]",
                              lustrefs.lf_fsname)
        if ret < 0:
            ret2 = ret
    return ret2


def clownfish_command_umount(connection, args):
    """
    Umount the filesystems
    """
    return clownfish_command_filesystem(connection, args,
                                        CLOWNFISH_COMMNAD_UMOUNT)

COMMAND = clownfish_command_common.ClownfishCommand(CLOWNFISH_COMMNAD_UMOUNT, clownfish_command_umount)
COMMAND.cc_add_argument(clownfish_command_filesystem_argument)
SUBSYSTEM_FS.ss_command_dict[CLOWNFISH_COMMNAD_UMOUNT] = COMMAND


def clownfish_command_mount(connection, args):
    """
    Mount the filesystems
    """
    return clownfish_command_filesystem(connection, args, CLOWNFISH_COMMNAD_MOUNT)

COMMAND = clownfish_command_common.ClownfishCommand(CLOWNFISH_COMMNAD_MOUNT,
                                                    clownfish_command_mount)
COMMAND.cc_add_argument(clownfish_command_filesystem_argument)
SUBSYSTEM_FS.ss_command_dict[CLOWNFISH_COMMNAD_MOUNT] = COMMAND
