# Copyright (c) 2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com

"""
Subsystem of option
"""
from pyclownfish import clownfish_command_common
from pylcommon import cstr

SUBSYSTEM_OPTION_NAME = "option"
SUBSYSTEM_OPTION = clownfish_command_common.Subsystem(SUBSYSTEM_OPTION_NAME)
CLOWNFISH_COMMNAD_DISABLE = "disable"
CLOWNFISH_COMMNAD_ENABLE = "enable"


def clownfish_command_enable_or_disable(connection, args, enable):
    """
    Enable or disable the option
    """
    # pylint: disable=redefined-variable-type,unused-argument
    if enable:
        cmd_name = CLOWNFISH_COMMNAD_ENABLE
    else:
        cmd_name = CLOWNFISH_COMMNAD_DISABLE

    log = connection.cc_command_log
    if ((clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP in args) or
            (clownfish_command_common.CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: %s %s <config>...
%s configuration(s)""" %
                      (SUBSYSTEM_OPTION_NAME, cmd_name, cmd_name))
        return 0

    if len(args) == 0:
        log.cl_stderr("please specify the option name after [%s]",
                      cmd_name)
        return -1

    ret = 0
    for arg in args:
        if arg == cstr.CSTR_LAZY_PREPARE:
            connection.cc_instance.ci_lazy_prepare = enable
            log.cl_stdout("%sd %s", cmd_name, arg)
        elif arg == cstr.CSTR_HIGH_AVAILABILITY:
            if enable:
                connection.cc_instance.ci_high_availability_enable()
            else:
                connection.cc_instance.ci_high_availability_disable(log)
            log.cl_stdout("%sd %s", cmd_name, arg)
        else:
            log.cl_stderr("unknown option [%s] to %s", cmd_name)
            ret = -1
    return ret


def clownfish_command_enable(connection, args):
    """
    Enable the option
    """
    return clownfish_command_enable_or_disable(connection, args, True)


def clownfish_command_enable_or_disable_argument(connection, complete_status):
    """
    Return argument that can be enabled
    """
    # pylint: disable=unused-argument
    return [cstr.CSTR_LAZY_PREPARE, cstr.CSTR_HIGH_AVAILABILITY]


COMMAND = clownfish_command_common.ClownfishCommand(CLOWNFISH_COMMNAD_ENABLE, clownfish_command_enable)
COMMAND.cc_add_argument(clownfish_command_enable_or_disable_argument)
SUBSYSTEM_OPTION.ss_command_dict[CLOWNFISH_COMMNAD_ENABLE] = COMMAND


def clownfish_command_disable(connection, args):
    """
    Disable the option
    """
    return clownfish_command_enable_or_disable(connection, args, False)


COMMAND = clownfish_command_common.ClownfishCommand(CLOWNFISH_COMMNAD_DISABLE, clownfish_command_disable)
COMMAND.cc_add_argument(clownfish_command_enable_or_disable_argument)
SUBSYSTEM_OPTION.ss_command_dict[CLOWNFISH_COMMNAD_DISABLE] = COMMAND
