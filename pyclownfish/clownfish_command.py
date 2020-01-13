# Copyright (c) 2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com

"""
Command library of clownfish
"""
from pyclownfish import clownfish_command_common
from pyclownfish import clownfish_subsystem_option
from pyclownfish import clownfish_subsystem_fs

# Key: subsystem name. Value: calss Subsystem
SUBSYSTEM_DICT = {}
SUBSYSTEM_NONE = clownfish_command_common.Subsystem("global")

CLOWNFISH_COMMNAD_FORMAT_ALL = "format_all"
CLOWNFISH_COMMNAD_HELP = "h"
CLOWNFISH_COMMNAD_MOUNT_ALL = "mount_all"
CLOWNFISH_COMMNAD_NONEXISTENT = "nonexistent"
CLOWNFISH_COMMNAD_PREPARE = "prepare"
CLOWNFISH_COMMNAD_QUIT = "q"
CLOWNFISH_COMMNAD_RETVAL = "retval"
CLOWNFISH_COMMNAD_UMOUNT_ALL = "umount_all"

CLOWNFISH_DELIMITER_AND = "AND"
CLOWNFISH_DELIMITER_OR = "OR"
CLOWNFISH_DELIMITER_CONT = "CONT"
MAX_FAST_COMMAND_TIME = 1


def clownfish_command_format_all(connection, args):
    """
    Format all the filesystems
    """
    log = connection.cc_command_log
    if ((clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP in args) or
            (clownfish_command_common.CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: format_all
Format *all* Lustre device(s)
  -f: force running the command without asking for confirmation""")
        return 0

    confirmed = False
    if "-f" in args or "--force" in args:
        confirmed = True

    if not confirmed:
        input_result = connection.cc_ask_for_input("Are you sure to format all Lustre devices? (y,N) ")
        if input_result is None:
            log.cl_error("failed to get input")
            return -1
        if input_result.startswith("y") or input_result.startswith("Y"):
            confirmed = True

    if not confirmed:
        log.cl_stdout("won't format any Lustre devices")
        return -1

    instance = connection.cc_instance
    return instance.ci_format_all(log)


SUBSYSTEM_NONE.ss_command_dict[CLOWNFISH_COMMNAD_FORMAT_ALL] = \
    clownfish_command_common.ClownfishCommand(CLOWNFISH_COMMNAD_FORMAT_ALL,
                                              clownfish_command_format_all,
                                              speed=clownfish_command_common.SPEED_ALWAYS_SLOW)


def clownfish_command_mount_all(connection, args):
    """
    Mount all the filesystems
    """
    log = connection.cc_command_log
    if ((clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP in args) or
            (clownfish_command_common.CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: mount_all
Mount *all* Lustre device(s)""")
        return 0

    instance = connection.cc_instance
    return instance.ci_mount_all(log)


SUBSYSTEM_NONE.ss_command_dict[CLOWNFISH_COMMNAD_MOUNT_ALL] = \
    clownfish_command_common.ClownfishCommand(CLOWNFISH_COMMNAD_MOUNT_ALL,
                                              clownfish_command_mount_all,
                                              speed=clownfish_command_common.SPEED_ALWAYS_SLOW)


def clownfish_command_umount_all(connection, args):
    """
    Umount all the filesystems
    """
    log = connection.cc_command_log
    if ((clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP in args) or
            (clownfish_command_common.CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: umount_all
Umount *all* Lustre device(s)""")
        return 0

    instance = connection.cc_instance
    return instance.ci_umount_all(log)


SUBSYSTEM_NONE.ss_command_dict[CLOWNFISH_COMMNAD_UMOUNT_ALL] = \
    clownfish_command_common.ClownfishCommand(CLOWNFISH_COMMNAD_UMOUNT_ALL,
                                              clownfish_command_umount_all,
                                              speed=clownfish_command_common.SPEED_ALWAYS_SLOW)


def clownfish_command_help(connection, args):
    # pylint: disable=unused-argument
    """
    Print the help string
    """
    log = connection.cc_command_log
    if len(args) <= 0:
        log.cl_stdout("""Command action:
  format_all           format all Lustre filesystems and MGTs
  h                    print this menu
  h <cmdline>          print help of command line
  prepare              prepare all hosts
  q                    quit
  fs mount             mount Lustre filesystem(s)
  fs umount            umount Lustre filesystem(s)
  mount_all            mount all Lustre filesystems and MGTs
  option disable       disable option(s)
  option enable        enable option(s)
  umount_all           umount all Lustre filesystems and MGTs""")
        return 0

    if (args[0] == CLOWNFISH_COMMNAD_HELP or
            args[0] == clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP or
            args[0] == clownfish_command_common.CLOWNFISH_OPTION_LONG_HELP):
        log.cl_stdout("""Usage %s [-a] [cmdline]
Print the help info of command line
  -a: Print the help infos of all commands""",
                      (CLOWNFISH_COMMNAD_HELP))
        return 0

    if args[0] == "-a":
        ret = SUBSYSTEM_NONE.ss_help_all(connection)
        if ret:
            return ret

        for subsystem in SUBSYSTEM_DICT.values():
            ret = subsystem.ss_help_all(connection)
            if ret:
                return ret
        return 0

    cmdline = " ".join(args)

    ccommand = args2command(args)
    if ccommand is not None:
        ret = ccommand.cc_function(connection,
                                   [clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP])
        if ret:
            log.cl_stderr("failed to print help message for command line [%s]",
                          cmdline)
            return -1
    elif len(args) == 1:
        subsystem = name2subsystem(args[0])
        if subsystem is None:
            log.cl_stderr("no subsystem or global command with name [%s]", args[0])
            return -1
        return subsystem.ss_help_all(connection)
    else:
        log.cl_stderr("""command line [%s] is not valid""",
                      (cmdline))
        return -1

    return 0


def clownfish_command_help_argument(connection, complete_status):
    """
    Return candidates for argument of help
    """
    # pylint: disable=unused-argument
    if complete_status.ccs_is_help:
        # Already in help, so return no candidate
        return []
    line = complete_status.ccs_line
    start_index = line.find(CLOWNFISH_COMMNAD_HELP)
    if start_index == -1:
        # Unexpected bug, line should starts with CLOWNFISH_COMMNAD_HELP
        return []

    if start_index + len(CLOWNFISH_COMMNAD_HELP) > len(line):
        # Unexpected bug, line should starts with CLOWNFISH_COMMNAD_HELP
        return []

    new_start = start_index + len(CLOWNFISH_COMMNAD_HELP)
    new_line = line[new_start:]
    begidx = complete_status.ccs_begidx - new_start
    endidx = complete_status.ccs_endidx - new_start
    return clownfish_interact_candidates(connection, new_line, begidx, endidx,
                                         True)


COMMAND = clownfish_command_common.ClownfishCommand(CLOWNFISH_COMMNAD_HELP, clownfish_command_help)
COMMAND.cc_add_argument(clownfish_command_help_argument)
SUBSYSTEM_NONE.ss_command_dict[CLOWNFISH_COMMNAD_HELP] = COMMAND


def clownfish_command_retval(connection, args):
    # pylint: disable=unused-argument
    """
    Retun the last exit status
    """
    log = connection.cc_command_log

    if ((clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP in args) or
            (clownfish_command_common.CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: %s
Print the exit value of last command""" %
                      (CLOWNFISH_COMMNAD_RETVAL))
        return 0
    log.cl_stdout("%s", connection.cc_last_retval)
    return 0


SUBSYSTEM_NONE.ss_command_dict[CLOWNFISH_COMMNAD_RETVAL] = \
    clownfish_command_common.ClownfishCommand(CLOWNFISH_COMMNAD_RETVAL, clownfish_command_retval)


def clownfish_command_quit(connection, args):
    # pylint: disable=unused-argument
    """
    Quit this connection
    """
    log = connection.cc_command_log
    if ((clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP in args) or
            (clownfish_command_common.CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: %s
Disconnect from server and quit""" %
                      (CLOWNFISH_COMMNAD_QUIT))
        return 0

    connection.cc_quit = True
    log.cl_stdout("disconnected from server")
    return 0


SUBSYSTEM_NONE.ss_command_dict[CLOWNFISH_COMMNAD_QUIT] = \
    clownfish_command_common.ClownfishCommand(CLOWNFISH_COMMNAD_QUIT, clownfish_command_quit)


def clownfish_command_prepare(connection, args):
    """
    Prepare the hosts
    """
    # pylint: disable=redefined-variable-type,unused-argument
    log = connection.cc_command_log
    if ((clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP in args) or
            (clownfish_command_common.CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: %s <host>...
Prepare Lustre host(s), including installing Lustre RPMs etc.""" %
                      (CLOWNFISH_COMMNAD_PREPARE))
        return 0

    return connection.cc_instance.ci_prepare_all(log, connection.cc_workspace)


SUBSYSTEM_NONE.ss_command_dict[CLOWNFISH_COMMNAD_PREPARE] = \
    clownfish_command_common.ClownfishCommand(CLOWNFISH_COMMNAD_PREPARE,
                                              clownfish_command_prepare,
                                              speed=clownfish_command_common.SPEED_ALWAYS_SLOW)


def clownfish_command_names():
    """
    Return the command names
    """
    return SUBSYSTEM_NONE.ss_command_dict.keys()


def clownfish_subsystem_names():
    """
    Return the subsystem names
    """
    subsystem_names = []
    for subsystem in SUBSYSTEM_DICT.values():
        subsystem_names.append(subsystem.ss_name)
    return subsystem_names


class CommandCompleteStatus(object):
    """
    The status of complete command line
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, line, begidx, endidx, options, being_completed,
                 is_help):
        # pylint: disable=too-many-arguments
        self.ccs_options = options
        self.ccs_line = line
        # Whether the status under help, in order to prevent infinit loop of
        # autocompleting help, i.e. h h h h h ...
        self.ccs_is_help = is_help
        self.ccs_begidx = begidx
        self.ccs_endidx = endidx
        self.ccs_being_completed = being_completed


def clownfish_interact_candidates(connection, line, begidx, endidx, is_help):
    """
    Return the candidates of interact
    """
    # pylint: disable=too-many-branches,too-many-locals
    root_candidates = clownfish_command_names() + clownfish_subsystem_names()
    assert begidx <= endidx
    assert len(line) >= endidx

    if len(line) == 0:
        # No input, send the root candidates
        return root_candidates
    if endidx == 0:
        # No input before cursor, send the root candidates
        return root_candidates

    being_completed = line[begidx:endidx]
    if begidx == 0:
        # No space before cursor, it is completing first word
        candidates = root_candidates
    else:
        words_before_cursor = line[:begidx].split()
        if len(words_before_cursor) == 0:
            # No actual word before cursor, it is completing first word
            candidates = root_candidates
        else:
            subsystem_name = words_before_cursor[0]
            if subsystem_name in SUBSYSTEM_DICT:
                subsystem = SUBSYSTEM_DICT[subsystem_name]
                if len(words_before_cursor) == 1:
                    # No command after subsystem, return all possible commands
                    candidates = subsystem.ss_command_dict.keys()
                else:
                    command_name = words_before_cursor[1]
                    options = words_before_cursor[2:]
                    if command_name not in subsystem.ss_command_dict:
                        return []
                    command = subsystem.ss_command_dict[command_name]
                    complete_status = CommandCompleteStatus(line, begidx,
                                                            endidx, options,
                                                            being_completed,
                                                            is_help)
                    candidates = command.cc_candidates(connection,
                                                       complete_status)
            else:
                command_name = subsystem_name
                subsystem = SUBSYSTEM_NONE
                options = words_before_cursor[1:]
                if command_name not in subsystem.ss_command_dict:
                    return []
                command = subsystem.ss_command_dict[command_name]
                complete_status = CommandCompleteStatus(line, begidx, endidx,
                                                        options,
                                                        being_completed,
                                                        is_help)
                candidates = command.cc_candidates(connection, complete_status)

    if len(being_completed) == 0:
        # Nothing to complete, return all of the possible candidates
        return candidates

    final_candidates = []
    for candidate in candidates:
        if not candidate.startswith(being_completed):
            continue
        final_candidates.append(candidate)

    return final_candidates


def name2subsystem(subsystem_name):
    """
    Find the subsystem from the name
    """
    if subsystem_name in SUBSYSTEM_DICT:
        return SUBSYSTEM_DICT[subsystem_name]
    else:
        return None


def args2command(args):
    """
    Find the command from arguments
    """
    if len(args) == 0:
        return None
    subsystem_name = args[0]
    if subsystem_name in SUBSYSTEM_DICT:
        subsystem = SUBSYSTEM_DICT[subsystem_name]
        if len(args) == 1:
            return None
        command_name = args[1]
    else:
        command_name = subsystem_name
        subsystem = SUBSYSTEM_NONE
    if command_name not in subsystem.ss_command_dict:
        return None
    return subsystem.ss_command_dict[command_name]


SUBSYSTEM_DICT[clownfish_subsystem_option.SUBSYSTEM_OPTION_NAME] = \
    clownfish_subsystem_option.SUBSYSTEM_OPTION
SUBSYSTEM_DICT[clownfish_subsystem_fs.SUBSYSTEM_FS_NAME] = \
    clownfish_subsystem_fs.SUBSYSTEM_FS
