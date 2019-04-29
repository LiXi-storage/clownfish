# Copyright (c) 2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com

"""
Command library of clownfish
"""
from pylcommon import cstr


class CommandOption(object):
    """
    An option of a C language command
    """
    # pylint: disable=too-few-public-methods,too-many-arguments
    def __init__(self, long_opt, has_arg, arg_funct=None, short_opt=None):
        self.co_short_opt = short_opt
        self.co_long_opt = long_opt
        self.co_has_arg = has_arg
        self.co_arg_funct = arg_funct


CLOWNFISH_COMMNAD_DISABLE = "disable"
CLOWNFISH_COMMNAD_ENABLE = "enable"
CLOWNFISH_COMMNAD_FORMAT = "format"
CLOWNFISH_COMMNAD_FORMAT_ALL = "format_all"
CLOWNFISH_COMMNAD_HELP = "h"
CLOWNFISH_COMMNAD_MANUAL = "m"
CLOWNFISH_COMMNAD_MOUNT = "mount"
CLOWNFISH_COMMNAD_MOUNT_ALL = "mount_all"
CLOWNFISH_COMMNAD_NONEXISTENT = "nonexistent"
CLOWNFISH_COMMNAD_PREPARE = "prepare"
CLOWNFISH_COMMNAD_QUIT = "q"
CLOWNFISH_COMMNAD_RETVAL = "retval"
CLOWNFISH_COMMNAD_UMOUNT = "umount"
CLOWNFISH_COMMNAD_UMOUNT_ALL = "umount_all"

CLOWNFISH_OPTION_SHORT_HELP = "-h"
CLOWNFISH_OPTION_LONG_HELP = "--help"

CLOWNFISH_DELIMITER_AND = "AND"
CLOWNFISH_DELIMITER_OR = "OR"
CLOWNFISH_DELIMITER_CONT = "CONT"

# The command that can never finish within MAX_FAST_COMMAND_TIME
SPEED_ALWAYS_SLOW = "always_slow"
# The command that can always finish within MAX_FAST_COMMAND_TIME
SPEED_ALWAYS_FAST = "always_fast"
SPEED_SLOW_OR_FAST = "slow_or_fast"
MAX_FAST_COMMAND_TIME = 1


class ClownfishCommand(object):
    """
    Config command
    """
    # pylint: disable=too-few-public-methods,too-many-arguments
    def __init__(self, command, function, speed=SPEED_ALWAYS_FAST):
        self.cc_command = command
        self.cc_function = function
        self.cc_speed = speed
        # Array of CommandOption
        self.cc_options = []
        # Array of functions that return candidates
        self.cc_arguments = []

    def cc_add_option(self, option):
        """
        Add a new option
        """
        self.cc_options.append(option)

    def cc_add_argument(self, argument):
        """
        Add a new argument
        """
        self.cc_arguments.append(argument)

    def _cc_all_candidates(self, connection, being_completed):
        """
        Return the candidates of arguments and also options
        """
        candidates = []
        for argument in self.cc_arguments:
            candidates += argument(connection, being_completed)

        for option in self.cc_options:
            if option.co_short_opt is not None:
                candidates.append(option.co_short_opt)
            candidates.append(option.co_long_opt)
        return candidates

    def cc_candidates(self, connection, words_before_cursor, being_completed):
        """
        Return the candidates of command
        """
        assert len(words_before_cursor) > 0
        assert words_before_cursor[0] == self.cc_command
        if len(words_before_cursor) == 1:
            # No option or argument yet, return all of the argument or option candidates
            return self._cc_all_candidates(connection, being_completed)

        last_word = words_before_cursor[-1]
        matched_option = None
        for option in self.cc_options:
            if option.co_short_opt == last_word or option.co_long_opt == last_word:
                matched_option = option
                break

        if matched_option is None:
            # Maybe a standalone argument
            return self._cc_all_candidates(connection, being_completed)
        elif not matched_option.co_has_arg:
            # Not expect any argument for this option
            return self._cc_all_candidates(connection, being_completed)
        elif matched_option.co_arg_funct is not None:
            return matched_option.co_arg_funct(connection, being_completed)
        else:
            # Option has argument, but do not know how to complete
            return []


# Server side commands
CLOWNFISH_COMMNADS = {}


def clownfish_command_help(connection, args):
    # pylint: disable=unused-argument
    """
    Print the help string
    """
    log = connection.cc_command_log
    if len(args) <= 1:
        log.cl_stdout("""Command action:
  disable              disable the current setting
  enable               enable the current setting
  format_all           format all of the filesystems
  h                    print this menu
  q                    quit
  mount                mount filesystem(s)
  mount_all            mount all of the filesystems
  umount               umount filesystem(s)
  umount_all            umount all of the filesystems""")
        return 0

    command_names = args[1:]
    if "-a" in args[1:]:
        command_names = clownfish_command_names()

    ret2 = 0
    need_new_line = False
    for command_name in command_names:
        if need_new_line:
            log.cl_stdout("")

        need_new_line = True
        if command_name not in CLOWNFISH_COMMNADS:
            log.cl_error("unknown command [%s]", command_name)
            ret2 = -1
            continue

        if command_name == CLOWNFISH_COMMNAD_HELP:
            log.cl_stdout("""Usage %s [-a] [command]...
Show the help info of command(s)
  -a: show help for all commands""",
                          (CLOWNFISH_COMMNAD_HELP))
            continue

        ccommand = CLOWNFISH_COMMNADS[command_name]
        ret = ccommand.cc_function(connection, [command_name, "-h"])
        if ret:
            log.cl_stderr("failed to print help message for command [%s]",
                          command_name)
            ret2 = ret
        need_new_line = True

    return ret2


def clownfish_command_help_argument(connection, being_completed):
    """
    Return candidates for argument of help
    """
    # pylint: disable=unused-argument
    return clownfish_command_names()


COMMAND = ClownfishCommand(CLOWNFISH_COMMNAD_HELP, clownfish_command_help)
COMMAND.cc_add_argument(clownfish_command_help_argument)
CLOWNFISH_COMMNADS[CLOWNFISH_COMMNAD_HELP] = COMMAND


def clownfish_command_retval(connection, args):
    # pylint: disable=unused-argument
    """
    Retun the last exit status
    """
    log = connection.cc_command_log

    if ((CLOWNFISH_OPTION_SHORT_HELP in args) or
            (CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: %s
Print the exit value of last command""" %
                      (CLOWNFISH_COMMNAD_RETVAL))
        return 0
    log.cl_stdout("%s", connection.cc_last_retval)
    return 0


CLOWNFISH_COMMNADS[CLOWNFISH_COMMNAD_RETVAL] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_RETVAL, clownfish_command_retval)


def clownfish_command_quit(connection, args):
    # pylint: disable=unused-argument
    """
    Quit this connection
    """
    log = connection.cc_command_log
    if ((CLOWNFISH_OPTION_SHORT_HELP in args) or
            (CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: %s
Disconnect from server and quit""" %
                      (CLOWNFISH_COMMNAD_QUIT))
        return 0

    connection.cc_quit = True
    log.cl_stdout("disconnected from server")
    return 0


CLOWNFISH_COMMNADS[CLOWNFISH_COMMNAD_QUIT] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_QUIT, clownfish_command_quit)


def clownfish_command_enable_or_disable(connection, args, enable):
    """
    Enable or disable the option
    """
    # pylint: disable=redefined-variable-type,unused-argument
    if enable:
        cmd_name = CLOWNFISH_COMMNAD_ENABLE
        operation = "Enable"
    else:
        cmd_name = CLOWNFISH_COMMNAD_DISABLE
        operation = "Disable"

    log = connection.cc_command_log
    if ((CLOWNFISH_OPTION_SHORT_HELP in args) or
            (CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: %s <config>...
%s configuration(s)""" %
                      (cmd_name, operation))
        return 0

    ret = 0
    for arg in args[1:]:
        if arg == cstr.CSTR_LAZY_PREPARE:
            connection.cc_instance.ci_lazy_prepare = enable
            log.cl_stdout("%sd %s", operation, arg)
        elif arg == cstr.CSTR_HIGH_AVAILABILITY:
            if enable:
                connection.cc_instance.ci_high_availability_enable()
            else:
                connection.cc_instance.ci_high_availability_disable(log)
            log.cl_stdout("%sd %s", operation, arg)
        else:
            log.cl_stderr("unknown option [%s] to %s", cmd_name)
            ret = -1
    return ret


def clownfish_command_enable(connection, args):
    """
    Enable the option
    """
    return clownfish_command_enable_or_disable(connection, args, True)


def clownfish_command_enable_or_disable_argument(connection, being_completed):
    """
    Return argument that can be enabled
    """
    # pylint: disable=unused-argument
    return [cstr.CSTR_LAZY_PREPARE, cstr.CSTR_HIGH_AVAILABILITY]


COMMAND = ClownfishCommand(CLOWNFISH_COMMNAD_ENABLE, clownfish_command_enable)
COMMAND.cc_add_argument(clownfish_command_enable_or_disable_argument)
CLOWNFISH_COMMNADS[CLOWNFISH_COMMNAD_ENABLE] = COMMAND


def clownfish_command_disable(connection, args):
    """
    Disable the option
    """
    return clownfish_command_enable_or_disable(connection, args, False)


COMMAND = ClownfishCommand(CLOWNFISH_COMMNAD_DISABLE, clownfish_command_disable)
COMMAND.cc_add_argument(clownfish_command_enable_or_disable_argument)
CLOWNFISH_COMMNADS[CLOWNFISH_COMMNAD_DISABLE] = COMMAND


def clownfish_command_filesystem_all(connection, args, command):
    """
    Run command on all the filesystems
    """
    # pylint: disable=redefined-variable-type,unused-argument
    log = connection.cc_command_log
    if ((CLOWNFISH_OPTION_SHORT_HELP in args) or
            (CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: %s_all
Run %s on *all* Lustre filesystem(s)""" %
                      (command, command))
        return 0

    instance = connection.cc_instance
    if command == CLOWNFISH_COMMNAD_FORMAT:
        return instance.ci_format_all(log)
    elif command == CLOWNFISH_COMMNAD_MOUNT:
        return instance.ci_mount_all(log)
    elif command == CLOWNFISH_COMMNAD_UMOUNT:
        return instance.ci_umount_all(log)
    else:
        log.cl_error("unknown command [%s] on all file system(s)", command)
        return -1


def clownfish_command_format_all(connection, args):
    """
    Format all the filesystems
    """
    log = connection.cc_command_log
    if ((CLOWNFISH_OPTION_SHORT_HELP in args) or
            (CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: %s_all
Format *all* Lustre device(s)
  -f: force running the command without asking for confirmation""" %
                      (args[0]))
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


CLOWNFISH_COMMNADS[CLOWNFISH_COMMNAD_FORMAT_ALL] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_FORMAT_ALL, clownfish_command_format_all,
                     speed=SPEED_ALWAYS_SLOW)


def clownfish_command_mount_all(connection, args):
    """
    Mount all the filesystems
    """
    return clownfish_command_filesystem_all(connection, args,
                                            CLOWNFISH_COMMNAD_MOUNT)


CLOWNFISH_COMMNADS[CLOWNFISH_COMMNAD_MOUNT_ALL] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_MOUNT_ALL, clownfish_command_mount_all,
                     speed=SPEED_ALWAYS_SLOW)


def clownfish_command_umount_all(connection, args):
    """
    Umount all the filesystems
    """
    return clownfish_command_filesystem_all(connection, args,
                                            CLOWNFISH_COMMNAD_UMOUNT)


CLOWNFISH_COMMNADS[CLOWNFISH_COMMNAD_UMOUNT_ALL] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_UMOUNT_ALL, clownfish_command_umount_all,
                     speed=SPEED_ALWAYS_SLOW)


def clownfish_command_filesystem_argument(connection, being_completed):
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
    log.cl_stdout("""Usage: %s [-f] <filesystem>...
Run %s on Lustre filesystem(s)
  -f: force running the command without asking for confirmation""" %
                  (command, command))


def clownfish_command_filesystem(connection, args, command):
    """
    Run command on the filesystems
    """
    log = connection.cc_command_log
    if command != CLOWNFISH_COMMNAD_MOUNT and command != CLOWNFISH_COMMNAD_UMOUNT:
        log.cl_error("unknown command [%s] to file system(s)", command)
        return -1

    log = connection.cc_command_log
    if ((CLOWNFISH_OPTION_SHORT_HELP in args) or
            (CLOWNFISH_OPTION_LONG_HELP in args)):
        clownfish_command_filesystem_usage(log, command)
        return 0

    if len(args) <= 1:
        log.cl_error("please specify the file system(s) to %s", command)
        clownfish_command_filesystem_usage(log, command)
        return -1

    instance = connection.cc_instance
    ret2 = 0
    for arg in args[1:]:
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
    return clownfish_command_filesystem(connection, args, CLOWNFISH_COMMNAD_UMOUNT)

COMMAND = ClownfishCommand(CLOWNFISH_COMMNAD_UMOUNT, clownfish_command_umount)
COMMAND.cc_add_argument(clownfish_command_filesystem_argument)
CLOWNFISH_COMMNADS[CLOWNFISH_COMMNAD_UMOUNT] = COMMAND


def clownfish_command_mount(connection, args):
    """
    Mount the filesystems
    """
    return clownfish_command_filesystem(connection, args, CLOWNFISH_COMMNAD_MOUNT)

COMMAND = ClownfishCommand(CLOWNFISH_COMMNAD_MOUNT, clownfish_command_mount)
COMMAND.cc_add_argument(clownfish_command_filesystem_argument)
CLOWNFISH_COMMNADS[CLOWNFISH_COMMNAD_MOUNT] = COMMAND


def clownfish_command_prepare(connection, args):
    """
    Prepare the hosts
    """
    # pylint: disable=redefined-variable-type,unused-argument
    log = connection.cc_command_log
    if ((CLOWNFISH_OPTION_SHORT_HELP in args) or
            (CLOWNFISH_OPTION_LONG_HELP in args)):
        log.cl_stdout("""Usage: %s <host>...
Prepare Lustre host(s), including installing Lustre RPMs etc.""" %
                      (CLOWNFISH_COMMNAD_PREPARE))
        return 0

    return connection.cc_instance.ci_prepare_all(log, connection.cc_workspace)


CLOWNFISH_COMMNADS[CLOWNFISH_COMMNAD_PREPARE] = \
    ClownfishCommand(CLOWNFISH_COMMNAD_PREPARE, clownfish_command_prepare,
                     speed=SPEED_ALWAYS_SLOW)


def clownfish_command_names():
    """
    Return the candidates of command
    """
    return sorted(CLOWNFISH_COMMNADS.keys())


def clownfish_interact_candidates(condition, line, begidx, endidx):
    """
    Return the candidates of interact
    """
    root_candidates = clownfish_command_names()
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
            command_name = words_before_cursor[0]
            if command_name not in CLOWNFISH_COMMNADS:
                return []
            command = CLOWNFISH_COMMNADS[command_name]
            candidates = command.cc_candidates(condition, words_before_cursor, being_completed)

    if len(being_completed) == 0:
        # Nothing to complete, return all of the possible candidates
        return candidates

    final_candidates = []
    for candidate in candidates:
        if not candidate.startswith(being_completed):
            continue
        final_candidates.append(candidate)

    return final_candidates
