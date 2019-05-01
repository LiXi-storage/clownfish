# Copyright (c) 2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com

"""
Command library of clownfish
"""


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


class Subsystem(object):
    """
    Subsystem
    """
    # pylint: disable=too-few-public-methods
    def __init__(self, name):
        self.ss_name = name
        # Key: command string. Value: calss ClownfishCommand
        self.ss_command_dict = {}

    def ss_help_all(self, connection):
        """
        Print help of all commands
        """
        log = connection.cc_command_log
        log.cl_stdout("----- %s commands -----", self.ss_name)
        for ccommand in self.ss_command_dict.values():
            ret = ccommand.cc_function(connection,
                                       [CLOWNFISH_OPTION_SHORT_HELP])
            if ret:
                log.cl_stderr("failed to print help message for command [%s]",
                              ccommand.cc_command)
                return -1
            log.cl_stdout("")
        return 0

# The command that can never finish within MAX_FAST_COMMAND_TIME
SPEED_ALWAYS_SLOW = "always_slow"
# The command that can always finish within MAX_FAST_COMMAND_TIME
SPEED_ALWAYS_FAST = "always_fast"
SPEED_SLOW_OR_FAST = "slow_or_fast"


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

    def _cc_all_candidates(self, connection, complete_status):
        """
        Return the candidates of arguments and also options
        """
        candidates = []
        for argument in self.cc_arguments:
            candidates += argument(connection, complete_status)

        for option in self.cc_options:
            if option.co_short_opt is not None:
                candidates.append(option.co_short_opt)
            candidates.append(option.co_long_opt)
        return candidates

    def cc_candidates(self, connection, complete_status):
        """
        Return the candidates of command
        """
        options = complete_status.ccs_options
        if len(options) == 0:
            # No option or argument yet, return all of the argument or option candidates
            return self._cc_all_candidates(connection, complete_status)

        last_word = options[-1]
        matched_option = None
        for option in self.cc_options:
            if option.co_short_opt == last_word or option.co_long_opt == last_word:
                matched_option = option
                break

        if matched_option is None:
            # Maybe a standalone argument
            return self._cc_all_candidates(connection, complete_status)
        elif not matched_option.co_has_arg:
            # Not expect any argument for this option
            return self._cc_all_candidates(connection, complete_status)
        elif matched_option.co_arg_funct is not None:
            return matched_option.co_arg_funct(connection, complete_status)
        else:
            # Option has argument, but do not know how to complete
            return []

CLOWNFISH_OPTION_SHORT_HELP = "-h"
CLOWNFISH_OPTION_LONG_HELP = "--help"
