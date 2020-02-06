# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Test Library for clownfish
Clownfish is an automatic management system for Lustre
"""
# pylint: disable=too-many-lines
import traceback
import os
import time
import yaml

# Local libs
from pylcommon import utils
from pylcommon import cstr
from pylcommon import cmd_general
from pylcommon import ssh_host
from pylcommon import test_common
from pylcommon import constants
from pylcommon import rwlock
from pyclownfish import clownfish_console
from pyclownfish import clownfish_command
from pyclownfish import clownfish_command_common
from pyclownfish import clownfish_subsystem_option
from pyclownfish import clownfish_install_nodeps

COMMAND_ABORT_TIMEOUT = 10
# Some command only quit when lock wait times out, so need to wait more time
# to let it happen
if COMMAND_ABORT_TIMEOUT < 2 * rwlock.LOCK_WAIT_TIMEOUT:
    COMMAND_ABORT_TIMEOUT = 2 * rwlock.LOCK_WAIT_TIMEOUT
CLOWNFISH_TESTS = []


def run_commands(log, cclient, cmds):
    """
    Run a list of commands, if exit status is none zero, return failure
    """
    result = log.cl_result
    cmd_index = 0
    for command in cmds:
        cclient.cc_command(log, command)
        if result.cr_exit_status:
            log.cl_error("failed to run command [%s] in command list %s, "
                         "comand index [%d]", command, cmds, cmd_index)
            return -1
        cmd_index += 1
    return 0


def delimiter_tests(log, workspace, cclient):
    """
    Tests of AND and OR
    """
    # pylint: disable=unused-argument,too-many-return-statements
    # pylint: disable=too-many-branches,too-many-statements
    result = log.cl_result
    # tailing AND is not allowed
    command = (clownfish_command.CLOWNFISH_COMMNAD_HELP + " " +
               clownfish_command.CLOWNFISH_DELIMITER_AND)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # AND AND is not allowed
    command = (clownfish_command.CLOWNFISH_COMMNAD_HELP + " " +
               clownfish_command.CLOWNFISH_DELIMITER_AND + " " +
               clownfish_command.CLOWNFISH_DELIMITER_AND + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # leading AND is not allowed
    command = (clownfish_command.CLOWNFISH_DELIMITER_AND + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # AND after failed command will quit
    command = (clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish_command.CLOWNFISH_DELIMITER_AND + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if (result.cr_exit_status == 0 or result.cr_stderr == "" or
            result.cr_stdout != ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # AND after succeeded command will execute
    command = (clownfish_command.CLOWNFISH_COMMNAD_HELP + " " +
               clownfish_command.CLOWNFISH_DELIMITER_AND + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if (result.cr_exit_status != 0 or result.cr_stderr != "" or
            result.cr_stdout == ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # AND: failure after succeeded command will return failure
    command = (clownfish_command.CLOWNFISH_COMMNAD_HELP + " " +
               clownfish_command.CLOWNFISH_DELIMITER_AND + " " +
               clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # AND: failure after succeeded commands will return failure
    command = (clownfish_command.CLOWNFISH_COMMNAD_HELP + " " +
               clownfish_command.CLOWNFISH_DELIMITER_AND + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP + " " +
               clownfish_command.CLOWNFISH_DELIMITER_AND + " " +
               clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT)
    cclient.cc_command(log, command)
    if (result.cr_exit_status == 0 or result.cr_stdout == "" or
            result.cr_stderr == ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # tailing OR is not allowed
    command = (clownfish_command.CLOWNFISH_COMMNAD_HELP + " " +
               clownfish_command.CLOWNFISH_DELIMITER_OR)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # leading OR is not allowed
    command = (clownfish_command.CLOWNFISH_DELIMITER_OR + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # OR OR is not allowed
    command = (clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish_command.CLOWNFISH_DELIMITER_OR + " " +
               clownfish_command.CLOWNFISH_DELIMITER_OR + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # OR: success after failed command will return success
    command = (clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish_command.CLOWNFISH_DELIMITER_OR + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if result.cr_exit_status != 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # OR: success after failed commands will return success
    command = (clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish_command.CLOWNFISH_DELIMITER_OR + " " +
               clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish_command.CLOWNFISH_DELIMITER_OR + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if result.cr_exit_status != 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # OR: success before OR will return sucess
    command = (clownfish_command.CLOWNFISH_COMMNAD_HELP + " " +
               clownfish_command.CLOWNFISH_DELIMITER_OR + " " +
               clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT)
    cclient.cc_command(log, command)
    if result.cr_exit_status != 0 or result.cr_stderr != "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # failure OR SUCESS AND sucess -> success
    command = (clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish_command.CLOWNFISH_DELIMITER_OR + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP + " " +
               clownfish_command.CLOWNFISH_DELIMITER_AND + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if (result.cr_exit_status != 0 or result.cr_stderr == ""
            or result.cr_stdout == ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # tailing CONT is not allowed
    command = (clownfish_command.CLOWNFISH_COMMNAD_HELP + " " +
               clownfish_command.CLOWNFISH_DELIMITER_CONT)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # leading CONT is not allowed
    command = (clownfish_command.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # CONT CONT is not allowed
    command = (clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish_command.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish_command.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0 or result.cr_stderr == "":
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # failure CONT success -> succeess
    command = (clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish_command.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if (result.cr_exit_status != 0 or result.cr_stderr == "" or
            result.cr_stdout == ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # success CONT failure -> failure
    command = (clownfish_command.CLOWNFISH_COMMNAD_HELP + " " +
               clownfish_command.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT)
    cclient.cc_command(log, command)
    if (result.cr_exit_status == 0 or result.cr_stderr == "" or
            result.cr_stdout == ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # failure CONT failure -> failure
    command = (clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT + " " +
               clownfish_command.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT)
    cclient.cc_command(log, command)
    if (result.cr_exit_status == 0 or result.cr_stderr == "" or
            result.cr_stdout != ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1

    # success CONT success -> success
    command = (clownfish_command.CLOWNFISH_COMMNAD_HELP + " " +
               clownfish_command.CLOWNFISH_DELIMITER_CONT + " " +
               clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cclient.cc_command(log, command)
    if (result.cr_exit_status != 0 or result.cr_stderr != "" or
            result.cr_stdout == ""):
        log.cl_error("unexpected result of command [%s]", command)
        return -1
    return 0


CLOWNFISH_TESTS.append(delimiter_tests)


def nonexistent_command(log, workspace, cclient):
    # pylint: disable=unused-argument,too-many-return-statements
    """
    Nonexistent command should return failure
    """
    result = log.cl_result

    command = clownfish_command.CLOWNFISH_COMMNAD_NONEXISTENT
    cclient.cc_command(log, command)
    if result.cr_exit_status == 0:
        log.cl_error("nonexistent command [%s] succeeded unexpectly", command)
        return -1
    elif result.cr_stderr == "":
        log.cl_error("nonexistent command [%s] has no error output", command)
        return -1

    return 0

CLOWNFISH_TESTS.append(nonexistent_command)


def help_all(log, workspace, cclient):
    # pylint: disable=unused-argument,too-many-return-statements
    """
    Run "help -a" to make sure helps of all commands work well
    """
    result = log.cl_result

    command = clownfish_command.CLOWNFISH_COMMNAD_HELP + " -a"
    cclient.cc_command(log, command)
    if result.cr_exit_status != 0 or result.cr_stderr != "":
        log.cl_error("command [%s] got failure", command)
        return -1

    return 0

CLOWNFISH_TESTS.append(help_all)


def umount_prepare_format_mount_umount_mount_format(log, workspace, cclient):
    """
    prepare the hosts, mount file systems, umount file system
    """
    # pylint: disable=invalid-name,unused-argument
    cmds = []
    # Enable lazy_prepare
    cmds.append(clownfish_subsystem_option.SUBSYSTEM_OPTION_NAME + " " +
                clownfish_subsystem_option.CLOWNFISH_COMMNAD_ENABLE + " " +
                cstr.CSTR_LAZY_PREPARE)
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_UMOUNT_ALL)
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_PREPARE)
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_FORMAT_ALL + " -f")
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_MOUNT_ALL)
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_UMOUNT_ALL)
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_MOUNT_ALL)
    # This tests that format will umount automatically
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_FORMAT_ALL + " -f")

    ret = run_commands(log, cclient, cmds)
    if ret:
        log.cl_error("failed to do umount_prepare_format_mount_umount_mount_format")
        return -1
    return 0


CLOWNFISH_TESTS.append(umount_prepare_format_mount_umount_mount_format)


def abort_command(log, workspace, cclient):
    """
    Test of abort a command
    """
    # pylint: disable=unused-argument
    # Change to ROOT directory
    result = log.cl_result
    cclient.cc_abort_event.set()
    cmdlines = []
    cmdlines.append(clownfish_subsystem_option.SUBSYSTEM_OPTION_NAME + " " +
                    clownfish_subsystem_option.CLOWNFISH_COMMNAD_DISABLE + " " +
                    cstr.CSTR_LAZY_PREPARE)
    cmdlines.append(clownfish_subsystem_option.SUBSYSTEM_OPTION_NAME + " " +
                    clownfish_subsystem_option.CLOWNFISH_COMMNAD_ENABLE + " " +
                    cstr.CSTR_LAZY_PREPARE)
    cmdlines.append(clownfish_command.CLOWNFISH_COMMNAD_FORMAT_ALL + " -f")
    cmdlines.append(clownfish_command.CLOWNFISH_COMMNAD_MOUNT_ALL)
    cmdlines.append(clownfish_command.CLOWNFISH_COMMNAD_UMOUNT_ALL)
    cmdlines.append(clownfish_command.CLOWNFISH_COMMNAD_HELP)
    cmdlines.append(clownfish_command.CLOWNFISH_COMMNAD_RETVAL)
    cmdlines.append(clownfish_command.CLOWNFISH_COMMNAD_PREPARE)
    for cmdline in cmdlines:
        args = cmdline.split()
        ccommand = clownfish_command.args2command(args)
        if ccommand is None:
            log.cl_error("failed to find command from cmdline [%s]",
                         cmdline)
            return -1

        time_start = time.time()
        log.cl_info("start to run command [%s] at time [%s]", cmdline,
                    time_start)
        cclient.cc_command(log, cmdline)
        time_end = time.time()
        log.cl_info("finished running command [%s] at time [%s]", cmdline,
                    time_end)
        if time_start + COMMAND_ABORT_TIMEOUT < time_end:
            log.cl_error("command [%s] costs [%s] seconds even when aborting",
                         cmdline, time_end - time_start)
            return -1

        if ccommand.cc_speed == clownfish_command_common.SPEED_ALWAYS_SLOW:
            if result.cr_exit_status == 0:
                log.cl_error("slow command [%s] should return failure when "
                             "aborting", cmdline)
                return -1
        elif ccommand.cc_speed == clownfish_command_common.SPEED_ALWAYS_FAST:
            if result.cr_exit_status != 0:
                log.cl_error("quick command [%s] should succeed when "
                             "aborting", cmdline)
                return -1
    cclient.cc_abort_event.clear()
    return 0


CLOWNFISH_TESTS.append(abort_command)


def prepare_twice(log, workspace, cclient):
    """
    Test of running prepare for twice
    """
    # pylint: disable=unused-argument
    # EX-234: failure when prepare for multiple times
    result = log.cl_result
    command = clownfish_command.CLOWNFISH_COMMNAD_PREPARE
    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1

    cclient.cc_command(log, command)
    if result.cr_exit_status:
        log.cl_error("failed to run command [%s]", command)
        return -1
    return 0


CLOWNFISH_TESTS.append(prepare_twice)


def umount_x2_mount_x2_umount_x2(log, workspace, cclient):
    """
    1) umount twice, 2) mount twice and 3) umount twice
    Umount or mount twice makes sure the commands can be run for multiple times
    """
    # pylint: disable=invalid-name,unused-argument
    cmds = []
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_UMOUNT_ALL)
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_UMOUNT_ALL)
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_MOUNT_ALL)
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_MOUNT_ALL)
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_UMOUNT_ALL)
    cmds.append(clownfish_command.CLOWNFISH_COMMNAD_UMOUNT_ALL)
    ret = run_commands(log, cclient, cmds)
    if ret:
        log.cl_error("failed to do umount_x2_mount_x2_umount_x2 to all file"
                     "system(s)")
        return -1
    return 0

CLOWNFISH_TESTS.append(umount_x2_mount_x2_umount_x2)


def do_test_connected(log, workspace, console_client,
                      test_config, test_config_fpath,
                      test_functs):
    """
    Run test with the console connected
    """
     # pylint: disable=too-many-branches,too-many-locals,too-many-arguments
    test_dict = {}
    for test_funct in test_functs:
        test_dict[test_funct.__name__] = test_funct

    quit_on_error = True
    only_test_configs = utils.config_value(test_config,
                                           cstr.CSTR_ONLY_TESTS)
    if only_test_configs is None:
        log.cl_debug("no [%s] is configured, run all tests",
                     cstr.CSTR_ONLY_TESTS)
        selected_tests = test_functs
    else:
        selected_tests = []
        for test_name in only_test_configs:
            if test_name not in test_dict:
                log.cl_error("test [%s] doenot exist, please correct file "
                             "[%s]", test_name, test_config_fpath)
                return -1
            test_funct = test_dict[test_name]
            selected_tests.append(test_funct)

    not_selected_tests = []
    for test_funct in test_functs:
        if test_funct not in selected_tests:
            not_selected_tests.append(test_funct)

    passed_tests = []
    failed_tests = []
    skipped_tests = []

    for test_func in selected_tests:
        test_workspace = workspace + "/" + test_func.__name__
        ret = utils.mkdir(test_workspace)
        if ret:
            log.cl_error("failed to create directory [%s] on local host",
                         test_workspace)
            return -1

        ret = test_func(log, test_workspace, console_client)
        if ret < 0:
            log.cl_error("test [%s] failed", test_func.__name__)
            failed_tests.append(test_func)
            if quit_on_error:
                return -1
        elif ret == 1:
            log.cl_warning("test [%s] skipped", test_func.__name__)
            skipped_tests.append(test_func)
        else:
            log.cl_info("test [%s] passed", test_func.__name__)
            passed_tests.append(test_func)

    if len(not_selected_tests) != 0:
        for not_selected_test in not_selected_tests:
            log.cl_warning("test [%s] is not selected", not_selected_test.__name__)

    if len(skipped_tests) != 0:
        for skipped_test in skipped_tests:
            log.cl_warning("test [%s] skipped", skipped_test.__name__)

    ret = 0
    if len(failed_tests) != 0:
        for failed_test in failed_tests:
            log.cl_error("test [%s] failed", failed_test.__name__)
            ret = -1

    if len(passed_tests) != 0:
        for passed_test in passed_tests:
            log.cl_info("test [%s] passed", passed_test.__name__)
    return ret


def connect_and_test(log, workspace, test_config, test_config_fpath,
                     install_config, install_config_fpath,
                     clownfish_config, clownfish_config_fpath,
                     test_functs):
    """
    Connect Clownfish and test
    """
    # pylint: disable=too-many-arguments
    clownfish_server_ip = utils.config_value(install_config, cstr.CSTR_VIRTUAL_IP)
    if not clownfish_server_ip:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_VIRTUAL_IP, install_config_fpath)
        return -1

    clownfish_server_port = utils.config_value(clownfish_config,
                                               cstr.CSTR_CLOWNFISH_PORT)
    if clownfish_server_port is None:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_CLOWNFISH_PORT, clownfish_config_fpath)
        return -1

    server_url = "tcp://%s:%s" % (clownfish_server_ip, clownfish_server_port)
    console_client = clownfish_console.ClownfishClient(log, workspace,
                                                       server_url)
    ret = console_client.cc_init()
    if ret == 0:
        ret = do_test_connected(log, workspace, console_client,
                                test_config, test_config_fpath,
                                test_functs)
        if ret:
            log.cl_error("failed to run test with console connected to "
                         "Clownfish server")
    else:
        log.cl_error("failed to connect to Clownfish server")

    # No matter connection fails or not, need to finish
    console_client.cc_fini()
    return ret


def clownfish_send_packages(log, install_config,
                            install_config_fpath,
                            config, config_fpath):
    """
    Send the required packages to the server hosts
    """
    # pylint: disable=too-many-locals,too-many-branches
    server_hosts = clownfish_install_nodeps.clownfish_parse_server_hosts(log,
                                                                         install_config,
                                                                         install_config_fpath)
    if server_hosts is None:
        log.cl_error("failed to parse Clownfish server hosts, please correct "
                     "file [%s]", install_config_fpath)
        return -1

    dist_configs = utils.config_value(config, cstr.CSTR_LUSTRE_DISTRIBUTIONS)
    if dist_configs is None:
        log.cl_error("can NOT find [%s] in the config file, "
                     "please correct file [%s]",
                     cstr.CSTR_LUSTRE_DISTRIBUTIONS, config_fpath)
        return None

    packages = []
    for dist_config in dist_configs:
        lustre_rpm_dir = utils.config_value(dist_config,
                                            cstr.CSTR_LUSTRE_RPM_DIR)
        if lustre_rpm_dir is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_LUSTRE_RPM_DIR, config_fpath)
            return None
        lustre_rpm_dir = lustre_rpm_dir.rstrip("/")
        if lustre_rpm_dir not in packages:
            packages.append(lustre_rpm_dir)

        e2fsprogs_rpm_dir = utils.config_value(dist_config,
                                               cstr.CSTR_E2FSPROGS_RPM_DIR)
        if e2fsprogs_rpm_dir is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_E2FSPROGS_RPM_DIR, config_fpath)
            return None

        e2fsprogs_rpm_dir = e2fsprogs_rpm_dir.rstrip("/")
        if e2fsprogs_rpm_dir not in packages:
            packages.append(e2fsprogs_rpm_dir)

    iso_path = utils.config_value(config, cstr.CSTR_ISO_PATH)
    if iso_path is None:
        log.cl_info("no [%s] in the config file", cstr.CSTR_ISO_PATH)
    elif not os.path.exists(iso_path):
        log.cl_error("ISO file [%s] doesn't exist", iso_path)
        return None
    packages.append(iso_path)

    for server_host in server_hosts:
        for package in packages:
            parent = os.path.dirname(package)
            command = "mkdir -p %s" % parent
            retval = server_host.sh_run(log, command)
            if retval.cr_exit_status:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             server_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            ret = server_host.sh_send_file(log, package, parent)
            if ret:
                log.cl_error("failed to send file [%s] on local host to "
                             "directory [%s] on host [%s]",
                             package, parent,
                             server_host.sh_hostname)
                return -1
    return 0


def clownfish_do_test(log, workspace, test_config, test_config_fpath):
    """
    Start to test
    """
    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
    # pylint: disable=too-many-statements
    ret = test_common.test_install_virt(log, workspace, test_config,
                                        test_config_fpath)
    if ret:
        log.cl_error("failed to install virtual machine")
        return -1

    install_config_fpath = utils.config_value(test_config,
                                              cstr.CSTR_INSTALL_CONFIG)
    if install_config_fpath is None:
        log.cl_error("can NOT find [%s] in the test config, "
                     "please correct file [%s]",
                     cstr.CSTR_INSTALL_CONFIG, test_config_fpath)
        return -1

    skip_install = utils.config_value(test_config,
                                      cstr.CSTR_SKIP_INSTALL)
    if skip_install is None:
        log.cl_debug("no [%s] is configured, do not skip install")
        skip_install = False

    install_config_fd = open(install_config_fpath)
    ret = 0
    try:
        install_config = yaml.load(install_config_fd)
    except:
        log.cl_error("not able to load [%s] as yaml file: %s",
                     install_config_fpath, traceback.format_exc())
        ret = -1
    install_config_fd.close()
    if ret:
        return -1

    clownfish_config_fpath = utils.config_value(install_config,
                                                cstr.CSTR_CONFIG_FPATH)
    if clownfish_config_fpath is None:
        log.cl_error("can NOT find [%s] in the installation config, "
                     "please correct file [%s]",
                     cstr.CSTR_CONFIG_FPATH, install_config_fpath)
        return -1

    clownfish_config_fd = open(clownfish_config_fpath)
    ret = 0
    try:
        clownfish_config = yaml.load(clownfish_config_fd)
    except:
        log.cl_error("not able to load [%s] as yaml file: %s",
                     clownfish_config_fpath, traceback.format_exc())
        ret = -1
    clownfish_config_fd.close()
    if ret:
        return -1

    if not skip_install:
        ret = clownfish_send_packages(log, install_config,
                                      install_config_fpath,
                                      clownfish_config,
                                      clownfish_config_fpath)
        if ret:
            log.cl_error("failed to send Lustre RPMs")
            return -1

    install_server_config = utils.config_value(test_config,
                                               cstr.CSTR_INSTALL_SERVER)
    if install_server_config is None:
        log.cl_error("can NOT find [%s] in the config file [%s], "
                     "please correct it", cstr.CSTR_INSTALL_SERVER,
                     test_config_fpath)
        return -1

    install_server_hostname = utils.config_value(install_server_config,
                                                 cstr.CSTR_HOSTNAME)
    if install_server_hostname is None:
        log.cl_error("can NOT find [%s] in the config of installation host, "
                     "please correct file [%s]",
                     cstr.CSTR_HOSTNAME, test_config_fpath)
        return None

    ssh_identity_file = utils.config_value(install_server_config,
                                           cstr.CSTR_SSH_IDENTITY_FILE)
    install_server = ssh_host.SSHHost(install_server_hostname,
                                      identity_file=ssh_identity_file)

    ret = test_common.test_install(log, workspace, install_config_fpath,
                                   skip_install, install_server, "clownfish",
                                   constants.CLOWNFISH_INSTALL_CONFIG_FNAME)
    if ret:
        log.cl_error("failed to test installation of Clownfish")
        return -1

    ret = connect_and_test(log, workspace, test_config,
                           test_config_fpath, install_config,
                           install_config_fpath, clownfish_config,
                           clownfish_config_fpath, CLOWNFISH_TESTS)
    return ret


def clownfish_test(log, workspace, config_fpath):
    """
    Start to test holding the confiure lock
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
        ret = clownfish_do_test(log, workspace, config, config_fpath)
    except:
        ret = -1
        log.cl_error("exception: %s", traceback.format_exc())

    if ret:
        log.cl_error("test of Clownfish failed, please check [%s] for more "
                     "log", workspace)
    else:
        log.cl_info("test of Clownfish passed, please check [%s] "
                    "for more log", workspace)
    return ret


def main():
    """
    Start clownfish test
    """
    cmd_general.main(constants.CLOWNFISH_TEST_CONFIG,
                     constants.CLOWNFISH_TEST_LOG_DIR,
                     clownfish_test)
