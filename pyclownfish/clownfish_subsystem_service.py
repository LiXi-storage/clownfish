# Copyright (c) 2020 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com

"""
Subsystem of service
"""
from pyclownfish import clownfish_command_common
from pylcommon import lustre


SUBSYSTEM_SERVICE_COMMNAD_MOVE = "move"

SUBSYSTEM_SERVICE_NAME = "service"
SUBSYSTEM_SERVICE = clownfish_command_common.Subsystem(SUBSYSTEM_SERVICE_NAME)


def service_move_usage(log):
    """
    Usage of moving service
    """
    log.cl_stdout("""Usage: %s %s <service_name> <hostname>
        service_name: a Lustre service name, e.g. fsname-OST000a""" %
                  (SUBSYSTEM_SERVICE_NAME,
                   SUBSYSTEM_SERVICE_COMMNAD_MOVE))


def service_move(connection, args):
    """
    move the service(s)
    """
    # pylint: disable=too-many-branches
    log = connection.cc_command_log

    if ((clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP in args) or
            (clownfish_command_common.CLOWNFISH_OPTION_LONG_HELP in args)):
        service_move_usage(log)
        return 0

    instance = connection.cc_instance

    if len(args) != 2:
        service_move_usage(log)
        return -1

    service_name = args[0]
    hostname = args[1]

    service = instance.ci_name2service(service_name)
    if service is None:
        log.cl_error("invalid service name [%s]", service_name)
        return -1

    found = False
    for host in service.ls_hosts():
        if host.sh_hostname == hostname:
            found = True
            break

    if not found:
        log.cl_error("service [%s] doesn't have any instance on host [%s]",
                     service_name, hostname)
        return -1

    if service.ls_service_type == lustre.LUSTRE_SERVICE_TYPE_MGT:
        ret = service.ls_mount(log, hostname=hostname)
    else:
        ret = service.ls_lustre_fs.lf_mount_service(log, service, hostname=hostname)
    return ret


def service_move_argument(connection, complete_status):
    """
    Return argument that can be filesystem's service
    """
    instance = connection.cc_instance

    line = complete_status.ccs_line
    line_finished = line[0:complete_status.ccs_begidx]

    fields = line_finished.split()
    field_number = len(fields)

    # fields[0] and fields[1] should be "service" and "move"
    if field_number < 2:
        return []
    elif field_number == 2:
        candidates = []
        for lustrefs in instance.ci_lustres.values():
            for service in lustrefs.lf_service_dict.itervalues():
                if service.ls_service_name not in candidates:
                    candidates.append(service.ls_service_name)

        for mgs in instance.ci_mgs_dict.values():
            if mgs.ls_service_name not in candidates:
                candidates.append(mgs.ls_service_name)
        return candidates
    elif field_number == 3:
        service = instance.ci_name2service(fields[2])
        if service is None:
            return []
        candidates = []
        for host in service.ls_hosts():
            candidates.append(host.sh_hostname)

        return candidates
    else:
        return []


COMMAND = clownfish_command_common.ClownfishCommand(SUBSYSTEM_SERVICE_COMMNAD_MOVE, service_move)
COMMAND.cc_add_argument(service_move_argument)
SUBSYSTEM_SERVICE.ss_command_dict[SUBSYSTEM_SERVICE_COMMNAD_MOVE] = COMMAND

SUBSYSTEM_SERVICE_COMMNAD_UMOUNT = "umount"


def service_umount_usage(log):
    """
    Usage of moving service
    """
    log.cl_stdout("""Usage: %s %s <service_name>...
        service_name: a Lustre service name, e.g. fsname-OST000a""" %
                  (SUBSYSTEM_SERVICE_NAME,
                   SUBSYSTEM_SERVICE_COMMNAD_UMOUNT))


def service_umount(connection, args):
    """
    umount the service(s)
    """
    # pylint: disable=too-many-branches
    log = connection.cc_command_log

    if ((clownfish_command_common.CLOWNFISH_OPTION_SHORT_HELP in args) or
            (clownfish_command_common.CLOWNFISH_OPTION_LONG_HELP in args)):
        service_umount_usage(log)
        return 0

    instance = connection.cc_instance

    for service_name in args:
        service = instance.ci_name2service(service_name)
        if service is None:
            log.cl_stderr("service name [%s] is not configured in Clownfish", service_name)
            return -1

        if service.ls_service_type == lustre.LUSTRE_SERVICE_TYPE_MGT:
            ret = service.ls_umount(log)
        else:
            ret = service.ls_lustre_fs.lf_umount_service(log, service)
        if ret:
            return ret
    return ret


def service_umount_argument(connection, complete_status):
    """
    Return argument that can be filesystem's service
    """
    instance = connection.cc_instance

    line = complete_status.ccs_line
    line_finished = line[0:complete_status.ccs_begidx]

    fields = line_finished.split()
    field_number = len(fields)

    # fields[0] and fields[1] should be "service" and "umount"
    if field_number < 2:
        return []
    elif field_number == 2:
        candidates = []
        for lustrefs in instance.ci_lustres.values():
            for service in lustrefs.lf_service_dict.itervalues():
                if service.ls_service_name not in candidates:
                    candidates.append(service.ls_service_name)

        for mgs in instance.ci_mgs_dict.values():
            if mgs.ls_service_name not in candidates:
                candidates.append(mgs.ls_service_name)
        return candidates
    elif field_number == 3:
        service = instance.ci_name2service(fields[2])
        if service is None:
            return []
        candidates = []
        for host in service.ls_hosts():
            candidates.append(host.sh_hostname)

        return candidates
    else:
        return []


COMMAND = clownfish_command_common.ClownfishCommand(SUBSYSTEM_SERVICE_COMMNAD_UMOUNT, service_umount)
COMMAND.cc_add_argument(service_umount_argument)
SUBSYSTEM_SERVICE.ss_command_dict[SUBSYSTEM_SERVICE_COMMNAD_UMOUNT] = COMMAND
