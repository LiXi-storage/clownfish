# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for clownfish that manages Lustre
Clownfish is an automatic management system for Lustre
"""
# pylint: disable=too-many-lines
import threading
import os
import time
import prettytable

# Local libs
from pylcommon import utils
from pylcommon import parallel
from pylcommon import lustre
from pylcommon import cstr
from pylcommon import ssh_host
from pyclownfish import clownfish_qos
from pyclownfish import corosync

CLOWNFISH_STATUS_CHECK_INTERVAL = 1


class ClownfishServiceStatus(object):
    """
    A global object for service status
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, instance, log):
        self.css_instance = instance
        # Keys are the LustreService.ls_service_name, value is instance of
        # LustreServiceStatus
        self.css_service_status_dict = {}
        # Protects css_service_status_dict
        self.css_service_status_condition = threading.Condition()
        # The status of services that have problems.
        # Keys are the LustreService.ls_service_name, value is instance of
        # LustreServiceStatus
        self.css_problem_status_dict = {}
        # Protects css_problem_status_dict and css_fix_thread_waiting_number
        self.css_problem_condition = threading.Condition()
        # The fixing time of services
        # Keys are teh LustreService.ls_service_name, value is time.time()
        self.css_fix_time_dict = {}
        # The fixing services
        self.css_fix_services = []
        # Protected by css_problem_condition
        self.css_fix_thread_waiting_number = 0
        self.css_fix_thread_number = 5
        self.css_log = log
        self.css_start_status_threads()
        self.css_start_fix_threads()

    def css_service_status(self, service_name):
        """
        Return the service status
        """
        self.css_service_status_condition.acquire()
        if service_name in self.css_service_status_dict:
            status = self.css_service_status_dict[service_name]
        else:
            status = None
        self.css_service_status_condition.release()
        return status

    def css_update_status(self, status):
        """
        Update the status
        """
        service = status.lss_service
        service_name = service.ls_service_name
        self.css_service_status_condition.acquire()
        self.css_service_status_dict[service_name] = status
        self.css_service_status_condition.release()

        self.css_problem_condition.acquire()
        if status.lss_has_problem():
            self.css_problem_status_dict[service_name] = status
            self.css_problem_condition.notifyAll()
        else:
            if service_name in self.css_problem_status_dict:
                del self.css_problem_status_dict[service_name]
        self.css_problem_condition.release()

    def css_status_thread(self, service):
        """
        Thread that checks status of a service
        """
        service_name = service.ls_service_name
        instance = self.css_instance

        name = "thread_checking_service_%s" % service_name
        thread_workspace = instance.ci_workspace + "/" + name
        if not os.path.exists(thread_workspace):
            ret = utils.mkdir(thread_workspace)
            if ret:
                self.css_log.cl_error("failed to create direcotry [%s] on local host",
                                      thread_workspace)
                return -1
        elif not os.path.isdir(thread_workspace):
            self.css_log.cl_error("[%s] is not a directory", thread_workspace)
            return -1
        log = self.css_log.cl_get_child(name, resultsdir=thread_workspace)

        log.cl_info("starting thread that checks status of service [%s]",
                    service_name)
        while instance.ci_running:
            status = lustre.LustreServiceStatus(service)
            status.lss_check(log)

            self.css_update_status(status)

            time.sleep(CLOWNFISH_STATUS_CHECK_INTERVAL)
        log.cl_info("thread that checks status of service [%s] exited",
                    service_name)
        return 0

    def css_start_status_threads(self):
        """
        Start the status thread
        """
        instance = self.css_instance

        service_dict = {}

        for lustrefs in instance.ci_lustres.values():
            services = lustrefs.lf_services()
            for service in services:
                if service.ls_service_name not in service_dict:
                    service_dict[service.ls_service_name] = True
                    utils.thread_start(self.css_status_thread,
                                       (service, ))

        for mgs in instance.ci_mgs_dict.values():
            if mgs.ls_service_name not in service_dict:
                utils.thread_start(self.css_status_thread,
                                   (mgs, ))
                service_dict[mgs.ls_service_name] = True

    def css_fix_thread(self, thread_id):
        """
        Thread that fix the services
        """
        # pylint: disable=too-many-branches,too-many-statements,too-many-locals
        instance = self.css_instance

        name = "thread_fixing_service_%s" % thread_id
        thread_workspace = instance.ci_workspace + "/" + name
        if not os.path.exists(thread_workspace):
            ret = utils.mkdir(thread_workspace)
            if ret:
                self.css_log.cl_error("failed to create direcotry [%s] on local host",
                                      thread_workspace)
                return -1
        elif not os.path.isdir(thread_workspace):
            self.css_log.cl_error("[%s] is not a directory", thread_workspace)
            return -1
        log = self.css_log.cl_get_child(name, resultsdir=thread_workspace)

        log.cl_info("starting thread [%s] that fix services", thread_id)
        fixing_status = None
        while instance.ci_running:
            self.css_problem_condition.acquire()
            if fixing_status is not None:
                fix_name = fixing_status.lss_service.ls_service_name
                assert fix_name in self.css_fix_services
                fix_index = self.css_fix_services.index(fix_name)
                del self.css_fix_services[fix_index]

            # When HA is disabled, this thread does nothing
            self.css_fix_thread_waiting_number += 1
            self.css_problem_condition.notifyAll()
            while ((not instance.ci_native_ha) or
                   (len(self.css_problem_status_dict) == 0)):
                self.css_problem_condition.wait()
            self.css_fix_thread_waiting_number -= 1
            #
            # Do no remove the status from the dictionary, remove it after
            # fixing, because the check threads might add the status when
            # fixing anyway.
            #
            # The priority level of services is:
            # 1. The MGS or the MDT combined with MGS
            # 2. The MDTs
            # 3. The OSTs
            # For the services that have the same priority, the service that
            # has smaller fix time has the higher priority
            fixing_status = None
            for status in self.css_problem_status_dict.values():
                service = status.lss_service
                service_type = service.ls_service_type
                service_name = service.ls_service_name

                if service_name in self.css_fix_services:
                    continue

                if fixing_status is None:
                    fixing_status = status
                    continue

                fix_service = fixing_status.lss_service
                fix_type = fix_service.ls_service_type
                fix_name = fix_service.ls_service_name
                fix_is_mgs = bool((fix_type == lustre.LUSTRE_SERVICE_TYPE_MGT) or
                                  (fix_type == lustre.LUSTRE_SERVICE_TYPE_MDT and
                                   fix_service.lmdt_is_mgs))

                is_mgs = bool((service_type == lustre.LUSTRE_SERVICE_TYPE_MGT) or
                              (service_type == lustre.LUSTRE_SERVICE_TYPE_MDT and
                               service.lmdt_is_mgs))

                if fix_name not in self.css_fix_time_dict:
                    fix_time_ealier = True
                elif service_name not in self.css_fix_time_dict:
                    fix_time_ealier = False
                else:
                    fix_time_ealier = bool(self.css_fix_time_dict[fix_name] <=
                                           self.css_fix_time_dict[service_name])

                if is_mgs:
                    if fix_is_mgs:
                        if not fix_time_ealier:
                            fixing_status = status
                    else:
                        fixing_status = status
                elif service_type == lustre.LUSTRE_SERVICE_TYPE_MDT:
                    if fix_is_mgs:
                        pass
                    elif fix_type == lustre.LUSTRE_SERVICE_TYPE_MDT:
                        if not fix_time_ealier:
                            fixing_status = status
                    else:
                        fixing_status = status
                else:
                    if fix_is_mgs:
                        pass
                    elif fix_type == lustre.LUSTRE_SERVICE_TYPE_MDT:
                        pass
                    else:
                        if not fix_time_ealier:
                            fixing_status = status
            if fixing_status is not None:
                fix_name = fixing_status.lss_service.ls_service_name
                self.css_fix_time_dict[fix_name] = time.time()
                assert fix_name not in self.css_fix_services
                self.css_fix_services.append(fix_name)
            self.css_problem_condition.release()

            if fixing_status is None:
                continue

            service = fixing_status.lss_service
            service_name = service.ls_service_name

            log.cl_info("checking the status of service [%s]", service_name)
            # Check the status by myself, since the status might be outdated
            status = lustre.LustreServiceStatus(fixing_status.lss_service)
            status.lss_check(log)
            if status.lss_has_problem():
                ret = status.lss_fix_problem(log)
                if ret:
                    log.cl_error("failed to fix problem of service [%s]",
                                 service_name)

                service = fixing_status.lss_service
                service_name = service.ls_service_name
                status = lustre.LustreServiceStatus(fixing_status.lss_service)
                status.lss_check(log)
                if status.lss_has_problem():
                    log.cl_error("service [%s] still has problem after fixing",
                                 service_name)
                else:
                    log.cl_info("service [%s] was successfully fixed",
                                service_name)
            else:
                log.cl_info("the problem of service [%s] has disapeared "
                            "without fixing", service_name)

            # Update the status
            self.css_update_status(status)
        log.cl_info("thread [%s] that fix services exited", thread_id)

    def css_start_fix_threads(self):
        """
        Start the status thread
        """
        for thread_id in range(self.css_fix_thread_number):
            utils.thread_start(self.css_fix_thread, (thread_id, ))


class ClownfishInstance(object):
    """
    This instance saves the global clownfish information
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    # pylint: disable=too-many-arguments,too-many-public-methods
    def __init__(self, log, workspace, lazy_prepare, hosts, mgs_dict, lustres,
                 natvie_ha, corosync_cluster, qos_dict, iso_path, local_host,
                 mnt_path, no_operation=False):
        self.ci_lazy_prepare = lazy_prepare
        # Keys are the host IDs, not the hostnames
        self.ci_hosts = hosts
        # Keys are the MGS IDs, values are instances of LustreService
        self.ci_mgs_dict = mgs_dict
        # Keys are the fsnames, values are instances of LustreFilesystem
        self.ci_lustres = lustres
        self.ci_workspace = workspace
        self.ci_running = True
        # Whether native hight availability is enabled
        self.ci_native_ha = natvie_ha
        # LustreCorosyncCluster, if disabled, none
        self.ci_corosync_cluster = corosync_cluster
        # ISO path of Clownfish
        self.ci_iso_path = iso_path
        if not no_operation:
            self.ci_service_status = ClownfishServiceStatus(self, log)
        self.ci_qos_dict = qos_dict
        # Local host to umount the ISO
        self.ci_local_host = local_host
        # The mnt path of the ISO
        self.ci_mnt_path = mnt_path

    def ci_mount_lustres(self, log):
        """
        Mount all Lustre file systems, including MGS if necessary
        """
        for lustrefs in self.ci_lustres.values():
            if log.cl_abort:
                log.cl_stderr("aborting mounting file systems")
                return -1
            ret = lustrefs.lf_mount(log)
            if ret:
                log.cl_stderr("failed to mount file system [%s]",
                              lustrefs.lf_fsname)
                return -1
        return 0

    def ci_umount_lustres(self, log):
        """
        Umount all Lustre file systems, not including MGS
        """
        for lustrefs in self.ci_lustres.values():
            ret = lustrefs.lf_umount(log)
            if ret:
                log.cl_stderr("failed to umount file system [%s]",
                              lustrefs.lf_fsname)
                return -1
        return 0

    def ci_mount_mgs(self, log):
        """
        Mount all MGS
        """
        for mgs in self.ci_mgs_dict.values():
            ret = mgs.ls_mount(log)
            if ret:
                log.cl_stderr("failed to mount MGS [%s]",
                              mgs.ls_service_name)
                return -1
        return 0

    def ci_umount_mgs(self, log):
        """
        Umount all MGS
        """
        for mgs in self.ci_mgs_dict.values():
            ret = mgs.ls_umount(log)
            if ret:
                log.cl_stderr("failed to umount MGS [%s]",
                              mgs.ls_service_name)
                return -1
        return 0

    def ci_umount_all(self, log):
        """
        Umount all file system and MGS
        """
        ret = self.ci_umount_lustres(log)
        if ret:
            log.cl_stderr("failed to umount all Lustre file systems")
            return -1

        ret = self.ci_umount_mgs(log)
        if ret:
            log.cl_stderr("failed to umount all MGS")
            return -1

        return 0

    def ci_umount_all_nolock(self, log):
        """
        Umount all file system and MGS
        Locks should be held
        """
        for lustrefs in self.ci_lustres.values():
            ret = lustrefs.lf_umount_nolock(log)
            if ret:
                log.cl_stderr("failed to umount file system [%s]",
                              lustrefs.lf_fsname)
                return -1

        for mgs in self.ci_mgs_dict.values():
            ret = mgs.ls_umount_nolock(log)
            if ret:
                log.cl_stderr("failed to umount MGS [%s]",
                              mgs.ls_service_name)
                return -1
        return 0

    def ci_mount_all(self, log):
        """
        Mount all file system and MGS
        """
        ret = self.ci_mount_mgs(log)
        if ret:
            log.cl_stderr("failed to mount all MGS")
            return -1

        ret = self.ci_mount_lustres(log)
        if ret:
            log.cl_stderr("failed to mount all Lustre file systems")
            return -1

        return 0

    def ci_format_all_nolock(self, log):
        """
        Format all file system and MGS
        Locks should be held
        """
        ret = self.ci_umount_all_nolock(log)
        if ret:
            log.cl_stderr("failed to umount all")
            return ret

        for mgs in self.ci_mgs_dict.values():
            ret = mgs.ls_format_nolock(log)
            if ret:
                log.cl_stderr("failed to umount and format MGS [%s]",
                              mgs.ls_service_name)
                return -1

        for lustrefs in self.ci_lustres.values():
            ret = lustrefs.lf_format_nolock(log)
            if ret:
                log.cl_stderr("failed to umount and format Lustre file system "
                              "[%s]",
                              lustrefs.lf_fsname)
                return -1
        return 0

    def ci_format_all(self, log):
        """
        Format all file system and MGS
        """
        lock_handles = []
        for mgs in self.ci_mgs_dict.values():
            mgs_lock_handle = mgs.ls_lock.rwl_writer_acquire(log)
            if mgs_lock_handle is None:
                log.cl_stderr("aborting formating all file systems and MGS")
                for lock_handle in reversed(lock_handles):
                    lock_handle.rwh_release()
                return -1
            lock_handles.append(mgs_lock_handle)

        for lustrefs in self.ci_lustres.values():
            fs_lock_handle = lustrefs.lf_lock.rwl_writer_acquire(log)
            if fs_lock_handle is None:
                log.cl_stderr("aborting formating all file systems and MGS")
                for lock_handle in reversed(lock_handles):
                    lock_handle.rwh_release()
                return -1
            lock_handles.append(fs_lock_handle)

        ret = self.ci_format_all_nolock(log)

        for lock_handle in reversed(lock_handles):
            lock_handle.rwh_release()

        return ret

    def ci_prepare_all_nolock(self, log, workspace):
        """
        Prepare all hosts
        Locks should be held
        """
        ret = self.ci_umount_all_nolock(log)
        if ret:
            log.cl_stderr("failed to umount all")
            return ret

        args_array = []
        thread_ids = []
        for host in self.ci_hosts.values():
            args = (host, self.ci_lazy_prepare)
            args_array.append(args)
            thread_id = "prepare_%s" % host.sh_host_id
            thread_ids.append(thread_id)

        parallel_execute = parallel.ParallelExecute(log, workspace,
                                                    "host_prepare",
                                                    lustre.host_lustre_prepare,
                                                    args_array,
                                                    thread_ids=thread_ids,
                                                    parallelism=8)
        ret = parallel_execute.pe_run()

        if ret == 0 and self.ci_corosync_cluster is not None:
            ret = self.ci_corosync_cluster.ic_install(log, [], ["corosync", "pcs"])
            if ret:
                log.cl_error("failed to install Lustre corosync cluster")
                return -1

            ret = self.ci_corosync_cluster.lcc_config(log, workspace)
            if ret:
                log.cl_error("failed to configure Lustre corosync cluster")
                return -1

            ret = self.ci_corosync_cluster.ccl_start(log)
            if ret:
                log.cl_error("failed to start Lustre corosync cluster")
                return -1
        return ret

    def ci_prepare_all(self, log, workspace):
        """
        Prepare all hosts
        """
        lock_handles = []
        for mgs in self.ci_mgs_dict.values():
            mgs_lock_handle = mgs.ls_lock.rwl_writer_acquire(log)
            if mgs_lock_handle is None:
                log.cl_stderr("aborting preparing all hosts")
                for lock_handle in reversed(lock_handles):
                    lock_handle.rwh_release()
                return -1
            lock_handles.append(mgs_lock_handle)

        for lustrefs in self.ci_lustres.values():
            fs_lock_handle = lustrefs.lf_lock.rwl_writer_acquire(log)
            if fs_lock_handle is None:
                log.cl_stderr("aborting preparing all hosts")
                for lock_handle in reversed(lock_handles):
                    lock_handle.rwh_release()
                return -1
            lock_handles.append(fs_lock_handle)

        ret = self.ci_prepare_all_nolock(log, workspace)

        for lock_handle in reversed(lock_handles):
            lock_handle.rwh_release()

        return ret

    def ci_fini(self, log):
        """
        quiting
        """
        self.ci_running = False

        ret = 0
        command = ("umount %s" % (self.ci_mnt_path))
        retval = self.ci_local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.ci_local_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            ret = -1

        command = ("rmdir %s" % (self.ci_mnt_path))
        retval = self.ci_local_host.sh_run(log, command)
        if retval.cr_exit_status:
            log.cl_error("failed to run command [%s] on host [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         command,
                         self.ci_local_host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            ret = -1
        return ret

    def ci_native_ha_enable(self):
        """
        Enable high availability
        """
        self.ci_native_ha = True

    def ci_native_ha_disable(self, log):
        """
        disable high availability
        """
        service_status = self.ci_service_status

        self.ci_native_ha = False
        ret = 0
        service_status.css_problem_condition.acquire()
        while (service_status.css_fix_thread_waiting_number !=
               service_status.css_fix_thread_number):
            if log.cl_abort:
                ret = -1
                break
            service_status.css_problem_condition.wait()
        service_status.css_problem_condition.release()

        if log.cl_abort or ret < 0:
            ret = -1
            log.cl_stderr("abort waiting high availability to be disabled")
        return ret

    def ci_list_lustre(self, log):
        """
        Print information about Lustre filesystems
        """
        log.cl_stdout("Lustre filesystems")
        table = prettytable.PrettyTable()
        table.field_names = ["Filesystem name"]
        for lustrefs in self.ci_lustres.values():
            table.add_row([lustrefs.lf_fsname])
        log.cl_stdout(table)


def parse_qos_user_config(log, lustre_fs, qos_user_config, config_fpath,
                          qos_users, interval,
                          default_mbps_threshold, default_iops_threshold,
                          default_throttled_oss_rpc_rate,
                          default_throttled_mds_rpc_rate):
    # pylint: disable=too-many-arguments,too-many-locals
    """
    Parse the config for QoS user
    """
    uid = utils.config_value(qos_user_config, cstr.CSTR_UID)
    if uid is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_UID,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1

    uid = str(uid)
    if uid in qos_users:
        log.cl_error("multiple uid [%s] configured for QoS of file system "
                     "[%s], please correct file [%s]",
                     cstr.CSTR_UID,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1

    mbps_threshold = utils.config_value(qos_user_config,
                                        cstr.CSTR_MBPS_THRESHOLD)
    if mbps_threshold is None:
        log.cl_debug("no [%s] is configured for user [%s] of file system [%s], "
                     "use default value [%s]",
                     cstr.CSTR_MBPS_THRESHOLD,
                     uid,
                     lustre_fs.lf_fsname,
                     default_mbps_threshold)
        mbps_threshold = default_mbps_threshold

    iops_threshold = utils.config_value(qos_user_config,
                                        cstr.CSTR_IOPS_THRESHOLD)
    if iops_threshold is None:
        log.cl_debug("no [%s] is configured for user [%s] of file system [%s], "
                     "use default value [%s]",
                     cstr.CSTR_IOPS_THRESHOLD,
                     uid,
                     lustre_fs.lf_fsname,
                     default_iops_threshold)
        iops_threshold = default_iops_threshold

    throttled_oss_rpc_rate = utils.config_value(qos_user_config,
                                                cstr.CSTR_THROTTLED_OSS_RPC_RATE)
    if throttled_oss_rpc_rate is None:
        log.cl_debug("no [%s] is configured for user [%s] of file system [%s], "
                     "use default value [%s]",
                     cstr.CSTR_THROTTLED_OSS_RPC_RATE,
                     uid,
                     lustre_fs.lf_fsname,
                     default_throttled_oss_rpc_rate)
        throttled_oss_rpc_rate = default_throttled_oss_rpc_rate

    throttled_mds_rpc_rate = utils.config_value(qos_user_config,
                                                cstr.CSTR_THROTTLED_MDS_RPC_RATE)
    if throttled_mds_rpc_rate is None:
        log.cl_debug("no [%s] is configured for user [%s] of file system [%s], "
                     "use default value [%s]",
                     cstr.CSTR_THROTTLED_MDS_RPC_RATE,
                     uid,
                     lustre_fs.lf_fsname,
                     default_throttled_oss_rpc_rate)
        throttled_mds_rpc_rate = default_throttled_mds_rpc_rate

    qos_user = clownfish_qos.ClownfishDecayQoSUser(uid, interval,
                                                   mbps_threshold,
                                                   throttled_oss_rpc_rate,
                                                   iops_threshold,
                                                   throttled_mds_rpc_rate)
    qos_users[uid] = qos_user
    return 0


def parse_qos_config(log, lustre_fs, lustre_config, config_fpath, workspace):
    """
    Parse the config for QoS
    """
    # pylint: disable=too-many-locals,too-many-branches
    qos_config = utils.config_value(lustre_config, cstr.CSTR_QOS)
    if qos_config is None:
        log.cl_info("no [%s] is configured for file system [%s], no QoS "
                    "control for that file system",
                    cstr.CSTR_QOS, lustre_fs.lf_fsname)
        return 0, None

    esmon_server_hostname = utils.config_value(qos_config,
                                               cstr.CSTR_ESMON_SERVER_HOSTNAME)
    if esmon_server_hostname is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_ESMON_SERVER_HOSTNAME,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_enabled = utils.config_value(qos_config, cstr.CSTR_ENABLED)
    if qos_enabled is None:
        log.cl_error("no [%s] is configured for for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_ENABLED,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_interval = utils.config_value(qos_config,
                                      cstr.CSTR_INTERVAL)
    if qos_interval is None:
        log.cl_error("no [%s] is configured for for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_INTERVAL,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_iops_threshold = utils.config_value(qos_config,
                                            cstr.CSTR_IOPS_THRESHOLD)
    if qos_iops_threshold is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_IOPS_THRESHOLD, lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_mbps_threshold = utils.config_value(qos_config,
                                            cstr.CSTR_MBPS_THRESHOLD)
    if qos_mbps_threshold is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_MBPS_THRESHOLD, lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_throttled_oss_rpc_rate = utils.config_value(qos_config,
                                                    cstr.CSTR_THROTTLED_OSS_RPC_RATE)
    if qos_throttled_oss_rpc_rate is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_THROTTLED_OSS_RPC_RATE,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_throttled_mds_rpc_rate = utils.config_value(qos_config,
                                                    cstr.CSTR_THROTTLED_MDS_RPC_RATE)
    if qos_throttled_mds_rpc_rate is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_THROTTLED_MDS_RPC_RATE,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos_users = {}
    qos_user_configs = utils.config_value(qos_config,
                                          cstr.CSTR_USERS)
    if qos_user_configs is None:
        log.cl_info("no [%s] is configured for QoS of file system [%s]",
                    cstr.CSTR_USERS,
                    lustre_fs.lf_fsname)
        qos_user_configs = []

    for qos_user_config in qos_user_configs:
        ret = parse_qos_user_config(log, lustre_fs, qos_user_config,
                                    config_fpath,
                                    qos_users, qos_interval,
                                    qos_mbps_threshold,
                                    qos_iops_threshold,
                                    qos_throttled_oss_rpc_rate,
                                    qos_throttled_mds_rpc_rate)
        if ret:
            return -1, None
    esmon_collect_interval = utils.config_value(qos_config,
                                                cstr.CSTR_ESMON_COLLECT_INTERVAL)
    if esmon_collect_interval is None:
        log.cl_error("no [%s] is configured for QoS of file system [%s], "
                     "please correct file [%s]",
                     cstr.CSTR_ESMON_COLLECT_INTERVAL,
                     lustre_fs.lf_fsname,
                     config_fpath)
        return -1, None

    qos = clownfish_qos.ClownfishDecayQoS(log, lustre_fs,
                                          esmon_server_hostname,
                                          qos_interval,
                                          qos_mbps_threshold,
                                          qos_throttled_oss_rpc_rate,
                                          qos_iops_threshold,
                                          qos_throttled_mds_rpc_rate,
                                          esmon_collect_interval,
                                          qos_users, qos_enabled, workspace)
    return 0, qos


def init_instance(log, workspace, config, config_fpath, no_operation=False):
    """
    Parse the config and init the instance
    """
    # pylint: disable=too-many-locals,too-many-return-statements
    # pylint: disable=too-many-branches,too-many-statements
    lazy_prepare = utils.config_value(config, cstr.CSTR_LAZY_PREPARE)
    if lazy_prepare is None:
        lazy_prepare = False
        log.cl_info("no [%s] is configured, using default value false",
                    cstr.CSTR_LAZY_PREPARE)

    if lazy_prepare:
        lazy_prepare_string = "enabled"
    else:
        lazy_prepare_string = "disabled"
    log.cl_info("lazy prepare is %s", lazy_prepare_string)

    dist_configs = utils.config_value(config, cstr.CSTR_LUSTRE_DISTRIBUTIONS)
    if dist_configs is None:
        log.cl_error("can NOT find [%s] in the config file, "
                     "please correct file [%s]",
                     cstr.CSTR_LUSTRE_DISTRIBUTIONS, config_fpath)
        return None

    # Keys are the distribution IDs, values are LustreRPMs
    lustre_distributions = {}
    for dist_config in dist_configs:
        lustre_distribution_id = utils.config_value(dist_config,
                                                    cstr.CSTR_LUSTRE_DISTRIBUTION_ID)
        if lustre_distribution_id is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_LUSTRE_DISTRIBUTION_ID, config_fpath)
            return None

        if lustre_distribution_id in lustre_distributions:
            log.cl_error("multiple distributions with ID [%s] is "
                         "configured, please correct file [%s]",
                         lustre_distribution_id, config_fpath)
            return None

        lustre_rpm_dir = utils.config_value(dist_config,
                                            cstr.CSTR_LUSTRE_RPM_DIR)
        if lustre_rpm_dir is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_LUSTRE_RPM_DIR, config_fpath)
            return None

        lustre_rpm_dir = lustre_rpm_dir.rstrip("/")

        e2fsprogs_rpm_dir = utils.config_value(dist_config,
                                               cstr.CSTR_E2FSPROGS_RPM_DIR)
        if e2fsprogs_rpm_dir is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_E2FSPROGS_RPM_DIR, config_fpath)
            return None

        e2fsprogs_rpm_dir = e2fsprogs_rpm_dir.rstrip("/")

        lustre_rpms = lustre.LustreRPMs(lustre_distribution_id,
                                        lustre_rpm_dir, e2fsprogs_rpm_dir)
        ret = lustre_rpms.lr_prepare(log)
        if ret:
            log.cl_error("failed to prepare Lustre RPMs")
            return None

        lustre_distributions[lustre_distribution_id] = lustre_rpms

    iso_path = utils.config_value(config, cstr.CSTR_ISO_PATH)
    if iso_path is None:
        log.cl_info("no [%s] in the config file", cstr.CSTR_ISO_PATH)
    elif not os.path.exists(iso_path):
        log.cl_error("ISO file [%s] doesn't exist", iso_path)
        return None

    ssh_host_configs = utils.config_value(config, cstr.CSTR_SSH_HOSTS)
    if ssh_host_configs is None:
        log.cl_error("can NOT find [%s] in the config file, "
                     "please correct file [%s]",
                     cstr.CSTR_SSH_HOSTS, config_fpath)
        return None

    hosts = {}
    for host_config in ssh_host_configs:
        host_id = utils.config_value(host_config,
                                     cstr.CSTR_HOST_ID)
        if host_id is None:
            log.cl_error("can NOT find [%s] in the config of a "
                         "SSH host, please correct file [%s]",
                         cstr.CSTR_HOST_ID, config_fpath)
            return None

        if host_id in hosts:
            log.cl_error("multiple SSH hosts with the same ID [%s], please "
                         "correct file [%s]", host_id, config_fpath)
            return None

        lustre_distribution_id = utils.config_value(host_config,
                                                    cstr.CSTR_LUSTRE_DISTRIBUTION_ID)
        if lustre_distribution_id is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_LUSTRE_DISTRIBUTION_ID, config_fpath)
            return None

        if lustre_distribution_id not in lustre_distributions:
            log.cl_error("no Lustre distributions with ID [%s] is "
                         "configured, please correct file [%s]",
                         lustre_distribution_id, config_fpath)
            return None

        lustre_distribution = lustre_distributions[lustre_distribution_id]

        hostname = utils.config_value(host_config, cstr.CSTR_HOSTNAME)
        if hostname is None:
            log.cl_error("can NOT find [%s] in the config of SSH host "
                         "with ID [%s], please correct file [%s]",
                         cstr.CSTR_HOSTNAME, host_id, config_fpath)
            return None

        ssh_identity_file = utils.config_value(host_config, cstr.CSTR_SSH_IDENTITY_FILE)

        host = lustre.LustreServerHost(hostname,
                                       lustre_rpms=lustre_distribution,
                                       identity_file=ssh_identity_file,
                                       host_id=host_id)
        hosts[host_id] = host

    lustre_configs = utils.config_value(config, cstr.CSTR_LUSTRES)
    if lustre_configs is None:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_LUSTRES, config_fpath)
        return None

    mgs_configs = utils.config_value(config, cstr.CSTR_MGS_LIST)
    if mgs_configs is None:
        log.cl_debug("no [%s] is configured", cstr.CSTR_MGS_LIST)
        mgs_configs = []

    server_hosts = {}
    mgs_dict = {}
    for mgs_config in mgs_configs:
        # Parse MGS configs
        mgs_id = utils.config_value(mgs_config, cstr.CSTR_MGS_ID)
        if mgs_id is None:
            log.cl_error("no [%s] is configured for a MGS, please correct "
                         "file [%s]",
                         cstr.CSTR_MGS_ID, config_fpath)
            return None

        if mgs_id in mgs_dict:
            log.cl_error("multiple configurations for MGS [%s], please "
                         "correct file [%s]",
                         mgs_id, config_fpath)
            return None

        backfstype = utils.config_value(mgs_config, cstr.CSTR_BACKFSTYPE)
        if backfstype is None:
            log.cl_debug("no [%s] is configured for MGS [%s], using [%s] as "
                         "default value", cstr.CSTR_BACKFSTYPE, mgs_id,
                         lustre.BACKFSTYPE_LDISKFS)
            backfstype = lustre.BACKFSTYPE_LDISKFS

        mgs = lustre.LustreMGS(log, mgs_id, backfstype)
        mgs_dict[mgs_id] = mgs

        instance_configs = utils.config_value(mgs_config, cstr.CSTR_INSTANCES)
        if instance_configs is None:
            log.cl_error("no [%s] is configured for MGS [%s], please correct "
                         "file [%s]",
                         cstr.CSTR_INSTANCES, mgs_id, config_fpath)
            return None

        for instance_config in instance_configs:
            host_id = utils.config_value(instance_config, cstr.CSTR_HOST_ID)
            if host_id is None:
                log.cl_error("no [%s] is configured for instance of MGS "
                             "[%s], please correct file [%s]",
                             cstr.CSTR_HOST_ID, mgs_id, config_fpath)
                return None

            if host_id not in hosts:
                log.cl_error("no host with [%s] is configured in hosts, "
                             "please correct file [%s]",
                             host_id, config_fpath)
                return None

            device = utils.config_value(instance_config, cstr.CSTR_DEVICE)
            if device is None:
                log.cl_error("no [%s] is configured for instance of "
                             "MGS [%s], please correct file [%s]",
                             cstr.CSTR_DEVICE, mgs_id, config_fpath)
                return None

            if backfstype == lustre.BACKFSTYPE_ZFS:
                if device.startswith("/"):
                    log.cl_error("device [%s] with absolute path is "
                                 "configured for instance of MGS [%s] "
                                 "with ZFS type, please correct file [%s]",
                                 cstr.CSTR_DEVICE, mgs_id, config_fpath)
                    return None
            else:
                if not device.startswith("/"):
                    log.cl_error("device [%s] with absolute path should be "
                                 "configured for instance of MGS [%s] with "
                                 "ldiskfs type, please correct file [%s]",
                                 cstr.CSTR_DEVICE, mgs_id, config_fpath)
                    return None

            nid = utils.config_value(instance_config, cstr.CSTR_NID)
            if nid is None:
                log.cl_error("no [%s] is configured for instance of "
                             "MGS [%s], please correct file [%s]",
                             cstr.CSTR_NID, mgs_id, config_fpath)
                return None

            zpool_create = None
            if backfstype == lustre.BACKFSTYPE_ZFS:
                zpool_create = utils.config_value(instance_config,
                                                  cstr.CSTR_ZPOOL_CREATE)
                if zpool_create is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "MGS [%s], please correct file [%s]",
                                 cstr.CSTR_ZPOOL_CREATE, mgs_id, config_fpath)
                    return None

            lustre_host = hosts[host_id]
            if host_id not in server_hosts:
                server_hosts[host_id] = lustre_host

            mnt = "/mnt/mgs_%s" % (mgs_id)
            lustre.LustreMGSInstance(log, mgs, lustre_host, device, mnt,
                                     nid, add_to_host=True)

    lustres = {}
    qos_dict = {}
    for lustre_config in lustre_configs:
        # Parse general configs of Lustre file system
        fsname = utils.config_value(lustre_config, cstr.CSTR_FSNAME)
        if fsname is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_FSNAME, config_fpath)
            return None

        if fsname in lustres:
            log.cl_error("file system [%s] is configured for multiple times, "
                         "please correct file [%s]",
                         fsname, config_fpath)
            return None

        lustre_fs = lustre.LustreFilesystem(fsname)
        lustres[fsname] = lustre_fs

        mgs_configured = False

        # Parse MGS config
        mgs_id = utils.config_value(lustre_config, cstr.CSTR_MGS_ID)
        if mgs_id is not None:
            log.cl_debug("[%s] is configured for file system [%s]",
                         cstr.CSTR_MGS_ID, fsname)

            if mgs_id not in mgs_dict:
                log.cl_error("no MGS with ID [%s] is configured, please "
                             "correct file [%s]",
                             mgs_id, config_fpath)
                return None

            mgs = mgs_dict[mgs_id]

            ret = mgs.lmgs_add_fs(log, lustre_fs)
            if ret:
                log.cl_error("failed to add file system [%s] to MGS [%s], "
                             "please correct file [%s]",
                             fsname, mgs_id, config_fpath)
                return None

            mgs_configured = True

        # Parse MDT configs
        mdt_configs = utils.config_value(lustre_config, cstr.CSTR_MDTS)
        if mdt_configs is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_MDTS, config_fpath)
            return None

        for mdt_config in mdt_configs:
            mdt_index = utils.config_value(mdt_config, cstr.CSTR_INDEX)
            if mdt_index is None:
                log.cl_error("no [%s] is configured for a MDT of file system "
                             "[%s], please correct file [%s]",
                             cstr.CSTR_INDEX, fsname, config_fpath)
                return None

            is_mgs = utils.config_value(mdt_config, cstr.CSTR_IS_MGS)
            if is_mgs is None:
                log.cl_error("no [%s] is configured for MDT with index [%s] "
                             "of file system [%s], using default value [False]",
                             cstr.CSTR_IS_MGS, mdt_index, fsname)
                is_mgs = False

            if is_mgs:
                if mgs_configured:
                    log.cl_error("multiple MGS are configured for file "
                                 "system [%s], please correct file [%s]",
                                 fsname, config_fpath)
                    return None
                mgs_configured = True

            backfstype = utils.config_value(mdt_config, cstr.CSTR_BACKFSTYPE)
            if backfstype is None:
                log.cl_debug("no [%s] is configured for MDT with index [%s] "
                             "of file system [%s], using [%s] as the default "
                             "value", cstr.CSTR_BACKFSTYPE, mdt_index, fsname,
                             lustre.BACKFSTYPE_LDISKFS)
                backfstype = lustre.BACKFSTYPE_LDISKFS

            mdt = lustre.LustreMDT(log, lustre_fs, mdt_index, backfstype,
                                   is_mgs=is_mgs)

            instance_configs = utils.config_value(mdt_config, cstr.CSTR_INSTANCES)
            if instance_configs is None:
                log.cl_error("no [%s] is configured, please correct file [%s]",
                             cstr.CSTR_INSTANCES, config_fpath)
                return None

            for instance_config in instance_configs:
                host_id = utils.config_value(instance_config, cstr.CSTR_HOST_ID)
                if host_id is None:
                    log.cl_error("no [%s] is configured, please correct file [%s]",
                                 cstr.CSTR_HOST_ID, config_fpath)
                    return None

                if host_id not in hosts:
                    log.cl_error("no host with [%s] is configured in hosts, "
                                 "please correct file [%s]",
                                 host_id, config_fpath)
                    return None

                device = utils.config_value(instance_config, cstr.CSTR_DEVICE)
                if device is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "MDT with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 cstr.CSTR_DEVICE, mdt_index, fsname,
                                 config_fpath)
                    return None

                if backfstype == lustre.BACKFSTYPE_ZFS:
                    if device.startswith("/"):
                        log.cl_error("device [%s] with absolute path is "
                                     "configured for an instance of MDT "
                                     "with index [%s] of file system [%s] "
                                     "with ZFS type, please correct file [%s]",
                                     cstr.CSTR_DEVICE, mdt_index, fsname,
                                     config_fpath)
                        return None
                else:
                    if not device.startswith("/"):
                        log.cl_error("device [%s] with absolute path is "
                                     "configured for an instance of MDT "
                                     "with index [%s] of file system [%s] "
                                     "with ldiskfs type, please correct file "
                                     "[%s]",
                                     cstr.CSTR_DEVICE, mdt_index, fsname,
                                     config_fpath)
                        return None

                nid = utils.config_value(instance_config, cstr.CSTR_NID)
                if nid is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "MDT with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 cstr.CSTR_NID, mdt_index, fsname,
                                 config_fpath)
                    return None

                zpool_create = None
                if backfstype == lustre.BACKFSTYPE_ZFS:
                    zpool_create = utils.config_value(instance_config,
                                                      cstr.CSTR_ZPOOL_CREATE)
                    if zpool_create is None:
                        log.cl_error("no [%s] is configured for an instance of "
                                     "MDT with index [%s] of file system [%s], "
                                     "please correct file [%s]",
                                     cstr.CSTR_ZPOOL_CREATE, mdt_index, fsname,
                                     config_fpath)
                        return None

                lustre_host = hosts[host_id]
                if host_id not in server_hosts:
                    server_hosts[host_id] = lustre_host

                mnt = "/mnt/%s_mdt_%s" % (fsname, mdt_index)
                lustre.LustreMDTInstance(log, mdt, lustre_host, device, mnt,
                                         nid, add_to_host=True,
                                         zpool_create=zpool_create)

        if not mgs_configured:
            log.cl_error("None MGS is configured, please correct file [%s]",
                         config_fpath)
            return None

        # Parse OST configs
        ost_configs = utils.config_value(lustre_config, cstr.CSTR_OSTS)
        if ost_configs is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_OSTS, config_fpath)
            return None

        for ost_config in ost_configs:
            ost_index = utils.config_value(ost_config, cstr.CSTR_INDEX)
            if ost_index is None:
                log.cl_error("no [%s] is configured, please correct file [%s]",
                             cstr.CSTR_INDEX, config_fpath)
                return None

            backfstype = utils.config_value(ost_config, cstr.CSTR_BACKFSTYPE)
            if backfstype is None:
                log.cl_debug("no [%s] is configured for OST with index [%s] "
                             "of file system [%s], using [%s] as default",
                             cstr.CSTR_BACKFSTYPE, ost_index, fsname,
                             lustre.BACKFSTYPE_LDISKFS)
                backfstype = lustre.BACKFSTYPE_LDISKFS

            ost = lustre.LustreOST(log, lustre_fs, ost_index, backfstype)

            instance_configs = utils.config_value(ost_config, cstr.CSTR_INSTANCES)
            if instance_configs is None:
                log.cl_error("no [%s] is configured for OST with index [%s] "
                             "of file system [%s], please correct file [%s]",
                             cstr.CSTR_INSTANCES, ost_index, fsname,
                             config_fpath)
                return None

            for instance_config in instance_configs:
                host_id = utils.config_value(instance_config, cstr.CSTR_HOST_ID)
                if host_id is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "OST with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 cstr.CSTR_HOST_ID, ost_index, fsname,
                                 config_fpath)
                    return None

                if host_id not in hosts:
                    log.cl_error("no host with ID [%s] is configured in hosts, "
                                 "please correct file [%s]",
                                 host_id, config_fpath)
                    return None

                device = utils.config_value(instance_config, cstr.CSTR_DEVICE)
                if device is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "OST with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 cstr.CSTR_DEVICE, ost_index, fsname,
                                 config_fpath)
                    return None

                if backfstype == lustre.BACKFSTYPE_ZFS:
                    if device.startswith("/"):
                        log.cl_error("device [%s] with absolute path is "
                                     "configured for an instance of OST "
                                     "with index [%s] of file system [%s] "
                                     "with ZFS type, please correct file [%s]",
                                     cstr.CSTR_DEVICE, ost_index, fsname,
                                     config_fpath)
                        return None
                else:
                    if not device.startswith("/"):
                        log.cl_error("device [%s] with none-absolute path is "
                                     "configured for an instance of OST "
                                     "with index [%s] of file system [%s] "
                                     "with ldiskfs type, please correct file "
                                     "[%s]",
                                     cstr.CSTR_DEVICE, ost_index, fsname,
                                     config_fpath)
                        return None

                nid = utils.config_value(instance_config, cstr.CSTR_NID)
                if nid is None:
                    log.cl_error("no [%s] is configured for an instance of "
                                 "OST with index [%s] of file system [%s], "
                                 "please correct file [%s]",
                                 cstr.CSTR_NID, ost_index, fsname,
                                 config_fpath)
                    return None

                zpool_create = None
                if backfstype == lustre.BACKFSTYPE_ZFS:
                    zpool_create = utils.config_value(instance_config, cstr.CSTR_ZPOOL_CREATE)
                    if zpool_create is None:
                        log.cl_error("no [%s] is configured for an instance of "
                                     "OST with index [%s] of file system [%s], "
                                     "please correct file [%s]",
                                     cstr.CSTR_ZPOOL_CREATE, mdt_index, fsname,
                                     config_fpath)
                        return None

                lustre_host = hosts[host_id]
                if host_id not in server_hosts:
                    server_hosts[host_id] = lustre_host

                mnt = "/mnt/%s_ost_%s" % (fsname, ost_index)
                lustre.LustreOSTInstance(log, ost, lustre_host, device, mnt,
                                         nid, add_to_host=True,
                                         zpool_create=zpool_create)
        # Parse client configs
        client_configs = utils.config_value(lustre_config,
                                            cstr.CSTR_CLIENTS)
        if client_configs is None:
            log.cl_error("no [%s] is configured, please correct file [%s]",
                         cstr.CSTR_CLIENTS, config_fpath)
            return None

        for client_config in client_configs:
            host_id = utils.config_value(client_config, cstr.CSTR_HOST_ID)
            if host_id is None:
                log.cl_error("no [%s] is configured, please correct file [%s]",
                             cstr.CSTR_HOST_ID, config_fpath)
                return None

            if host_id not in hosts:
                log.cl_error("no host with [%s] is configured in hosts, "
                             "please correct file [%s]",
                             host_id, config_fpath)
                return None

            lustre_host = hosts[host_id]

            mnt = utils.config_value(client_config, cstr.CSTR_MNT)
            if mnt is None:
                log.cl_error("no [%s] is configured, please correct file [%s]",
                             cstr.CSTR_MNT, config_fpath)
                return None

            lustre.LustreClient(log, lustre_fs, lustre_host, mnt, add_to_host=True)

        # No operation means this instance should not do any operation.
        # QoS won't be used.
        if no_operation:
            continue

        ret, qos = parse_qos_config(log, lustre_fs, lustre_config,
                                    config_fpath, workspace)
        if ret:
            log.cl_error("failed to parse QoS for file system [%s]",
                         lustre_fs.lf_fsname)
            return None

        if qos is None:
            continue
        qos_dict[lustre_fs.lf_fsname] = qos

    high_availability = utils.config_value(config,
                                           cstr.CSTR_HIGH_AVAILABILITY)
    ha_native = False
    bindnetaddr = None
    if high_availability is None:
        ha_enabled = False
        log.cl_info("no [%s] is configured, disabling high availability",
                    cstr.CSTR_HIGH_AVAILABILITY)
    elif not isinstance(high_availability, dict):
        log.cl_error("high availability section is configured in the wrong way")
        return None
    else:
        ha_enabled = utils.config_value(high_availability, cstr.CSTR_ENABLED)
        if ha_enabled is None:
            ha_enabled = False
            log.cl_info("no [%s] is configured in high availability section, "
                        "disabling high availability",
                        cstr.CSTR_ENABLED)

        if ha_enabled:
            ha_native = utils.config_value(high_availability, cstr.CSTR_NATIVE)
            if ha_native is None:
                ha_native = False

            if not ha_native:
                bindnetaddr = utils.config_value(high_availability,
                                                 cstr.CSTR_BINDNETADDR)
                if bindnetaddr is None:
                    log.cl_error("no [%s] is configured in high availability section",
                                 cstr.CSTR_BINDNETADDR)
                    return None

    if ha_enabled:
        if ha_native:
            log.cl_info("native high availability is enabled")
        else:
            log.cl_info("high availability based on corosync enabled")
    else:
        log.cl_info("high availability is disabled")

    local_host = ssh_host.SSHHost("localhost", local=True)
    mnt_path = "/mnt/" + utils.random_word(8)

    command = ("mkdir -p %s && mount -o loop %s %s" %
               (mnt_path, iso_path, mnt_path))
    retval = local_host.sh_run(log, command)
    if retval.cr_exit_status:
        log.cl_error("failed to run command [%s] on host [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     local_host.sh_hostname,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return None

    corosync_cluster = None
    if ha_enabled and not ha_native:
        corosync_cluster = corosync.LustreCorosyncCluster(mgs_dict, lustres,
                                                          bindnetaddr,
                                                          workspace, mnt_path)

    return ClownfishInstance(log, workspace, lazy_prepare, hosts, mgs_dict,
                             lustres, ha_native, corosync_cluster, qos_dict,
                             iso_path, local_host, mnt_path, no_operation=no_operation)
