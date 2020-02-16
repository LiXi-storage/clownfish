# Copyright (c) 2020 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for Corosync
"""
from pylcommon import install_common
from pylcommon import constants


CLOWNFISH_COROSYNC_FNAME = "corosync.conf"
CLOWNFISH_AUTHKEY_FNAME = "authkey"
COROSYNC_CONFIG_DIR = "/etc/corosync/"
CLOWNFISH_COROSYNC_CONFIG = COROSYNC_CONFIG_DIR + CLOWNFISH_COROSYNC_FNAME
CLOWNFISH_COROSYNC_AUTHKEY = COROSYNC_CONFIG_DIR + CLOWNFISH_AUTHKEY_FNAME
CLOWNFISH_RESOURCE_PREFIX = "clf_"


class LustreCorosyncCluster(install_common.InstallationCluster):
    # pylint: disable=too-few-public-methods,too-many-arguments
    """
    Lustre HA cluster config.
    """
    def __init__(self, mgs_dict, lustres, bindnetaddr, workspace, mnt_path, iso_path):
        # Key is mgs_id, value is LustreMGS
        self.lcc_mgs_dict = mgs_dict
        # Key is fsname, value is LustreFilesystem
        self.lcc_lustres = lustres
        self.lcc_iso_path = iso_path
        self.lcc_bindnetaddr = bindnetaddr
        self.lcc_corosync_config = ("""
totem {
    version: 2
    interface {
        ringnumber: 0
        bindnetaddr: %s
        mcastaddr: 226.94.1.2
        mcastport: 5405
        ttl: 1
    }
}
service {
    ver:  0
    name: pacemaker
}
logging {
    to_logfile: yes
    logfile: /var/log/cluster/corosync.log
    to_syslog: yes
    logger_subsys {
        subsys: QUORUM
        debug: off
    }
}
aisexec {
    user: root
    group: root
}
quorum {
    provider: corosync_votequorum
}
""" % (bindnetaddr))
        nodelist_string = "nodelist {"

        # Key is hostname, value is host
        self.lcc_hosts = {}
        for lustrefs in lustres.values():
            services = lustrefs.lf_services()
            for service in services:
                for instance in service.ls_instances.values():
                    host = instance.lsi_host
                    if host.sh_hostname not in self.lcc_hosts:
                        self.lcc_hosts[host.sh_hostname] = host

        for mgs in mgs_dict.values():
            for instance in mgs.ls_instances.values():
                host = instance.lsi_host
                if host.sh_hostname not in self.lcc_hosts:
                    self.lcc_hosts[host.sh_hostname] = host

        for hostname in self.lcc_hosts.iterkeys():
            nodelist_string += ("""
    node {
        ring0_addr: %s
    }""" % (hostname))
        nodelist_string += """
}"""
        super(LustreCorosyncCluster, self).__init__(workspace,
                                                    self.lcc_hosts.values(),
                                                    mnt_path, iso_path)
        self.lcc_corosync_config += nodelist_string

    def lcc_cleanup(self, log):
        """
        Cleanup the whole cluster
        """
        for host in self.lcc_hosts.itervalues():
            log.cl_info("destroying corosync cluster on host [%s]",
                        host.sh_hostname)
            command = "pcs cluster destroy"
            retval = host.sh_run(log, command, timeout=60)
            if retval.cr_exit_status != 0:
                # Stop might fail, kill -9 by force
                log.cl_info("failed to run command [%s] on host "
                            "[%s], ret = [%d], stdout = [%s], stderr = "
                            "[%s], trying to kill it by force",
                            command,
                            host.sh_hostname,
                            retval.cr_exit_status,
                            retval.cr_stdout,
                            retval.cr_stderr)

                command = "killall -9 corosync"
                retval = host.sh_run(log, command)

                command = "pcs cluster destroy"
                retval = host.sh_run(log, command)
                if retval.cr_exit_status != 0:
                    log.cl_info("failed to run command [%s] on host "
                                "[%s], ret = [%d], stdout = [%s], stderr = "
                                "[%s], igoring",
                                command,
                                host.sh_hostname,
                                retval.cr_exit_status,
                                retval.cr_stdout,
                                retval.cr_stderr)
        return 0

    def lcc_config(self, log, workspace):
        """
        Configure corosync and pacemaker, and add target resource
        """
        # pylint: disable=too-many-branches
        # edit corosync.conf and sync to all ha hosts
        corosync_config_fpath = workspace + "/" + CLOWNFISH_COROSYNC_FNAME
        corosync_config_fd = open(corosync_config_fpath, 'w')
        if not corosync_config_fd:
            log.cl_error("failed to open file [%s] on localhost",
                         corosync_config_fpath)
            return -1
        corosync_config_fd.write(self.lcc_corosync_config)
        corosync_config_fd.close()

        # Generate corosync authkey on host 0
        host_first = self.ic_hosts[0]
        command = "/usr/sbin/corosync-keygen --less-secure"
        retval = host_first.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to start run command [%s] on host "
                         "[%s], ret = [%d], stdout = [%s], stderr = "
                         "[%s]",
                         command,
                         host_first.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        # sync corosync.conf and authkey to all ha hosts
        for host in self.lcc_hosts.itervalues():
            ret = host.sh_send_file(log, constants.CLOWNFISH_CONFIG,
                                    constants.CLOWNFISH_CONFIG)
            if ret:
                log.cl_error("failed to send file [%s] on local host to "
                             "file [%s] on host [%s]",
                             constants.CLOWNFISH_CONFIG,
                             constants.CLOWNFISH_CONFIG,
                             host.sh_hostname)
                return ret

            ret = host.sh_send_file(log, corosync_config_fpath,
                                    CLOWNFISH_COROSYNC_CONFIG)
            if ret:
                log.cl_error("failed to send file [%s] on local host to "
                             "file [%s] on host [%s]",
                             corosync_config_fpath,
                             CLOWNFISH_COROSYNC_CONFIG,
                             host.sh_hostname)
                return ret

            if host != host_first:
                ret = host_first.sh_send_file(log, CLOWNFISH_COROSYNC_AUTHKEY,
                                              CLOWNFISH_COROSYNC_AUTHKEY,
                                              from_local=False,
                                              remote_host=host)
                if ret:
                    log.cl_error("failed to send file [%s] on host [%s] to "
                                 "file [%s] on host [%s]",
                                 CLOWNFISH_COROSYNC_AUTHKEY,
                                 host_first.sh_hostname,
                                 CLOWNFISH_COROSYNC_AUTHKEY,
                                 host.sh_hostname)
                    return ret

            log.cl_info("configuring autostart of corosync and pacemaker on "
                        "host [%s]", host.sh_hostname)
            command = "systemctl enable corosync pacemaker"
            retval = host.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host [%s], "
                             "ret = [%d], stdout = [%s], stderr = [%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        return 0

    def _ccl_resource_limit_hosts(self, log, pcs_host, resource_name,
                                  lustre_service):
        """
        Limit the resource to run on some hosts
        """
        disable_hostnames = self.lcc_hosts.keys()
        for instance in lustre_service.ls_instances.itervalues():
            host = instance.lsi_host
            hostname = host.sh_hostname
            if hostname in disable_hostnames:
                disable_hostnames.remove(hostname)
        for hostname in disable_hostnames:
            command = ("pcs constraint location %s prefers %s=-INFINITY" %
                       (resource_name, hostname))
            retval = pcs_host.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host "
                             "[%s], ret = [%d], stdout = [%s], stderr = "
                             "[%s]",
                             command,
                             pcs_host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1
        return 0

    def _ccl_create_mdt_resource(self, log, host, mdt, use_template=True):
        """
        Create resource for Lustre MDT
        """
        service_name = mdt.ls_service_name
        lustrefs = mdt.ls_lustre_fs
        fsname = lustrefs.lf_fsname
        template_name = CLOWNFISH_RESOURCE_PREFIX + fsname + "_MDT"
        resource_name = CLOWNFISH_RESOURCE_PREFIX + service_name

        if use_template:
            type_string = "@" + template_name
        else:
            type_string = "ocf:clownfish:lustre_server.sh"
        command = ("crm configure primitive %s %s params service=%s" %
                   (resource_name, type_string, service_name))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host "
                         "[%s], ret = [%d], stdout = [%s], stderr = "
                         "[%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        retval = self._ccl_resource_limit_hosts(log, host, resource_name,
                                                mdt)
        if retval:
            log.cl_error("failed to disable resource [%s] location of hosts for service [%s]",
                         resource_name, mdt.ls_service_name)
            return -1
        return 0

    def _ccl_create_mdt_template(self, log, host, fsname):
        """
        Create template for Lustre MDT
        """
        # pylint: disable=no-self-use
        template_name = CLOWNFISH_RESOURCE_PREFIX + fsname + "_MDT"
        command = ("crm configure rsc_template %s ocf:clownfish:lustre_server.sh" %
                   (template_name))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host "
                         "[%s], ret = [%d], stdout = [%s], stderr = "
                         "[%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def _ccl_create_ost_resource(self, log, host, ost):
        """
        Create resource for Lustre OST
        """
        service_name = ost.ls_service_name
        lustrefs = ost.ls_lustre_fs
        fsname = lustrefs.lf_fsname
        template_name = CLOWNFISH_RESOURCE_PREFIX + fsname + "_OST"
        resource_name = CLOWNFISH_RESOURCE_PREFIX + service_name

        type_string = "@" + template_name
        command = ("crm configure primitive %s %s params service=%s" %
                   (resource_name, type_string, service_name))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host "
                         "[%s], ret = [%d], stdout = [%s], stderr = "
                         "[%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        retval = self._ccl_resource_limit_hosts(log, host, resource_name,
                                                ost)
        if retval:
            log.cl_error("failed to disable resource [%s] location of hosts for service [%s]",
                         resource_name, ost.ls_service_name)
            return -1
        return 0

    def _ccl_create_ost_template(self, log, host, fsname):
        """
        Create template for Lustre OST
        """
        # pylint: disable=no-self-use
        template_name = CLOWNFISH_RESOURCE_PREFIX + fsname + "_OST"
        command = ("crm configure rsc_template %s ocf:clownfish:lustre_server.sh" %
                   (template_name))
        retval = host.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host "
                         "[%s], ret = [%d], stdout = [%s], stderr = "
                         "[%s]",
                         command,
                         host.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1
        return 0

    def ccl_start(self, log):
        """
        Config and create Lustre resource.
        """
        # pylint: disable=too-many-branches,too-many-locals,too-many-statements
        # stop corosync, stopping might fail
        for host in self.lcc_hosts.itervalues():
            command = "systemctl stop corosync"
            retval = host.sh_run(log, command, timeout=60)
            if retval.cr_exit_status != 0:
                # Stop might fail, kill -9 by force
                log.cl_info("failed to run command [%s] on host "
                            "[%s], ret = [%d], stdout = [%s], stderr = "
                            "[%s], trying to kill it by force",
                            command,
                            host.sh_hostname,
                            retval.cr_exit_status,
                            retval.cr_stdout,
                            retval.cr_stderr)

                command = "killall -9 corosync"
                retval = host.sh_run(log, command)

                command = "systemctl stop corosync"
                retval = host.sh_run(log, command)
                if retval.cr_exit_status != 0:
                    log.cl_error("failed to run command [%s] on host "
                                 "[%s], ret = [%d], stdout = [%s], stderr = "
                                 "[%s]",
                                 command,
                                 host.sh_hostname,
                                 retval.cr_exit_status,
                                 retval.cr_stdout,
                                 retval.cr_stderr)
                    return -1

        # start pacemaker and corosync
        command = "systemctl start corosync pacemaker"
        for host in self.lcc_hosts.itervalues():
            retval = host.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host "
                             "[%s], ret = [%d], stdout = [%s], stderr = "
                             "[%s]",
                             command,
                             host.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        host0 = self.lcc_hosts.values()[0]
        ret = host0.sh_pcs_resources_clear(log)
        if ret:
            log.cl_error("failed to clear PCS resources on host [%s]",
                         host0.sh_hostname)
            return ret

        # Disable stonish otherwise resource won't start
        command = "pcs property set stonith-enabled=false"
        retval = host0.sh_run(log, command)
        if retval.cr_exit_status != 0:
            log.cl_error("failed to run command [%s] on host "
                         "[%s], ret = [%d], stdout = [%s], stderr = "
                         "[%s]",
                         command,
                         host0.sh_hostname,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
            return -1

        for mgs in self.lcc_mgs_dict.itervalues():
            mgs_id = mgs.ls_service_name
            resource_name = CLOWNFISH_RESOURCE_PREFIX + mgs_id
            command = ("pcs resource create %s ocf:clownfish:lustre_server.sh service=%s" %
                       (resource_name, mgs_id))
            retval = host0.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host "
                             "[%s], ret = [%d], stdout = [%s], stderr = "
                             "[%s]",
                             command,
                             host0.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

            retval = self._ccl_resource_limit_hosts(log, host0, resource_name,
                                                    mgs)
            if retval:
                log.cl_error("failed to disable resource [%s] location of hosts for service [%s]",
                             resource_name, mgs.ls_service_name)
                return -1

        for lustrefs in self.lcc_lustres.itervalues():
            fsname = lustrefs.lf_fsname
            have_mdt = True
            if lustrefs.lf_mgs is not None:
                mgs_id = lustrefs.lf_mgs.ls_service_name
                mgs_resource_name = CLOWNFISH_RESOURCE_PREFIX + mgs_id
            else:
                assert lustrefs.lf_mgs_mdt is not None
                mgs_mdt = lustrefs.lf_mgs_mdt
                mgs_mdt_id = mgs_mdt.ls_service_name
                ret = self._ccl_create_mdt_resource(log, host0, mgs_mdt,
                                                    use_template=False)
                if ret:
                    log.cl_error("failed to create Pacemaker resource for Lustre service [%s]",
                                 mgs_mdt_id)
                    return -1
                mgs_resource_name = CLOWNFISH_RESOURCE_PREFIX + mgs_mdt_id

                if len(lustrefs.lf_mdts) == 1:
                    have_mdt = False

            if have_mdt:
                ret = self._ccl_create_mdt_template(log, host, fsname)
                if ret:
                    log.cl_error("failed to create MDT template for Lustre file system [%s]",
                                 fsname)
                    return -1

                mdt_resource_string = "\("
                for mdt in lustrefs.lf_mdts.itervalues():
                    service_name = mdt.ls_service_name
                    if mdt.lmdt_is_mgs:
                        continue
                    ret = self._ccl_create_mdt_resource(log, host0, mdt,
                                                        use_template=True)
                    if ret:
                        log.cl_error("failed to create Pacemaker resource for Lustre service [%s]",
                                     service_name)
                        return -1
                    resource_name = CLOWNFISH_RESOURCE_PREFIX + service_name
                    mdt_resource_string += " " + resource_name + ":start"
                mdt_resource_string += " \)"

                order_id = CLOWNFISH_RESOURCE_PREFIX + fsname + "_mgs_before_mdt"
                command = ("crm configure order %s Optional: %s %s" %
                           (order_id, mgs_resource_name, mdt_resource_string))
                retval = host0.sh_run(log, command)
                if retval.cr_exit_status != 0:
                    log.cl_error("failed to run command [%s] on host "
                                 "[%s], ret = [%d], stdout = [%s], stderr = "
                                 "[%s]",
                                 command,
                                 host0.sh_hostname,
                                 retval.cr_exit_status,
                                 retval.cr_stdout,
                                 retval.cr_stderr)
                    return -1

            ret = self._ccl_create_ost_template(log, host, fsname)
            if ret:
                log.cl_error("failed to create OST template for Lustre file system [%s]",
                             fsname)
                return -1

            ost_resource_string = "\("
            for ost in lustrefs.lf_osts.itervalues():
                service_name = ost.ls_service_name
                ret = self._ccl_create_ost_resource(log, host0, ost)
                if ret:
                    log.cl_error("failed to create Pacemaker resource for Lustre service [%s]",
                                 service_name)
                    return -1
                resource_name = CLOWNFISH_RESOURCE_PREFIX + service_name
                ost_resource_string += " " + resource_name + ":start"
            ost_resource_string += " \)"

            if have_mdt:
                order_id = CLOWNFISH_RESOURCE_PREFIX + fsname + "_mdt_before_ost"
                command = ("crm configure order %s Optional: %s %s" %
                           (order_id, mdt_resource_string, ost_resource_string))
                retval = host0.sh_run(log, command)
                if retval.cr_exit_status != 0:
                    log.cl_error("failed to run command [%s] on host "
                                 "[%s], ret = [%d], stdout = [%s], stderr = "
                                 "[%s]",
                                 command,
                                 host0.sh_hostname,
                                 retval.cr_exit_status,
                                 retval.cr_stdout,
                                 retval.cr_stderr)
                    return -1

            order_id = CLOWNFISH_RESOURCE_PREFIX + fsname + "_mgs_before_ost"
            command = ("crm configure order %s Optional: %s %s" %
                       (order_id, mgs_resource_name, ost_resource_string))
            retval = host0.sh_run(log, command)
            if retval.cr_exit_status != 0:
                log.cl_error("failed to run command [%s] on host "
                             "[%s], ret = [%d], stdout = [%s], stderr = "
                             "[%s]",
                             command,
                             host0.sh_hostname,
                             retval.cr_exit_status,
                             retval.cr_stdout,
                             retval.cr_stderr)
                return -1

        log.cl_info("corosync and pacemaker is started in the cluster")

        return 0
