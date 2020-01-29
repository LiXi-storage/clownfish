# Copyright (c) 2020 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for Corosync
"""
from pylcommon import install_common


CLOWNFISH_COROSYNC_FNAME = "corosync.conf"
CLOWNFISH_AUTHKEY_FNAME = "authkey"
COROSYNC_CONFIG_DIR = "/etc/corosync/"
CLOWNFISH_COROSYNC_CONFIG = COROSYNC_CONFIG_DIR + CLOWNFISH_COROSYNC_FNAME
CLOWNFISH_COROSYNC_AUTHKEY = COROSYNC_CONFIG_DIR + CLOWNFISH_AUTHKEY_FNAME


class LustreCorosyncCluster(install_common.InstallationCluster):
    # pylint: disable=too-few-public-methods,too-many-arguments
    """
    Lustre HA cluster config.
    """
    def __init__(self, mgs_dict, lustres, bindnetaddr, workspace, mnt_path):
        self.lcc_mgs_dict = mgs_dict
        self.lcc_lustres = lustres
        self.lcc_bindnetaddr = bindnetaddr
        self.lcc_corosync_config = ("""
totem {
    version: 2
    interface {
        ringnumber: 0
        bindnetaddr: %s
        mcastaddr: 226.94.1.1
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
                                                    mnt_path)
        self.lcc_corosync_config += nodelist_string

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
        log.cl_info("corosync and pacemaker is properly configured in the cluster")
        return 0

    def ccl_start(self, log):
        """
        Config and create Lustre resource.
        """
        # start pacemaker and corosync
        command = "systemctl restart corosync pacemaker"
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
        return 0
