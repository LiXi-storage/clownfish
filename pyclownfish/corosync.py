# Copyright (c) 2020 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for Corosync
"""


from pylcommon import install_common


class LustreCorosyncCluster(install_common.InstallationCluster):
    # pylint: disable=too-few-public-methods,too-many-arguments
    """
    Lustre HA cluster config.
    """
    def __init__(self, mgs_dict, lustres, bindnetaddr, workspace, mnt_path):
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

    def ccl_start(self, log):
        """
        Config and create Lustre resource.
        """
        # start pacemaker and corosync
        command = "systemctl restart corosync pacemaker"
        for host in self.lcc_hosts:
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
