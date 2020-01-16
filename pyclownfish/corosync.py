# Copyright (c) 2020 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Library for Corosync
"""


class LustreCorosyncCluster(object):
    # pylint: disable=too-few-public-methods
    """
    Lustre HA cluster config.
    """
    def __init__(self, mgs_dict, lustres, bindnetaddr):
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

        hostnames = []
        for lustrefs in lustres.values():
            services = lustrefs.lf_services()
            for service in services:
                for instance in service.ls_instances.values():
                    host = instance.lsi_host
                    if host.sh_hostname not in hostnames:
                        hostnames.append(host.sh_hostname)

        for mgs in mgs_dict.values():
            for instance in mgs.ls_instances.values():
                host = instance.lsi_host
                if host.sh_hostname not in hostnames:
                    hostnames.append(host.sh_hostname)

        for hostname in hostnames:
            nodelist_string += ("""
    node {
        ring0_addr: %s
    }""" % (hostname))
        nodelist_string += """
}"""
        self.lcc_corosync_config += nodelist_string
