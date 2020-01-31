"""
Common library for clownfish
"""
#
# pacemaker, corosync, pcs are needed by HA of Clownfish
#
CLOWNFISH_DEPENDENT_RPMS = ["corosync",
                            "crmsh",
                            "libyaml",
                            "PyYAML",
                            "pacemaker",
                            "pcs",
                            "protobuf-python",
                            "python2-filelock",
                            "python2-zmq",
                            "python-dateutil",
                            "python-requests",
                            "python-prettytable",
                            "pytz",
                            "rsync",
                            "zeromq3"]
