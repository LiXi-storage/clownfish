"""
Common library for clownfish
"""
#
# pacemaker, corosync, pcs are needed by HA of Clownfish
#
CLOWNFISH_DEPENDENT_RPMS = ["corosync",
                            "pacemaker",
                            "pcs",
                            "rsync",
                            "libyaml",
                            "PyYAML",
                            "python2-filelock",
                            "pytz",
                            "python-dateutil",
                            "zeromq3",
                            "python2-zmq",
                            "protobuf-python",
                            "python-requests",
                            "python-prettytable"]
