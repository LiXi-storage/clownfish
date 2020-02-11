# Copyright (c) 2018 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Clownfish Server that a console can connect o
Clownfish is an automatic management system for Lustre
"""
import threading
import traceback
import sys
import os
import time
import yaml
import zmq

# Local libs
from pylcommon import utils
from pylcommon import cstr
from pylcommon import cmd_general
from pylcommon import constants
from pyclownfish import clownfish_pb2
from pyclownfish import clownfish
from pyclownfish import clownfish_command

CLOWNFISH_WORKER_NUMBER = 10
CLOWNFISH_CONNECTION_TIMEOUT = 30

CLOWNFISH_SERVER_LOG_DIR = "/var/log/clownfish_server"


def remove_tailing_newline(log, output):
    """
    Prepare the command reply
    """
    if output != "":
        if output[-1] != "\n":
            log.cl_error("unexpected output [%s], no tailing newline is "
                         "found", output)
        else:
            output = output[0:-1]
    return output


class ClownfishConnection(object):
    """
    Each connection from a client has an object of this type
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, parent_log, client_hash, sequence, instance):
        self.cc_client_hash = client_hash
        self.cc_sequence = sequence
        self.cc_atime = time.time()
        self.cc_connection_name = "connection_%s" % sequence
        self.cc_instance = instance
        self.cc_workspace = instance.ci_workspace + "/" + self.cc_connection_name
        ret = utils.mkdir(self.cc_workspace)
        if ret:
            reason = ("failed to create directory [%s] on local host" %
                      (self.cc_workspace))
            parent_log.cl_error(reason)
            raise Exception(reason)
        self.cc_condition = threading.Condition()
        self.cc_command_log = parent_log.cl_get_child(self.cc_connection_name,
                                                      resultsdir=self.cc_workspace,
                                                      record_consumer=True,
                                                      condition=self.cc_condition)
        self.cc_last_retval = None
        self.cc_quit = False
        # Used when a command running thread needs input from console
        self.cc_input_prompt = None
        self.cc_input_result = None

    def cc_update_atime(self):
        """
        Whenever the connection has a message, udpate the atime
        """
        self.cc_atime = time.time()

    def cc_cmdline_finish(self):
        """
        notify that the cmdline thread finished
        """
        self.cc_condition.acquire()
        self.cc_condition.notifyAll()
        self.cc_condition.release()

    def cc_cmdline_thread(self, cmdline):
        """
        Thread to run a command line
        """
        # pylint: disable=broad-except,too-many-branches,too-many-statements
        # pylint: disable=too-many-locals
        log = self.cc_command_log
        log.cl_debug("start thread of command line [%s]", cmdline)
        args = cmdline.split()
        argc = len(args)
        if argc == 0:
            log.cl_stderr("empty command line [%s]", cmdline)
            log.cl_result.cr_exit_status = -1
            self.cc_cmdline_finish()
            return

        operation = clownfish_command.CLOWNFISH_DELIMITER_AND
        retval = 0
        while operation != "":
            if ((operation == clownfish_command.CLOWNFISH_DELIMITER_AND and retval != 0) or
                    (operation == clownfish_command.CLOWNFISH_DELIMITER_OR and retval == 0)):
                log.cl_debug("finish cmdline because delimiter [%s] and "
                             "retval [%d]", operation, retval)
                break

            argc = len(args)
            assert argc > 0

            operation = ""
            for argc_index in range(argc):
                arg = args[argc_index]
                if arg == clownfish_command.CLOWNFISH_DELIMITER_AND:
                    operation = clownfish_command.CLOWNFISH_DELIMITER_AND
                elif arg == clownfish_command.CLOWNFISH_DELIMITER_OR:
                    operation = clownfish_command.CLOWNFISH_DELIMITER_OR
                elif arg == clownfish_command.CLOWNFISH_DELIMITER_CONT:
                    operation = clownfish_command.CLOWNFISH_DELIMITER_CONT

                if operation != "":
                    if argc_index == 0:
                        log.cl_stderr("invalid command line [%s]: no command before [%s]",
                                      cmdline, arg)
                        log.cl_result.cr_exit_status = -1
                        self.cc_cmdline_finish()
                        return
                    current_args = args[:argc_index]

                    if argc_index == argc - 1:
                        log.cl_stderr("invalid command line [%s]: tailing [%s]",
                                      cmdline, arg)
                        log.cl_result.cr_exit_status = -1
                        self.cc_cmdline_finish()
                        return
                    args = args[argc_index + 1:]
                    break
            if operation == "":
                current_args = args

            subsystem_name = current_args[0]
            if subsystem_name in clownfish_command.SUBSYSTEM_DICT:
                subsystem = clownfish_command.SUBSYSTEM_DICT[subsystem_name]
                if len(current_args) == 1:
                    log.cl_stderr("please specify command after [%s]",
                                  subsystem_name)
                    retval = -1
                    continue
                else:
                    command = current_args[1]
                    options = current_args[2:]
            else:
                command = subsystem_name
                subsystem = clownfish_command.SUBSYSTEM_NONE
                options = current_args[1:]
            if command not in subsystem.ss_command_dict:
                log.cl_stderr('unknown command [%s] for subsystem [%s]',
                              command, subsystem.ss_name)
                retval = -1
                continue
            else:
                ccommand = subsystem.ss_command_dict[command]
                try:
                    retval = ccommand.cc_function(self, options)
                    log.cl_debug("finished cmdline part %s", current_args)
                except Exception, err:
                    log.cl_stderr("failed to run cmdline part %s, exception: "
                                  "%s, %s",
                                  current_args, err, traceback.format_exc())
                    retval = -1
                    continue

        log.cl_debug("finished thread of command line [%s]", cmdline)
        log.cl_result.cr_exit_status = retval
        self.cc_cmdline_finish()

    def cc_ask_for_input(self, prompt, timeout=60):
        """
        Ask for input from console
        """
        log = self.cc_command_log
        self.cc_condition.acquire()
        self.cc_input_prompt = prompt
        self.cc_input_result = None
        self.cc_condition.notifyAll()
        self.cc_condition.release()

        time_start = time.time()
        result = None
        while True:
            time_now = time.time()
            elapsed = time_now - time_start
            if elapsed >= timeout:
                log.cl_error("timeout after waiting [%d] seconds for input",
                             timeout)
                return None

            self.cc_condition.acquire()
            if self.cc_input_result is None:
                self.cc_condition.wait(10)
            else:
                result = self.cc_input_result
                log.cl_debug("got input [%s] from console", result)
                self.cc_input_result = None
            self.cc_condition.release()
            if result is not None:
                break
        log.cl_debug("got input [%s] from console", result)
        return result

    def cc_abort(self):
        """
        Set the abort flag of the log
        """
        self.cc_command_log.cl_abort = True

    def cc_consume_command_log(self, thread_log, command_reply):
        """
        Get the log of the command
        """
        thread_log.cl_debug("consuming log of connection [%s]",
                            self.cc_connection_name)
        log = self.cc_command_log
        log.cl_debug("consuming log of connection [%s]",
                     self.cc_connection_name)

        self.cc_condition.acquire()
        if ((log.cl_result.cr_exit_status is None) and
                log.cl_is_empty_nolock() and (not self.cc_input_prompt)):
            self.cc_condition.wait(clownfish_command.MAX_FAST_COMMAND_TIME)
        input_prompt = self.cc_input_prompt
        self.cc_input_prompt = None
        self.cc_condition.release()

        if log.cl_result.cr_exit_status is not None:
            command_reply.ccry_type = clownfish_pb2.ClownfishMessage.CCRYT_FINAL
            command_reply.ccry_final.ccfr_exit_status = log.cl_result.cr_exit_status
            command_reply.ccry_final.ccfr_quit = self.cc_quit
        elif input_prompt is not None:
            log.cl_debug("asking for input")
            command_reply.ccry_type = clownfish_pb2.ClownfishMessage.CCRYT_INPUT
            command_reply.ccry_input_request.ccirt_prompt = input_prompt
        else:
            command_reply.ccry_type = clownfish_pb2.ClownfishMessage.CCRYT_PARTWAY
        records = command_reply.ccry_logs
        for clog_record in log.cl_consume():
            record = records.add()
            log_record = clog_record.clr_record
            record.clr_is_stdout = clog_record.clr_is_stdout
            record.clr_is_stderr = clog_record.clr_is_stderr
            record.clr_name = log_record.name
            record.clr_levelno = log_record.levelno
            record.clr_pathname = log_record.pathname
            record.clr_lineno = log_record.lineno
            record.clr_funcname = log_record.funcName
            record.clr_created_second = int(log_record.created)
            record.clr_msg = log_record.msg

    def cc_command(self, thread_log, cmd_line, command_reply):
        """
        Run command for a connection
        """
        # pylint: disable=broad-except
        thread_log.cl_info("running command [%s]", cmd_line)
        log = self.cc_command_log
        log.cl_debug("start running the command on server")
        self.cc_last_retval = log.cl_result.cr_exit_status
        log.cl_result.cr_clear()
        log.cl_abort = False

        utils.thread_start(self.cc_cmdline_thread, (cmd_line, ))
        self.cc_consume_command_log(thread_log, command_reply)
        thread_log.cl_debug("returned reply of command [%s]", cmd_line)

    def cc_interact(self, thread_log, request, reply):
        """
        Handle the interact request
        """
        # pylint: disable=no-self-use
        thread_log.cl_info("handling interact")
        candidates = \
            clownfish_command.clownfish_interact_candidates(self,
                                                            request.cirt_line,
                                                            request.cirt_begidx,
                                                            request.cirt_endidx,
                                                            False)
        print candidates
        for candidate in candidates:
            reply.ciry_candidates.append(candidate)


class ClownfishServer(object):
    """
    This server that listen and handle requests from console
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, log, server_port, instance):
        self.cs_log = log
        self.cs_running = True
        self.cs_instance = instance
        assert isinstance(instance, clownfish.ClownfishInstance)
        self.cs_url_client = "tcp://*:" + str(server_port)
        self.cs_url_worker = "inproc://workers"
        self.cs_context = zmq.Context.instance()
        self.cs_client_socket = self.cs_context.socket(zmq.ROUTER)
        self.cs_client_socket.bind(self.cs_url_client)
        self.cs_worker_socket = self.cs_context.socket(zmq.DEALER)
        self.cs_worker_socket.bind(self.cs_url_worker)
        # Sequence is protected by cs_condition
        self.cs_sequence = 0
        # The key is the sequence of the connection, protected by cs_condition
        self.cs_connections = {}
        self.cs_condition = threading.Condition()
        for worker_index in range(CLOWNFISH_WORKER_NUMBER):
            log.cl_info("starting worker thread [%d]", worker_index)
            utils.thread_start(self.cs_worker_thread, (worker_index, ))
        utils.thread_start(self.cs_connection_cleanup_thread, ())

    def cs_connection_cleanup_thread(self):
        """
        Cleanup dead thread
        """
        log = self.cs_log
        log.cl_info("starting connection cleanup thread")
        while self.cs_running:
            sleep_time = CLOWNFISH_CONNECTION_TIMEOUT
            now = time.time()
            self.cs_condition.acquire()
            for client_uuid, connection in self.cs_connections.items():
                if (connection.cc_atime + CLOWNFISH_CONNECTION_TIMEOUT <=
                        now):
                    log.cl_info("connection [%s] times out, cleaning it up",
                                client_uuid)
                    del self.cs_connections[client_uuid]
                else:
                    my_sleep_time = (connection.cc_atime +
                                     CLOWNFISH_CONNECTION_TIMEOUT - now)
                    if my_sleep_time < sleep_time:
                        sleep_time = my_sleep_time
            self.cs_condition.release()
            time.sleep(sleep_time)
        log.cl_info("connection cleanup thread exited")

    def cs_connection_allocate(self, client_hash):
        """
        Allocate a new connection
        """
        log = self.cs_log
        self.cs_condition.acquire()
        sequence = self.cs_sequence
        self.cs_sequence += 1
        connection = ClownfishConnection(log, client_hash, sequence, self.cs_instance)
        self.cs_connections[str(sequence)] = connection
        self.cs_condition.release()
        log.cl_debug("allocated uuid [%s] for client with hash [%s]",
                     sequence, client_hash)
        return connection

    def cs_connection_find(self, client_uuid):
        """
        Find the connection from UUID
        """
        log = self.cs_log
        self.cs_condition.acquire()
        sequence_string = str(client_uuid)
        if sequence_string in self.cs_connections:
            connection = self.cs_connections[sequence_string]
        else:
            connection = None
        self.cs_condition.release()
        if connection is None:
            log.cl_debug("can not find client with uuid [%s]",
                         client_uuid)
        else:
            log.cl_debug("found client with uuid [%s]",
                         client_uuid)
            connection.cc_update_atime()
        return connection

    def cs_connection_delete(self, client_uuid):
        """
        Find the connection from UUID
        """
        log = self.cs_log
        sequence_string = str(client_uuid)
        self.cs_condition.acquire()
        if sequence_string in self.cs_connections:
            del self.cs_connections[sequence_string]
            ret = 0
        else:
            ret = -1
        self.cs_condition.release()
        if ret == 0:
            log.cl_info("disconnected client [%s] is cleaned up",
                        client_uuid)
        else:
            log.cl_error("failed to delete connection from [%s], because it "
                         "doesnot exist", client_uuid)
        return ret

    def cs_fini(self, log):
        """
        Finish server
        """
        self.cs_instance.ci_fini(log)
        self.cs_running = False
        self.cs_client_socket.close()
        self.cs_worker_socket.close()
        self.cs_context.term()

    def cs_worker_thread(self, worker_index):
        """
        Worker routine
        """
        # pylint: disable=too-many-nested-blocks,too-many-locals
        # pylint: disable=too-many-branches,too-many-statements
        # Socket to talk to dispatcher
        instance = self.cs_instance

        name = "thread_worker_%s" % worker_index
        thread_workspace = instance.ci_workspace + "/" + name
        if not os.path.exists(thread_workspace):
            ret = utils.mkdir(thread_workspace)
            if ret:
                self.cs_log.cl_error("failed to create directory [%s] on local host",
                                     thread_workspace)
                return -1
        elif not os.path.isdir(thread_workspace):
            self.cs_log.cl_error("[%s] is not a directory", thread_workspace)
            return -1
        log = self.cs_log.cl_get_child(name, resultsdir=thread_workspace)

        log.cl_info("starting worker thread [%s]", worker_index)
        dispatcher_socket = self.cs_context.socket(zmq.REP)
        dispatcher_socket.connect(self.cs_url_worker)

        while self.cs_running:
            try:
                request_message = dispatcher_socket.recv()
            except zmq.ContextTerminated:
                log.cl_info("worker thread [%s] exiting because context has "
                            "been terminated", worker_index)
                break
            cmessage = clownfish_pb2.ClownfishMessage
            request = cmessage()
            request.ParseFromString(request_message)
            log.cl_debug("received request with type [%s]", request.cm_type)
            reply = cmessage()
            reply.cm_protocol_version = cmessage.CPV_ZERO
            reply.cm_errno = cmessage.CE_NO_ERROR

            if request.cm_type == cmessage.CMT_CONNECT_REQUEST:
                client_hash = request.cm_connect_request.ccrt_client_hash
                connection = self.cs_connection_allocate(client_hash)
                reply.cm_type = cmessage.CMT_CONNECT_REPLY
                reply.cm_connect_reply.ccry_client_hash = client_hash
                reply.cm_client_uuid = connection.cc_sequence
            else:
                client_uuid = request.cm_client_uuid
                reply.cm_client_uuid = client_uuid
                connection = self.cs_connection_find(client_uuid)
                if connection is None:
                    log.cl_error("received a request with UUID [%s] that "
                                 "doesnot exist",
                                 client_uuid)
                    reply.cm_type = cmessage.CMT_GENERAL
                    reply.cm_errno = cmessage.CE_NO_UUID
                elif request.cm_type == cmessage.CMT_PING_REQUEST:
                    reply.cm_type = cmessage.CMT_PING_REPLY
                elif request.cm_type == cmessage.CMT_INTERACT_REQUEST:
                    reply.cm_type = cmessage.CMT_INTERACT_REPLY
                    connection.cc_interact(log, request.cm_interact_request,
                                           reply.cm_interact_reply)
                elif request.cm_type == cmessage.CMT_COMMAND_REQUEST:
                    reply.cm_type = cmessage.CMT_COMMAND_REPLY
                    cmd_line = request.cm_command_request.ccrt_cmd_line
                    connection.cc_command(log, cmd_line, reply.cm_command_reply)
                elif request.cm_type == cmessage.CMT_COMMAND_PARTWAY_QUERY:
                    reply.cm_type = cmessage.CMT_COMMAND_REPLY
                    query = request.cm_command_partway_query
                    if query.ccpq_abort:
                        connection.cc_abort()
                    connection.cc_consume_command_log(log,
                                                      reply.cm_command_reply)
                elif request.cm_type == cmessage.CMT_COMMAND_INPT_REPLY:
                    reply.cm_type = cmessage.CMT_COMMAND_REPLY
                    input_reply = request.cm_command_input_reply
                    if input_reply.cciry_abort:
                        connection.cc_abort()
                    log.cl_debug("got input [%s] for command",
                                 input_reply.cciry_input)
                    connection.cc_condition.acquire()
                    connection.cc_input_result = input_reply.cciry_input
                    connection.cc_condition.notifyAll()
                    connection.cc_condition.release()
                    connection.cc_consume_command_log(log,
                                                      reply.cm_command_reply)
                else:
                    reply.cm_type = cmessage.CMT_GENERAL
                    reply.cm_errno = cmessage.CE_NO_TYPE
                    log.cl_error("recived a request with type [%s] that "
                                 "is not supported",
                                 request.cm_type)

                if (reply.cm_type == cmessage.CMT_COMMAND_REPLY and
                        reply.cm_command_reply.ccry_type == cmessage.CCRYT_FINAL and
                        reply.cm_command_reply.ccry_final.ccfr_quit):
                    ret = self.cs_connection_delete(connection.cc_sequence)
                    if ret:
                        log.cl_error("failed to delete connection [%s]",
                                     connection.cc_sequence)
                        reply.cm_errno = cmessage.CE_NO_UUID

            reply_message = reply.SerializeToString()
            dispatcher_socket.send(reply_message)
        dispatcher_socket.close()
        log.cl_info("worker thread [%s] exited", worker_index)

    def cs_loop(self):
        """
        Proxy the server
        """
        # pylint: disable=bare-except
        try:
            zmq.proxy(self.cs_client_socket, self.cs_worker_socket)
        except:
            self.cs_log.cl_info("got exception when running proxy, exiting")


def clownfish_server_do_loop(log, workspace, config, config_fpath):
    """
    Server routine
    """
    server_config = utils.config_value(config, cstr.CSTR_CLOWNFISH_SERVER)
    if server_config is None:
        log.cl_error("no [%s] is configured, please correct file [%s]",
                     cstr.CSTR_CLOWNFISH_SERVER, config_fpath)
        return -1

    clownfish_server_port = utils.config_value(server_config, cstr.CSTR_PORT)
    if clownfish_server_port is None:
        log.cl_info("no [%s] is configured in config [%s], using port [%s]",
                    cstr.CSTR_PORT, cstr.CSTR_CLOWNFISH_SERVER,
                    constants.CLOWNFISH_DEFAULT_SERVER_PORT)
        clownfish_server_port = constants.CLOWNFISH_DEFAULT_SERVER_PORT

    clownfish_instance = clownfish.init_instance(log, workspace, config,
                                                 config_fpath)
    if clownfish_instance is None:
        log.cl_error("failed to init Clownfish")
        return -1

    cserver = ClownfishServer(log, clownfish_server_port, clownfish_instance)
    cserver.cs_loop()
    cserver.cs_fini(log)


def clownfish_server_loop(log, workspace, config_fpath):
    """
    Start Clownfish holding the configure lock
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
        ret = clownfish_server_do_loop(log, workspace, config, config_fpath)
    except:
        ret = -1
        log.cl_error("exception: %s", traceback.format_exc())

    return ret


def usage():
    """
    Print usage string
    """
    utils.eprint("Usage: %s <config_file>" %
                 sys.argv[0])


def main():
    """
    Start clownfish server
    """
    cmd_general.main(constants.CLOWNFISH_CONFIG, CLOWNFISH_SERVER_LOG_DIR,
                     clownfish_server_loop)
