#!/usr/bin/env python3
#######################################################################
# -*- c-basic-offset: 8 -*-
# Copyright (c) 2020 Los Alamos National Labs, All rights reserved.
# Copyright (c) 2020 Open Grid Computing, Inc. All rights reserved.
#
# This software is available to you under a choice of one of two
# licenses.  You may choose to be licensed under the terms of the GNU
# General Public License (GPL) Version 2, available from the file
# COPYING in the main directory of this source tree, or the BSD-type
# license below:
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#      Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#
#      Redistributions in binary form must reproduce the above
#      copyright notice, this list of conditions and the following
#      disclaimer in the documentation and/or other materials provided
#      with the distribution.
#
#      Neither the name of Sandia nor the names of any contributors may
#      be used to endorse or promote products derived from this software
#      without specific prior written permission.
#
#      Neither the name of Open Grid Computing nor the names of any
#      contributors may be used to endorse or promote products derived
#      from this software without specific prior written permission.
#
#      Modified source versions must be plainly marked as such, and
#      must not be misrepresented as being the original software.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#######################################################################
import os
from ovis_ldms import ldms
from ldmsd.ldmsd_request import LDMSD_Request, LDMSD_Req_Attr
from maestro_util import cvt_intrvl_str_to_us, check_offset
import json
import errno
"""
@module Communicator
"""

class Communicator(object):
    """Implements an interface between a client and an instance of an ldmsd daemon"""
    msg_hdr_len = 24

    INIT        = 1
    CONNECTED   = 2
    CLOSED      = 3
    CTRL_STATES = [ INIT, CONNECTED, CLOSED ]

    def __init__(self, xprt, host, port, auth=None, auth_opt=None):
        """Create a communicator interface with an LDMS Daemon (LDMSD)

        Parameters:
        - The transport name: 'sock', 'rdma', 'ugni', or 'fabric'
        - The host name
        - The port number

        Keyword Parameters:
        auth     - The authentication plugin name
        auth_opt - Options (if any) to pass to the authentication plugin
        """
        self.INIT = Communicator.INIT
        self.CONNECTED = Communicator.CONNECTED
        self.CLOSED = Communicator.CLOSED
        self.host = host
        self.port = port
        self.xprt = xprt
        self.state = self.INIT
        self.auth = auth
        self.auth_opt = auth_opt
        self.ldms = None
        self.ldms = ldms.Xprt(name=self.xprt, auth=auth, auth_opts=auth_opt)

        if not self.ldms:
            raise ValueError(f"Failed to create LDMS transport "
                            f"{xprt}, {host}, {port}, {auth}, {auth_opt}")

        self.max_recv_len = self.ldms.msg_max

    def __del__(self):
        if self.ldms:
            self.ldms.close()
            self.ldms = None

    def __repr__(self):
        return f"<LDMSD_Communicator: host = {self.host}, port = {self.port}, "\
               f"xprt = {self.xprt}, state = {self.state}, "\
               f"max_recv_len = {self.max_recv_len}>"

    def reconnect(self):
        if self.ldms:
            self.close()
        self.ldms = ldms.Xprt(name=self.xprt, auth=self.auth, auth_opts=self.auth_opt)
        if self.ldms is None:
            return False
        self.max_recv_len = self.ldms.msg_max
        return self.connect()

    def connect(self):
        try:
            self.ldms.connect(self.host, self.port)
        except:
            return False
        self.type = "inband"
        self.state = self.CONNECTED
        return True

    def getState(self):
        return self.state

    def getMaxRecvLen(self):
        return self.max_recv_len

    def getHost(self):
        return self.host

    def getPort(self):
        return self.port

    def send_command(self, cmd):
        """This is called by the LDMSRequest class to send a message"""
        if self.state != self.CONNECTED:
            raise ConnectionError("Transport is not connected.")
        return self.ldms.send(cmd)
 
    def receive_response(self, recv_len = None):
        """This is called by the LDMSRequest class to receive a reply"""
        if self.state != self.CONNECTED:
            raise RuntimeError("Transport is not connected.")
        try:
            rsp = self.ldms.recv(timeout=5)
        except Exception as e:
            self.close()
            raise ConnectionError(str(e))
        return rsp

    def auth_add(self, name, plugin=None, auth_opt=None):
        """
        Add an authentication domain
        Parameters:
        name - The authentication domain name
        <plugin-specific attribute> e.g. conf=ldmsauth.conf
        """
        attrs=[ LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name) ]
        if plugin is not None:
            attrs.append(LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.PLUGIN, value=plugin))
        if auth_opt:
            if len(auth_opt.split('=')) == 1:
                auth_opt = 'conf='+auth_opt
            attrs.append(LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.STRING, value=auth_opt))
        req = LDMSD_Request(
                command_id=LDMSD_Request.AUTH_ADD,
                attrs=attrs
                )
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            return errno.ENOTCONN, None

    def listen(self, xprt, port, host=None, auth=None):
        """
        Add a listening endpoint

        Parameters:
        xprt - Transport name [sock, rdma, ugni]
        port - Port number
        [host] - Hostname
        [auth] - Authentication domain - If none, the default
                 authentication given the command line
                 (-a and -A) will be used

        """
        attr_list = [ LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.XPRT, value=xprt),
                      LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.PORT, value=port)
        ]
        if auth:
            attr_list.append(LDMSD_Req_Attr(attr_name='auth', value=auth))
        req = LDMSD_Request(
                command='listen',
                attrs=attr_list
              )
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception as e:
            return errno.ENOTCONN, None

    def dir_list(self):
        """
        Return the dir sets of this ldms daemon
        """
        try:
            dlist = self.ldms.dir()
            return 0, dlist
        except Exception as e:
            return errno.ENOTCONN, None

    def plugn_load(self, name):
        """
        Load an LDMSD plugin.

        Parameters:
        name  - The plugin name

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.PLUGN_LOAD,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            return errno.ENOTCONN, None

    def plugn_config(self, name, cfg_str):
        """
        Configure an LDMSD plugin

        Parameters:
        - The plugin name

        Keyword Parameters:
        - dictionary of plugin specific key/value pairs
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.PLUGN_CONFIG,
                attrs=[ LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
                        LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.STRING, value=cfg_str)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def plugn_stop(self, name):
        """
        Stop a LDMSD Plugin

        Parameters:
        - The plugin name
        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.PLUGN_STOP,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception as e:
            self.close()
            return errno.ENOTCONN, None

    def smplr_load(self, name):
        """
        Load an LDMSD sampler plugin.

        Parameters:
        name  - The plugin name

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.PLUGN_LOAD,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def smplr_status(self, name = None):
        """
        Query the LDMSD for the status of one or more sampler plugins.

        Keyword Parameters:
        name - If not None (default), the name of the producer to query.
      
        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or
          the object containing the producer status
        """
        if name:
            attrs = [ attrs.append(LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name)) ]
        else:
            attrs = None
        try:
            req = LDMSD_Request(command_id=LDMSD_Request.PLUGN_STATUS, attrs=attrs)
            req.send(self)
            resp = req.receive(self)
        except Exception:
            self.close()
            return errno.ENOTCONN, None
        err = resp['errcode']
        if err == 0 and resp['msg'] is not None:
            status = json.loads(resp['msg'])
        else:
            status = None
        return err, status

    def smplrset_status(self, name=None):
        """
        Return the metric sets provided by a sampler plugin.

        Keyword Parameters:
        name  - The name of the sampler to query. If None (default), all
                samplers are queried.

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or
          the object containing the sampler set status
        """
        if name:
            attrs = [
                LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name)
            ]
        else:
            attrs = None
        req = LDMSD_Request(
                command_id=LDMSD_Request.PLUGN_SETS,
                attrs=attrs
        )
        try:
            req.send(self)
            resp = req.receive(self)
            err = resp['errcode']
        except Exception:
            self.close()
            return errno.ENOTCONN, None
        if err == 0:
            status = json.loads(resp['msg'])
        else:
            status = None
        return err, status

    def plugn_start(self, name, interval_us, offset_us=None):
        # If offset unspecified, start in non-synchronous mode
        req_attrs = [ LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
                      LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.INTERVAL, value=str(interval_us))
                    ]
        if offset_us != None:
            offset_us = check_offset(interval_us, offset_us)
            req_attrs.append(LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.OFFSET, value=str(offset_us)))
        req = LDMSD_Request(
                command_id = LDMSD_Request.PLUGN_START,
                attrs=req_attrs
                )
        try:
            req.send(self)
            resp = req.receive(self)
            err = resp['errcode']
        except Exception:
            self.close()
            return errno.ENOTCONN, None
        if err == 0 and resp['msg'] is not None:
            status = json.loads(resp['msg'])
        else:
            status = None
        return err, status

    def prdcr_add(self, name, ptype, xprt, host, port, reconnect, auth=None, perm=None):
        """
        Add a producer. A producer is a peer to the LDMSD being configured.
        Once started, the LDSMD will attempt to connect to this peer
        periodically until the connection succeeds.

        A producer starts in the STOPPED state. Use the prdcr_start() function
        to start the producer.

        Parameters:
        - The name to give the producer. This name must be unique on the producer.
        - The type of the producer, one of 'passive', or 'active'
        - The transport type, one of 'sock', 'ugni', 'rdma', or 'fabric'
        - The hostname
        - The port number
        - The reconnect interval in microseconds

        Keyword Parameters:
        perm - The configuration client permission required to
               modify the producer configuration. Default is None.

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        attrs = [
            LDMSD_Req_Attr(attr_id = LDMSD_Req_Attr.NAME, value=name),
            LDMSD_Req_Attr(attr_id = LDMSD_Req_Attr.TYPE, value=ptype),
            LDMSD_Req_Attr(attr_id = LDMSD_Req_Attr.XPRT, value=xprt),
            LDMSD_Req_Attr(attr_id = LDMSD_Req_Attr.HOST, value=host),
            LDMSD_Req_Attr(attr_id = LDMSD_Req_Attr.PORT, value=str(port)),
            LDMSD_Req_Attr(attr_id = LDMSD_Req_Attr.INTERVAL, value=str(reconnect))
        ]
        if auth:
            attrs.append(LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.AUTH, value=auth))
        if perm:
            attrs.append(LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.PERM, value=str(perm)))

        req = LDMSD_Request(
                command_id=LDMSD_Request.PRDCR_ADD,
                attrs=attrs)
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def prdcr_del(self, name):
        """
        Delete an LDMS producer. The producer cannot be RUNNING.

        Parameters:
        name - The producer name

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.PRDCR_DEL,
                attrs = [
                    LDMSD_Req_Attr(attr_id = LDMSD_Req_Attr.NAME, value=name)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def prdcr_start(self, name, regex=True, reconnect=None):
        """
        Start one or more STOPPED producers

        Parameters:
        - The name of the producer to start. If regex=True (default),
          this is a regular expression.

        Keyword Parameters:
        regex     - True, the 'name' parameter is a regular expression.
                    Default is False.
        reconnect - The reconnect interval in microseconds. If not None, this
                    will override the interval specified when the producer
                    was created. Default is None.

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        if regex:
            cmd_id = LDMSD_Request.PRDCR_START_REGEX
            name_id = LDMSD_Req_Attr.REGEX
        else:
            cmd_id = LDMSD_Request.PRDCR_START
            name_id = LDMSD_Req_Attr.NAME

        attrs = [
            LDMSD_Req_Attr(attr_id = name_id, value=name),
        ]
        if reconnect:
            attrs.append(LDMSD_Req_Attr(attr_id = LDMSD_Req_Attr.INTERVAL,
                                        value = str(reconnect)))

        req = LDMSD_Request(command_id = cmd_id, attrs = attrs)
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None


    def prdcr_stop(self, name, regex=True):
        """
        Stop one or more RUNNING producers

        Parameters:
        - The name of the producer to start. If regex=True (default),
          this is a regular expression.

        Keyword Parameters:
        regex     - True, the 'name' parameter is a regular expression.
                    Default is False.

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        if regex:
            cmd_id = LDMSD_Request.PRDCR_STOP_REGEX
            name_id = LDMSD_Req_Attr.REGEX
        else:
            cmd_id = LDMSD_Request.PRDCR_STOP
            name_id = LDMSD_Req_Attr.NAME

        attrs = [
            LDMSD_Req_Attr(attr_id = name_id, value=name),
        ]

        req = LDMSD_Request(command_id = cmd_id, attrs = attrs)
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def prdcr_subscribe(self, regex, stream):
        """
        Subscribe to stream data from matching producers

        Parameters:
        - A regular expression matching producer names
        - The name of the stream

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(command_id = LDMSD_Request.PRDCR_SUBSCRIBE,
                attrs = [
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.REGEX, value=regex),
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.STREAM, value=stream)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def prdcr_status(self, name = None):
        """
        Query the LDMSD for the status of one or more producers.

        Keyword Parameters:
        name - If not None (default), the name of the producer to query.
      
        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or
          the object containing the producer status
        """
        if name:
            attrs = [ attrs.append(LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name)) ]
        else:
            attrs = None
        req = LDMSD_Request(command_id=LDMSD_Request.PRDCR_STATUS, attrs=attrs)
        try:
            req.send(self)
            resp = req.receive(self)
            err = resp['errcode']
        except Exception:
            self.close()
            return errno.ENOTCONN, None
        if err == 0:
            status = json.loads(resp['msg'])
        else:
            status = None
        return err, status

    def prdcrset_status(self, name = None, instance = None, schema = None):
        """
        Query the LDMSD for one or all producer's set status

        Keyword Parameters:
        name     - If not None (default), the producer to query
        instance - If not None (default), the set's instance name
        schema   - If not None (default), the set's schema name

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or
          the object containing the producer sets status
        """
        attrs = []
        if name:
            attrs.append(attrs.append(LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name)))
        if instance:
            attrs.append(attrs.append(LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.INSTANCE, value=instance)))
        if schema:
            attrs.append(attrs.append(LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.SCHEMA, value=schema)))
        if len(attrs) == 0:
            attrs = None    
        req = LDMSD_Request(command_id=LDMSD_Request.PRDCR_SET_STATUS, attrs=attrs)
        try:
            req.send(self)
            resp = req.receive(self)
        except Exception:
            self.close()
            return errno.ENOTCONN, None
        err = resp['errcode']
        if err == 0:
            status = json.loads(resp['msg'])
        else:
            status = None
        return err, status

    def updtr_add(self, name, interval=None, offset=None, push=None, auto=None, perm=None):
        """
        Add an Updater that will periodically update Producer metric sets either
        by pulling the content or by registering for an update push. The default
        is to pull a set's contents.

        Parameters:
        name      - The update policy name

        Keyword Parameters:
        interval  - The update data collection interval. This is when the
                    push argument is not given.
        push      - [onchange|true] 'onchange' means the updater will receive
                    updated set data the set sampler ends a transaction or
                    explicitly pushes the update. 'true' means the updater
                    will receive an update only when the set source explicitly
                    pushes the update.
                    If `push` is used, `auto_interval` cannot be `true`.
        auto      - [True|False] If True, the updater will schedule
                    set updates according to the update hint. The sets
                    with no hints will not be updated. If False, the
                    updater will schedule the set updates according to
                    the given sample interval. The default is False.
        perm      - The configuration client permission required to
                    modify the updater configuration.

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        attrs = [
            LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name)
        ]
        if interval:
            offset = check_offset(interval, offset)
            attrs += [
                LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.INTERVAL, value=str(interval)),
                LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.OFFSET, value=str(offset))
            ]
        elif push:
            if push != 'onchange' and push != True:
                return errno.EINVAL, "EINVAL"
            attrs += [
                LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.PUSH, value=str(push))
            ]
        else:
            if auto is None:
                return errno.EINVAL, "EINVAL"
            attrs += [
                LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.AUTO_INTERVAL, value=str(auto))
            ]
        if perm:
            attrs.append(LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.PERM, value=str(perm)))
        req = LDMSD_Request(command_id=LDMSD_Request.UPDTR_ADD, attrs=attrs)
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def updtr_del(self, name):
        """
        Delete an LDMS updater. The updater cannot be RUNNING.

        Parameters:
        name - The updater name

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.UPDTR_DEL,
                attrs = [
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def updtr_status(self, name=None):
        """
        Get the status of all updaters on a producer.

        Parameters:
        name - The name of the producer on which updater status is requested

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is the status of updaters on the producer, None if none exist, or an error message if status !=0.
        """
        attrs = []
        if name:
            attrs.append(LDMSD_Req_Attr(attr_id=LDMSD_Req_ATTR.NAME, value=name))
        req = LDMSD_Request(command_id=LDMSD_Request.UPDTR_STATUS, attrs=attrs)
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def updtr_start(self, name, interval=None, offset=None, auto=None):
        """
        Start a STOPPED updater.

        Parameters:
        - The name of the updater to start.

        Keyword Parameters:
        interval  - The update data collection interval in microseconds.
                    This is required if auto is False.
        auto      - [True|False] If True, the updater will schedule
                    set updates according to the update hint. The sets
                    with no hints will not be updated. If False, the
                    updater will schedule the set updates according to
                    the given sample interval. The default is False.

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        attrs = [
            LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
        ]
        if interval:
            offset = check_offset(interval, offset)
            if auto:
                return errno.EINVAL, "'auto' is incompatible with 'interval'"
            attrs += [
                LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.INTERVAL, value=str(interval)),
                LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.OFFSET, value=str(offset))
            ]
        elif auto:
            attrs += [
                LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.AUTO_INTERVAL, value=str(auto))
            ]

        req = LDMSD_Request(command_id=LDMSD_Request.UPDTR_START, attrs=attrs)
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def updtr_stop(self, name):
        """
        Stop a RUNNING updater.

        Parameters:
        - The name of the updater

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.UPDTR_STOP,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def updtr_prdcr_add(self, name, regex):
        """
        Add matching producers to an updater policy. The
        updater must be STOPPED.
        
        Parameters:
        - The updater name
        - A regular expression matching zero or more producers

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.UPDTR_PRDCR_ADD,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.REGEX, value=regex)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None
        
    def updtr_prdcr_del(self, name, regex):
        """
        Remove matching producers from an updater policy. The
        updater must be STOPPED.
        
        Parameters:
        - The updater name
        - A regular expression matching zero or more producers

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.UPDTR_PRDCR_DEL,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.REGEX, value=regex)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None
        
    def updtr_match_add(self, name, regex, match='schema'):
        """
        Add a match condition that identifies the set that will be
        updated.

        Parameters::
        name  - The update policy name
        regex - The regular expression string
        match - The value with which to compare; if match='inst' (default),
                the expression will match the set's instance name, if
                match='schema', the expression will match the set's
                schema name.

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.UPDTR_MATCH_ADD,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.REGEX, value=regex),
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.MATCH, value=match)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def updtr_match_del(self, name, regex, match='schema'):
        """
        Remove a match condition from an updater. The updater
        must be STOPPED.

        Parameters::
        name  - The update policy name
        regex - The regular expression string
        match - The value with which to compare; if match='inst' (default),
                the expression will match the set's instance name, if
                match='schema', the expression will match the set's
                schema name.

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.UPDTR_MATCH_DEL,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.REGEX, value=regex),
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.MATCH, value=match)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def updtr_match_list(self, name=None):
        """
        Return a list of sets that an updater is matched to update.
        name - The update policy name

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is a list of updaters and their sets, None if none exist, or an error message if status !=0.
        """
        req = LDMSD_Request(
            command_id=LDMSD_Request.UPDTR_MATCH_LIST,
            attrs=[
                LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name)
            ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def strgp_add(self, name, plugin, container, schema, perm=0o777, flush=None):
        """
        Add a Storage Policy that will store metric set data when
        updates complete on a metric set.

        Parameters:
        name        The unique storage policy name.
        plugin      The name of the storage backend.
        container   The storage backend container name.
        schema      The schema name of the metric set to store.

        Keyword Parameters:
        perm        The permission required to modify the storage policy,
                    default perm=0o600

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        attrs = [
            LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
            LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.PLUGIN, value=plugin),
            LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.CONTAINER, value=container),
            LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.SCHEMA, value=schema),
            LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.PERM, value=str(perm)),
        ]
        if flush is not None:
            attrs.append(LDMSD_Req_Attr(attr_name='flush', value=flush))
        req = LDMSD_Request(command_id=LDMSD_Request.STRGP_ADD, attrs=attrs)
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def strgp_del(self, name):
        """
        Delete a storage policy. The storage policy cannot be RUNNING.

        Parameters:
        name - The policy name

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.STRGP_DEL,
                attrs = [
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def strgp_start(self, name):
        """
        Start a STOPPED storage policy.

        Parameters:
        - The name of the storage policy to start.

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        attrs = [
            LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
        ]
        req = LDMSD_Request(command_id=LDMSD_Request.STRGP_START, attrs=attrs)
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def strgp_stop(self, name):
        """
        Stop a RUNNING storage policy.

        Parameters:
        - The name of the storage policy

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.STRGP_STOP,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def strgp_prdcr_add(self, name, regex):
        """
        Add matching producers to an storage policy. The
        storage policy must be STOPPED.
        
        Parameters:
        - The storage policy name
        - A regular expression matching zero or more producers

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.STRGP_PRDCR_ADD,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.REGEX, value=regex)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None
        
    def strgp_prdcr_del(self, name, regex):
        """
        Remove matching producers from an storage policy. The
        storage policy must be STOPPED.
        
        Parameters:
        - The storage policy name
        - A regular expression matching zero or more producers

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.STRGP_PRDCR_DEL,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.REGEX, value=regex)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None
        
    def strgp_metric_add(self, name, metric_name):
        """
        Add a metric name that will be stored. By default all metrics
        in the schema specified in strgp_add will be stored.

        Parameters::
        - The update policy name
        - The name of the metric to store

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.STRGP_METRIC_ADD,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.METRIC, value=metric_name)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def strgp_metric_del(self, name, metric_name):
        """
        Remove a metric name from the storage policy. The storage policy
        must be STOPPED.

        Parameters:
        - The storage policy name
        - The metric name to remove

        Returns:
        A tuple of status, data
        - status is an errno from the errno module
        - data is an error message if status != 0 or None
        """
        req = LDMSD_Request(
                command_id=LDMSD_Request.STRGP_METRIC_DEL,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.NAME, value=name),
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.METRIC, value=metric_name)
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def xprt_stats(self, reset=False):
        """Query the daemon's telemetry data"""
        req = LDMSD_Request(
                command_id=LDMSD_Request.XPRT_STATS,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.RESET,
                                value=str(reset)),
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def thread_stats(self, reset=False):
        """Query the daemon's I/O thread utilization data"""
        req = LDMSD_Request(
                command_id=LDMSD_Request.THREAD_STATS,
                attrs=[
                    LDMSD_Req_Attr(attr_id=LDMSD_Req_Attr.RESET,
                                value=str(reset)),
                ])
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def daemon_status(self):
        """Query the daemon's status"""
        req = LDMSD_Request(command_id=LDMSD_Request.DAEMON_STATUS)
        try:
            req.send(self)
            resp = req.receive(self)
            return resp['errcode'], resp['msg']
        except Exception:
            self.close()
            return errno.ENOTCONN, None

    def close(self):
        self.state = self.CLOSED
        if self.ldms:
            self.ldms.close()
            self.ldms = None

if __name__ == "__main__":
    comm = Communicator(
            "sock", "localhost", 10000, auth="munge"
        )
    error, status = comm.prdcr_add('orion-01', 'active', 'sock', 'orion-01', 10000, 20000000)
    error, status = comm.prdcr_status('orion-01')
    error, status = comm.smplr_status()
    error, status = comm.smplrset_status()
    pass

