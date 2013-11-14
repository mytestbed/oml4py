# 
# Description: Python Class for OML Connection and Data Uploading
#
# From http://oml.mytestbed.net/projects/oml/wiki/Description_of_Text_protocol
#
# The client opens a TCP connection to an OML server and then sends a header 
# followed by a sequence of measurement tuples. The header consists of a 
# sequence of key, value pairs terminated by a new line. The end of the header 
# section is identified by an empty line. Each measurement tuple consists of 
# a new-line-terminated string which contains TAB-separated text-based 
# serialisations of each tuple element. Finally, the client ends the session 
# by simply closing the TCP connection.
#
# Author: Fraida Fund
#
# Date: 08/06/2012
#

import argparse
import random
import re
import sys
import os
import socket
from time import time
from time import sleep

__version__ = "2.10.4"

# Compatibility with Python 2 and 3's string type
if float(sys.version[:3])<3:
    def to_bytes(s):
        return s
else:
    def to_bytes(s):
        return bytes(s, "UTF-8")


class OMLBase:
    """
    This is a Python OML class
    """

    VERSION = __version__ # XXX: Backward compatibility
    VERSION_STRING = ("OML4Py Client %s" % (__version__,))
    COPYRIGHT = "Copyright 2012, NYU-Poly, Fraida Fund, 2012-2013 NICTA"
    PROTOCOL = 4

    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = 3003

    args = None

    def _banner(self):
        sys.stderr.write("INFO\t%s [Protocol V%d] %s\n" % (self.VERSION_STRING, self.PROTOCOL, self.COPYRIGHT))


    def __init__(self,appname,domain=None,sender=None,uri=None,expid=None):
        self._banner()
        self.oml = True
        self.urandom = random.SystemRandom()

        # process the command line and consume OML arguments
        if self.args is None:
            parser = argparse.ArgumentParser(prog=appname)
            parser.add_argument("--oml-id", default=None, help="node identifier")
            parser.add_argument("--oml-domain", default=None, help="experimental domain")
            parser.add_argument("--oml-collect", default=None, help="URI for a remote collection point")
            newargv = [sys.argv[0]]
            self.args, sys.argv[1:] = parser.parse_known_args()

        # Set all the instance variables
        self.appname = appname
        if self.appname[:1].isdigit() or '-' in self.appname or '.' in self.appname:
            sys.stderr.write("ERROR\tInvalid app name: %s\n" %  self.appname)
            self._disable_oml()

        if expid is not None:
            sys.stderr.write("WARN\t%s parameter 'expid' is deprecated; please use 'domain' instead\n" % self.__class__.__name__)

        if domain is None:
            domain = expid

        if domain is not None:
            self.domain = domain
        elif self.args.oml_domain is not None:
            self.domain = self.args.oml_domain
        elif 'OML_DOMAIN' in os.environ.keys():
            self.domain = os.environ['OML_DOMAIN']
        elif 'OML_EXP_ID' in os.environ.keys():
            self.domain = os.environ['OML_EXP_ID']
            sys.stderr.write("WARN\tOML_EXP_ID is deprecated; please use OML_DOMAIN instead\n")
        else:
            sys.stderr.write("ERROR\tNo experimental domain specified\n")
            self._disable_oml()

        if uri is None:
            if self.args.oml_collect is not None:
                uri = self.args.oml_collect
            elif 'OML_COLLECT' in os.environ.keys():
                uri = os.environ['OML_COLLECT']
            elif 'OML_SERVER' in os.environ.keys():
                uri = os.environ['OML_SERVER']
                sys.stderr.write("WARN\tOML_SERVER is deprecated; please use OML_COLLECT instead\n")
            else:
                uri = "tcp:%s:%s" %(self.DEFAULT_HOST, self.DEFAULT_PORT)
        uri_l = uri.split(":")

        if len(uri_l) == 1:     # host
            self.omlserver = uri_l[0]
            self.omlport = self.DEFAULT_PORT
        elif len(uri_l) == 2:
            if uri_l[0] == "tcp": # tcp:host or host:port
                self.omlserver = uri_l[1]
                self.omlport = self.DEFAULT_PORT
            else:
                self.omlserver = uri_l[0]
                self.omlport = uri_l[1]
        elif len(uri_l) == 3:   # tcp:host:port
            self.omlserver = uri_l[1]
            self.omlport = uri_l[2]
            try:
                self.omlport = int(self.omlport)
            except ValueError:
                sys.stderr.write("ERROR\tCannot use '%s' as a port number\n" % self.omlport)
                self._disable_oml()

        if sender is not None:
            self.sender = sender
        elif self.args.oml_id is not None:
            self.sender = self.args.oml_id
        else:
            try:
                self.sender =  os.environ['OML_NAME']
            except KeyError:
                sys.stderr.write("ERROR\tNo sender ID specified (OML_NAME)\n")
                self._disable_oml()

        self.starttime = 0
        self.streams = 0
        self.schema = ""
        self.nextseq = {}
        self.streamid = {}
        self.fields = {}
        self.metaseq = {}
        self.sock = None
        self.addmp(None, "subject:string key:string value:string")


    def addmp(self,mpname,schema):
        if mpname and self._is_valid_name(mpname):
            sys.stderr.write("ERROR\tInvalid measurement point name: %s\n" %  mpname)
            self._disable_oml()
            return
        elif mpname in self.fields:
            sys.stderr.write("ERROR\tAttempted to add an existing MP '%s'\n" %  mpname)
            return
        # remember field names
        fs = set()
        names = re.findall("[A-Za-z_][A-Za-z0-9_]+(?=:[A-Za-z_][A-Za-z0-9_]+)", schema)
        for n in names:
            fs.add(n)
            self.fields[mpname] = fs
        # update the schema definition
        if self.streams > 0:
            self.schema += '\n'
        if mpname is None:
            target = "_experiment_metadata"
        else:
            target = self.appname + "_" + mpname
        str_schema = str(self.streams) + " " + target + " " + schema
        self.schema += "schema: " + str_schema
        self.nextseq[mpname] = 0
        self.metaseq[mpname] = 0
        self.streamid[mpname] = self.streams
        self.streams += 1
        # if we've already called start send schema update using schema 0
        if self.starttime != 0:
            self.inject_metadata(None, "schema", str_schema, None)


    def start(self):
        if self.oml:
            if self.sock is None:
                self.starttime = int(time())
                # create header
                header = "protocol: " + str(self.PROTOCOL) + '\n' + "domain: " + str(self.domain) + '\n' + \
                    "start-time: " + str(self.starttime) + '\n' + "sender-id: " + str(self.sender) + '\n' + \
                    "app-name: " + str(self.appname) + "\n" + \
                    str(self.schema) + '\n' + "content: text" + '\n' + '\n'    
                # connect to OML server
                sys.stderr.write("INFO\tCollection URI is tcp:%s:%d\n" % (self.omlserver, self.omlport))
                try:
                    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.sock.settimeout(5) 
                    self.sock.connect((self.omlserver,self.omlport))
                    self.sock.settimeout(None)
                    self.sock.send(to_bytes(header))
                except socket.error as e:
                    sys.stderr.write("ERROR\tCould not connect to OML server: %s\n" %  e)
                    self._disable_oml()
                    sys.stdout.write(header)
            else:
                sys.stderr.write("ERROR\tstart() called unexpectedly\n")
        else:
            sys.stderr.write("WARN\tOML disabled\n")


    def close(self):
        self.streamid = None
        if self.oml and self.sock:
            self.sock.close()
            self.sock = None;
            self.starttime = 0


    def generate_guid(self):
        guid = self.urandom.getrandbits(64)
        while 0 == guid:
            guid = self.urandom.getrandbits(64)
        return guid


    def inject(self,mpname,values):
        if self.oml and self.starttime == 0:
            sys.stderr.write("ERROR\tDid not call start()\n")
            self._disable_oml()
        # prepare the measurement info
        str_inject = ""
        timestamp = time() - self.starttime
        try:
            streamid = self.streamid[mpname]
            seqno = self.nextseq[mpname]
            str_inject = str(timestamp) + '\t' + str(streamid) + '\t' + str(seqno)
            self.nextseq[mpname] += 1
        except KeyError:
            sys.stderr.write("ERROR\tTried to inject into unknown MP '%s'\n" % mpname)
            return
        # prepare the measurement tuple
        try:
            for item in values:
                str_inject += '\t'
                str_inject += self._escape(str(item))
            str_inject += '\n'
        except TypeError:
            sys.stderr.write("ERROR\tInvalid measurement list\n")
            return
        # either inject it or display it
        if self.oml and self.sock:
            try:
                self.sock.send(to_bytes(str_inject))
            except:
                sys.stderr.write("ERROR\tCould not send injected sample\n")
                sys.stdout.write(str_inject)
        else:
            sys.stdout.write(str_inject)


    def inject_metadata(self,mpname,key,value,fname=None):
        # check parameters
        if self.oml and self.starttime == 0:
            sys.stderr.write("ERROR\tDid not call start()\n")
            return
        elif key is None or value is None:
            sys.stderr.write("ERROR\tMissing key or value\n")
            return
        elif self._is_valid_name(key):
            sys.stderr.write("ERROR\t'%s' is not a valid metadata key name\n" % key)
            return
        elif mpname and self._is_valid_name(mpname):
            sys.stderr.write("ERROR\t'%s' is not a valid MP name\n" % mpname)
            return
        # prepare the measurement info
        str_inject = ""
        timestamp = time() - self.starttime
        try:
            streamid = 0
            seqno = self.metaseq[mpname]
            str_inject = str(timestamp) + '\t' + str(streamid) + '\t' + str(seqno)
            self.metaseq[mpname] += 1
        except KeyError:
            sys.stderr.write("ERROR\tTried to inject metadata into unknown MP '%s'\n" % mpname)
            return
        # prepare the metadata
        subject = "."
        if mpname:
            target = self.appname + "_" + mpname
            subject += target
            if fname:
                if fname in self.fields[mpname]:
                    subject += "." + fname
                else:
                    sys.stderr.write("WARN\tField '%s' not found in MP '%s', not reporting\n" % (fname, mpname))
                    return
        str_inject += '\t' + subject + '\t' + str(key) + '\t' + self._escape(str(value)) + '\n'
        # either inject it or display it
        if self.oml and self.sock:
            try:
                self.sock.send(to_bytes(str_inject))
            except:
                sys.stderr.write("ERROR\tCould not send injected metadata\n")
                sys.stdout.write(str_inject)
        else:
            sys.stdout.write(str_inject)


    def _disable_oml(self):
        if not self.oml:
            return
        sys.stderr.write("WARN\tDisabling OML output\n")
        self.oml = False


    def _escape(self, s) :
        return s.replace('\\', r'\\').replace('\t', r'\t').replace('\r', r'\r').replace('\n', r'\n')


    def _is_valid_name(self, name):
        return re.match("[A-Za-z_]\([A-Za-z0-9_]\)*", name) is not None


# Local Variables:
# mode: Python
# indent-tabs-mode: nil
# tab-width: 4
# python-indent: 4
# End:
# vim: sw=4:sts=4:expandtab
