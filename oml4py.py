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
import sys
import os
import socket
from time import time
from time import sleep


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

    VERSION = "@VERSION@"
    VERSION_STRING = ("OML4Py Client V@VERSION@")
    COPYRIGHT = "Copyright 2012, NYU-Poly, Fraida Fund"
    PROTOCOL = 4

    DEFAULT_HOST="localhost"
    DEFAULT_PORT=3003


    def _banner(self):
        sys.stderr.write("INFO\t%s [Protocol V%d] %s\n" % (self.VERSION_STRING, self.PROTOCOL, self.COPYRIGHT))


    def __init__(self,appname,domain=None,sender=None,uri=None,expid=None):
        self._banner()
        self.oml = True
        seld.urandom = random.SystemRandom()

        # process the command line
        parser = argparse.ArgumentParser(prog=appname)
        parser.add_argument("--oml-id", default=None, help="node identifier")
        parser.add_argument("--oml-domain", default=None, help="experimental domain")
        parser.add_argument("--oml-collect", default="localhost", help="URI for a remote collection point")
        args = parser.parse_args()

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
        elif args.oml_domain is not None:
            self.domain = args.oml_domain
        elif 'OML_DOMAIN' in os.environ.keys():
            self.domain = os.environ['OML_DOMAIN']
        elif 'OML_EXP_ID' in os.environ.keys():
            self.domain = os.environ['OML_EXP_ID']
            sys.stderr.write("WARN\tOML_EXP_ID is deprecated; please use OML_DOMAIN instead\n")
        else:
            sys.stderr.write("ERROR\tNo experimental domain specified\n")
            self._disable_oml()

        if uri is None:
            if args.oml_collect is not None:
                uri = args.oml_collect
            elif 'OML_COLLECT' in os.environ.keys():
                uri = os.environ['OML_COLLECT']
            elif 'OML_SERVER' in os.environ.keys():
                uri = os.environ['OML_SERVER']
                sys.stderr.write("WARN\tOML_SERVER is deprecated; please use OML_COLLECT instead\n")
            else:
                uri = "tcp:%s:%s" %(self.DEFAULT_HOST, self.DEFAULT_PORT)
            uri_l = uri.split(":")

        if len(uri_l) == 1:       # host
            self.omlserver = uri_l[0]
            self.omlport = self.DEFAULT_PORT
        elif len(uri_l) == 2:
            if uri_l[0] == "tcp":   # tcp:host or host:port
                self.omlserver = uri_l[1]
                self.omlport = self.DEFAULT_PORT
            else:
                self.omlserver = uri_l[0]
                self.omlport = uri_l[1]
        elif len(uri_l) == 3:       # tcp:host:port
            self.omlserver = uri_l[1]
            self.omlport = uri_l[2]
            try:
                self.omlport = int(self.omlport)
            except ValueError:
                sys.stderr.write("ERROR\tCannot use '%s' as a port number\n" % self.omlport)
                self._disable_oml()

        if sender is not None:
            self.sender = sender
        elif args.oml_id is not None:
            self.sender = args.oml_id
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
        if self.oml:        
          # Set socket
          self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
          self.sock.settimeout(5) 


    def addmp(self,mpname,schema):
 
      if "-" in mpname or "." in mpname:
        sys.stderr.write("ERROR\tInvalid measurement point name: %s\n" %  mpname)
        self._disable_oml()
      else:
        # Count number of streams
        self.streams += 1
        if self.streams > 1:
           self.schema += '\n'
        str_schema = "schema: " + str(self.streams) + " " + self.appname + "_" + mpname + " " + schema
        self.schema += str_schema
        self.nextseq[mpname] = 0
        self.streamid[mpname] = self.streams
   

    def start(self):

      if self.oml:        
        # Use socket to connect to OML server
        sys.stderr.write("INFO\tCollection URI is tcp:%s:%d\n" % (self.omlserver, self.omlport))

        self.starttime = int(time())

        header = "protocol: " + str(self.PROTOCOL) + '\n' + "domain: " + str(self.domain) + '\n' + \
               "start-time: " + str(self.starttime) + '\n' + "sender-id: " + str(self.sender) + '\n' + \
               "app-name: " + str(self.appname) + '\n' + \
               str(self.schema) + '\n' + "content: text" + '\n' + '\n'    

        try:
          self.sock.connect((self.omlserver,self.omlport))
          self.sock.settimeout(None)
          self.sock.send(to_bytes(header))
        except socket.error as e:
          sys.stderr.write("ERROR\tCould not connect to OML server: %s\n" %  e)
          self._disable_oml()
          sys.stdout.write(header)
      else:
        sys.stderr.write("WARN\tOML disabled\n")


    def close(self):
        streamid = None
        if self.oml:
            self.sock.close()


    def inject(self,mpname,values):

        str_inject = ""
        if self.oml and self.starttime == 0:
            sys.stderr.write("ERROR\tDid not call start()\n")
            self._disable_oml()

        timestamp = time() - self.starttime
        try:
            streamid = self.streamid[mpname]
            seqno = self.nextseq[mpname]
            str_inject = str(timestamp) + '\t' + str(streamid) + '\t' + str(seqno)
        except KeyError:
            sys.stderr.write("ERROR\tTried to inject into unknown MP '%s'\n" % mpname)
            return

        try:
            for item in values:
                str_inject += '\t'
                str_inject += _escape(str(item))
                str_inject += '\n'
                self.nextseq[mpname]+=1
        except TypeError:
            sys.stderr.write("ERROR\tInvalid measurement list\n")
            return

        if self.oml:
            try:
                self.sock.send(to_bytes(str_inject))
            except:
                sys.stderr.write("ERROR\tCould not send injected sample\n")
        else:
            sys.stdout.write(str_inject)


    def generate_guid(self):
        guid = self.urandom.getrandbits(64)
        while 0 == guid:
            guid = self.urandom.getrandbits(64)
        return guid


    def _disable_oml(self):
        if not self.oml:
            return
        sys.stderr.write("WARN\tDisabling OML output\n")
        self.oml = False


    def _escape(self, s) :
        """
        Escape '\\', '\t', '\r' and '\n' characters in s
        """
        return s.replace('\\', r'\\').replace('\t', r'\t').replace('\r', r'\r').replace('\n', r'\n')


# Local Variables:
# mode: Python
# indent-tabs-mode: nil
# tab-width: 4
# python-indent: 4
# End:
# vim: sw=4:sts=4:expandtab
