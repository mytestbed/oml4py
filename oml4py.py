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
from base64 import b64encode
from time import sleep
from time import time

__version__ = "2.10.3"

# Compatibility with Python 2 and 3's string type
if float(sys.version[:3])<3:
    def to_bytes(s):
        return s
else:
    def to_bytes(s):
        return bytes(s, "UTF-8")


class OMLBase:

    """
    This is an OML client implemented as a Python class
    """

    VERSION = __version__ # XXX: Backward compatibility
    VERSION_STRING = ("OML4Py Client %s" % (__version__,))
    COPYRIGHT = "Copyright 2012, NYU-Poly, Fraida Fund, 2012-2013 NICTA"

    PROTOCOL = 4

    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = 3003

    _args = None

    # constants for controlling error messages
    #
    ALL = 3
    ERROR = 3
    WARNING = 2
    INFO = 1
    NONE = 0

    _log = ALL


    # Initializer 
    #
    def __init__(self, appname, domain=None, sender=None, uri=None, expid=None):

        OMLBase._info("%s [Protocol V%d] %s" % (OMLBase.VERSION_STRING, OMLBase.PROTOCOL, OMLBase.COPYRIGHT))

        # process the command line
        if OMLBase._args is None:
            parser = argparse.ArgumentParser(prog=appname)
            parser.add_argument("--oml-id", default=None, help="node identifier")
            parser.add_argument("--oml-domain", default=None, help="experimental domain")
            parser.add_argument("--oml-collect", default="localhost", help="URI for a remote collection point")
            OMLBase._args, sys.argv[1:] = parser.parse_known_args()

        # setup instance variables
        self._state = "DISCONNECTED"
        self._sock = None
        self._starttime = None
        self._streams = 0
        self._schemas = {}
        self._schema_str = ""
        self._urandom = random.SystemRandom()
        self._has_valid_connection_attrs = True

        # set the connection details
        self._appname = appname
        if self._appname[:1].isdigit() or '-' in self._appname or '.' in self._appname:
            OMLBase._error("Invalid app name: %s" %  self._appname)
            self._has_valid_connection_attrs = False

        if expid:
            OMLBase._warning("%s parameter 'expid' is deprecated; please use 'domain' instead" % self.__class__.__name__)

        self._oml_domain = OMLBase._init_from(domain or expid, "oml_domain", "OML_DOMAIN", "OML_EXP_ID", "UNKNOWN")
        self._oml_id = OMLBase._init_from(sender, "oml_id", "OML_ID", "OML_NAME", "UNKNOWN")
        default_uri =  "tcp:%s:%d" %(OMLBase.DEFAULT_HOST, OMLBase.DEFAULT_PORT)
        uri = OMLBase._init_from(uri, "oml_collect", "OML_COLLECT", "OML_SERVER", default_uri)

        # parse URI
        uri_l = uri.split(":")
        if len(uri_l) == 1:
            # host
            self._omlserver = uri_l[0]
            self._omlport = self.DEFAULT_PORT
        elif len(uri_l) == 2:
            if uri_l[0] == "tcp":
                # tcp:host
                self._omlserver = uri_l[1]
                self._omlport = self.DEFAULT_PORT
            else:
                # host:port
                self._omlserver = uri_l[0]
                self._omlport = uri_l[1]
        elif len(uri_l) == 3:
            # tcp:host:port
            self._omlserver = uri_l[1]
            self._omlport = uri_l[2]
        else:
            OMLBase._error("'%s' is not a valid OML server URI" % uri)
            self._has_valid_connection_attrs = False

        # check port number is valid
        try:
            self._omlport = int(self._omlport)
            if not (0 <= self._omlport and self._omlport <= 65535):               
                OMLBase._error("Invalid port number '%d'" % self._omlport)
                self._has_valid_connection_attrs = False
        except ValueError:
            OMLBase._error("Cannot use '%s' as a port number" % self._omlport)
            self._has_valid_connection_attrs = False

        # register metadata schema (aka schema 0)
        self._add_schema("_experiment_metadata", "subject:string key:string value:string")


    # Start a connection with the OML server
    #
    def start(self):
        if self._state == "DISCONNECTED" or self._state == "DISABLED":
            self._starttime = int(time())
            if self._has_valid_connection_attrs and self._connect():
                self._state = "CONNECTED"
            else:
                self._state = "DISABLED"
                OMLBase._warning("Disabling OML output")
        else:
            return OMLBase._error("start() called unexpectedly (state=%s)!" % (self._state))
        return True


    # Close the connection to the OML server
    #
    def close(self):
        if self._state == "CONNECTED":
            self._disconnect()
            self._starttime = None
            self._state = "DISCONNECTED"
        elif self._state == "DISABLED":
            self._starttime = None
            self._state = "DISCONNECTED"
        else:
            return OMLBase._error("close() called when MP not started")
        return True


    # Generate a new GUID
    #
    def generate_guid(self):
        guid = self._urandom.getrandbits(64)
        while 0 == guid:
            guid = self._urandom.getrandbits(64)
        return guid


    # Add a new measurement point 
    #
    def addmp(self, mpname, schema_str):
        # check params
        if mpname is None or not OMLBase._is_valid_name(mpname):
            return OMLBase._error("Invalid measurement point name: %s" % mpname)
        elif mpname in self._schemas:
            return OMLBase._error("Attempted to add an existing MP '%s'" % mpname)
        elif not self._is_valid_schema_str(schema_str.strip()):
            return OMLBase._error("Invalid MP schema: %s" % schema_str.strip())
        # process new MP
        if self._state == "CONNECTED":
            return self._add_schema(mpname, schema_str) and self._inject_schema(mpname)
        if self._state == "DISABLED":
            return self._add_schema(mpname, schema_str) and self._write_schema(mpname)
        else:
            return self._add_schema(mpname, schema_str)


    # Inject a new measurement tuple
    #
    def inject(self, mpname, values):
        # check params
        if mpname is None or not OMLBase._is_valid_name(mpname):
            return OMLBase._error("Invalid measurement point name '%s'" % mpname)
        elif mpname not in self._schemas:
            return OMLBase._error("Tried to inject into unknown MP '%s'" % mpname)
        elif values is None:
            return OMLBase._error("No measurement tuple")
        # process injection request
        if self._state == "CONNECTED":
            return self._inject_measurement(mpname, values)
        elif self._state == "DISABLED":
            return self._write_measurement(mpname, values);
        else:
            return OMLBase._error("inject() called when in %s state" % self._state)


    # Inject metadata
    #
    def inject_metadata(self, mpname, key, value, fname = None):
        # check parameters
        if mpname is None or not OMLBase._is_valid_name(mpname):
            return OMLBase._error("Invalid measurement point name %s" % mpname)
        elif mpname not in self._schemas:
            return OMLBase._error("Tried to inject into unknown MP '%s'" % mpname)
        elif key is None or value is None:
            return OMLBase._error("Missing key or value")
        elif not OMLBase._is_valid_name(key):
            return OMLBase._error("'%s' is not a valid metadata key name\n" % key)
        # process injection request
        if self._state == "CONNECTED":
            return self._inject_metadata(mpname, key, value, fname)
        elif self._state == "DISABLED":
            return self._write_metadata(mpname, key, value, fname);
        else:
            return OMLBase._error("Did not call start")


    # state machine actions

    # Connect to the OML server
    # 
    def _connect(self):
        try:
            OMLBase._info("Collection URI is tcp:%s:%d" % (self._omlserver, self._omlport))
            # establish a connection
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.settimeout(5) 
            self._sock.connect((self._omlserver, self._omlport))
            self._sock.shutdown(socket.SHUT_RD)
            self._sock.settimeout(None)
            # create and send header
            header = "protocol: 4\n"
            header += "domain: " + self._oml_domain + "\n"
            header += "start-time: " + str(self._starttime) + "\n"
            header += "sender-id: " + self._oml_id + "\n"
            header += "app-name: " + self._appname + "\n"
            header += self._schema_str
            header += "content: text\n\n"
            self._sock.send(to_bytes(header))
            return True
        except socket.error as ex:
            return OMLBase._error("Could not connect to OML server: %s" %  str(ex))
        except Exception as ex:
            return OMLBase._error("Unexpected " + str(ex))

    # Disconnect from the OML server
    #
    def _disconnect(self):
        try:
            self._sock.shutdown(socket.SHUT_WR)
            self._sock.close()
            self._sock = None
            return True
        except socket.error as ex:
            return OMLBase._error("Could not disconnect cleanly from OML server: %s" %  str(ex))
        except Exception as ex:
            return OMLBase._error("Unexpected " + str(ex))


    # Process MP schema
    #
    def _add_schema(self, mpname, schema_str):
        # parse schema string
        schema_str = schema_str.strip()
        schema = re.findall("([A-Za-z_][A-Za-z0-9_]*):([A-Za-z_][A-Za-z0-9_]*)", schema_str)
        names = set()
        types = []
        for s in schema:
            name, type = s
            # check name
            name = name.lower()
            if name not in names:
                names.add(name)
            else:
                OMLBase._error("Duplicate field name: %s" % name)
                return None
            # check type
            type = type.lower()
            if not self._is_valid_type(type):
                OMLBase._error("Invalid type for %s: %s" % (name, type))
                return None
        # update the schema definition
        if mpname == "_experiment_metadata":
            target = mpname
        else:
            target = self._appname + "_" + mpname
        self._schema_str += "schema: " + str(self._streams) + " " + target + " " + schema_str + "\n"
        self._schemas[mpname] = (self._streams, names, schema, schema_str, 0)
        self._streams += 1
        return schema


    # Inject a schema update using schema0
    #
    def _inject_schema(self, mpname):
        inject_str = self._marshal_schema(mpname)
        return self._inject_metadata("_experiment_metadata", "schema", inject_str, None)

    # Inject a schema update using schema0
    #
    def _write_schema(self, mpname):
        inject_str = self._marshal_schema(mpname)
        return self._write_metadata("_experiment_metadata", "schema", inject_str, None)

    # Marshal MP for insertion using schema0
    #
    def _marshal_schema(self, mpname):
        stream, _, _, schema_str, _ = self._schemas[mpname]
        inject_str = str(stream) + ' '
        inject_str += self._appname + '_' + mpname + ' '
        inject_str += schema_str
        return inject_str

    # Marshal and inject a measurement tuple
    #
    def _inject_measurement(self, mpname, values):
        inject_str = self._marshal_measurement(mpname, values)
        if inject_str:
            try:
                self._sock.send(to_bytes(inject_str))
                return True
            except:
                return OMLBase._error("Could not send injected sample\n%s" % inject_str)
        else:
            return False


    # Write measurement tuple to stdout
    #
    def _write_measurement(self, mpname, values):
        inject_str = self._marshal_measurement(mpname, values)
        if inject_str:
            sys.stdout.write(inject_str)
            return True
        else:
            return False


    # Marshal a measurement tuple
    #
    def _marshal_measurement(self, mpname, values):
        timestamp = time() - self._starttime
        stream, names, schema, schema_str, seqno = self._schemas[mpname]
        self._schemas[mpname] = (stream, names, schema, schema_str, seqno+1)
        return self._marshal(timestamp, stream, seqno, schema, values)


    # Marshal and inject a metadata tuple
    #
    def _inject_metadata(self, mpname, key, value, fname):
        inject_str = self._marshal_metadata(mpname, key, value, fname)
        if inject_str:
            try:
                self._sock.send(to_bytes(inject_str))
                return True
            except:
                return OMLBase._error("Could not send injected metadata\n%s" % inject_str)
        else:
            return False


    # Write metadata to stdout
    #
    def _write_metadata(self, mpname, key, value, fname):
        inject_str = self._marshal_metadata(mpname, key, value, fname)
        if inject_str:
            sys.stdout.write(inject_str)
            return True
        else:
            return False


    # Marshal metadata
    #
    def _marshal_metadata(self, mpname, key, value, fname):

        # get stream, schema + seqno from the schema 0 MP
        stream, names, schema, schema_str, seqno = self._schemas["_experiment_metadata"]
        self._schemas["_experiment_metadata"] = (stream, names, schema, schema_str, seqno+1)

        # get names for MP
        timestamp = time() - self._starttime
        _, names, _, _, _  = self._schemas[mpname]

        # setup the subject
        subject = "."
        if mpname != "_experiment_metadata":
            subject += self._appname + "_" + mpname
            if fname:
                if fname in names:
                    subject += "." + fname
                else:
                    OMLBase._error("Field '%s' not found in MP '%s', not reporting" % (fname, mpname))
                    return None
        return self._marshal(timestamp, stream, seqno, schema, [subject, key, value])


    # Marshal measurement/metadata values
    #
    def _marshal(self, timestamp, stream, seqno, schema, values):
        # ensure we have enough values in tuple
        if len(schema) != len(values):
            OMLBase._error("Measurement tuple (%s) does not match schema (%s)" % (values, schema))
            return None
        # prepare the measurement tuple
        inject_str = str(timestamp) + '\t' + str(stream) + '\t' + str(seqno)
        i = 0
        for s in schema:
            name, type = s
            item = values[i]
            i += 1
            try:
                if "int32" == type:
                    inject_str += '\t'
                    x = int(item)
                    if not (-2147483648 <= x and x <= 2147483647):
                        OMLBase._error("Value %d out of range for %s:%s" % (x, name, type))
                        return None
                    inject_str += str(x)
                elif "uint32" == type:
                    inject_str += '\t'
                    x = int(item)
                    if not (0 <= x and x <= 4294967296):
                        OMLBase._error("Value %d out of range for %s:%s" % (x, name, type))
                        return None
                    inject_str += str(x)
                elif "int64" == type:
                    inject_str += '\t'
                    x = int(item)
                    if not (-9223372036854775808 <= x and x <= 9223372036854775807):
                        OMLBase._error("Value %d out of range for %s:%s" % (x, name, type))
                        return None
                    inject_str += str(x)
                elif "uint64" == type or "guid" == type:
                    inject_str += '\t'
                    x = int(item)
                    if not (0 <= x and x <= 18446744073709551616):
                        OMLBase._error("Value %d out of range for %s:%s" % (x, name, type))
                        return None
                    inject_str += str(x)
                elif "bool" == type:
                    inject_str += '\t'
                    x = bool(item)
                    inject_str += str(x)
                elif "double" == type:
                    inject_str += '\t'
                    x = float(item)
                    inject_str += str(x)
                elif "blob" == type:
                    inject_str += '\t'
                    inject_str += base64.b64encode(item);
                elif "string" == type:
                    inject_str += '\t'
                    inject_str += OMLBase._escape(str(item))
                else:
                    OMLBase._error("Unknown type for %s:%s" % (name, type))
                    return None
            except TypeError:
                OMLBase._error("Illegal type '%s' for %s:%s" % (item, name, type))
                return None
            except ValueError:
                OMLBase._error("Illegal value '%s' for %s:%s" % (item, name, type))
                return None
        # finish up
        inject_str += '\n'
        return inject_str


    # utilities

    # Set the log level for printed messages
    #
    @staticmethod
    def set_log_level(level):
        if OMLBase.NONE <= level and level <= OMLBase.ALL:
            OMLBase._log = level
        else:
            OMLBase._log = OMLBase.ALL


    # Report an informational message
    #
    @staticmethod
    def _info(msg):
        if OMLBase.INFO <= OMLBase._log:
            sys.stderr.write("INFO\t%s\n" % msg)


    # Report a warning message
    #
    @staticmethod
    def _warning(msg):
        if OMLBase.WARNING <= OMLBase._log:
            sys.stderr.write("WARN\t%s\n" % msg)


    # Report an error
    #
    @staticmethod
    def _error(msg):
        if OMLBase.ERROR <= OMLBase._log:
            sys.stderr.write("ERROR\t%s\n" % msg)
        return False


    # Initialization helper function
    #
    @staticmethod
    def _init_from(param, arg, env, depr = None, default = None):
        if param:
            return param
        elif arg in OMLBase._args.__dict__:
            return OMLBase._args.__dict__[arg]
        elif env in os.environ.keys():
            return os.environ[env]
        elif depr is not None and depr in os.environ.keys():
            OMLBase._warning("%s is deprecated; please use %s instead" % (depr, env))
            return os.environ[depr]
        elif default is not None:
            return default
        else:
            OMLBase._error("No %s specified" % param)
            return None


    # Tests if t is a valid typename
    #
    @staticmethod
    def _is_valid_type(t):
        return OMLBase._is_valid_scalar_type(t) # or OMLBase._is_valid_vector_type(t)


    # Tests if t is a valid scalar typename
    #
    @staticmethod
    def _is_valid_scalar_type(t):
        return t in ("bool", "int32", "uint32", "int64", "uint64", "double", "string", "blob", "guid")


    # Tests if t is a valid vector typename
    #
    # @staticmethod
    # def _is_valid_vector_type():
    #     return t in ("[bool]", "[int32]", "[uint32]", "[int64]", "[uint64]", "[double]")

    # Escape backslashes, tabs and carriage returns and newlines in s
    #
    @staticmethod
    def _escape(s) :
        return s.replace('\\', r'\\').replace('\t', r'\t').replace('\r', r'\r').replace('\n', r'\n')


    # Tests that name is valid
    #
    @staticmethod
    def _is_valid_name(name):
        return re.match("[A-Za-z_][A-Za-z0-9_]*", name) is not None


    # Tests that appname is valid
    #
    @staticmethod
    def _is_valid_appname(appname):
        p = "^[A-Za-z][A-Za-z0-9_]*$"
        return re.match(p, appname) is not None


    # Tests that schema is valid
    #
    @staticmethod
    def _is_valid_schema_str(schema_str):
        p="^[A-Za-z_][A-Za-z_0-9]*:[A-Za-z_][A-Za-z_0-9]*( +[A-Za-z_][A-Za-z_0-9]*:[A-Za-z_][A-Za-z_0-9]*)*$"
        return re.match(p, schema_str) is not None


def _selftest():
    b = OMLBase("testing")
    b.addmp("example1", "i32:int32")
    b.start()

    b.inject_metadata("example1", "units", "Hz", "i32")
    b.inject("example1", [-2147483648])

    b.addmp("example2", "i64:int64")
    b.inject("example2", [-9223372036854775807])
    b.inject("example2", [-4294967296])
    b.inject_metadata("example2", "units", "Centimes", "i64")

    b.close()
    b.start()

    b.inject("example1", [0])
    b.inject("example1", [2147483647])

    b.inject("example2", [0])
    b.inject("example2", [4294967295])
    b.inject("example2", [9223372036854775806])

    b.addmp("example3", "u32:uint32")
    b.inject_metadata("example3", "units", "Ducks", "u32")
    b.inject("example2", [0])
    b.inject("example2", [4294967295])

    b.close()


if __name__ == '__main__':
    _selftest()


# Local Variables:
# mode: Python
# indent-tabs-mode: nil
# tab-width: 4
# python-indent: 4
# End:
# vim: sw=4:sts=4:expandtab
