OML4Py: Native OML Implementation in Python
===========================================

This is a Python module for the OML measurement library based on the OML text
protocol [oml-text].


Installation
------------

Install using pip [oml4py-pypi]::

    $ pip install oml4py

or download the package and run::

    $ python setup.py install


Usage
-----

This module provides the OMLBase class, which contains the following methods:

* init

* start

* addmp

* inject

* close

To use OML in a python project, import the OMLBase class::

    from oml4py import OMLBase

Start by initializing an OMLBase object. The init method takes up to four
arguments:

* the name of the application,

* the name of the experiment,

* the name of the node,

* and the OML server URI in the form ``tcp:hostname:port``


For example::

    x=OMLBase("app", "an-exp","r","tcp:myomlserver.com:3003")

The only mandatory argument is the first one (the name of the
application).  If you skip any of the others, they may be defined by
environment variables (OML_DOMAIN, OML_NAME, OML_COLLECT) or via
command-line options. If these variables are not passed in explicitly
and neither the command line options nor environment variables are
defined then the application will run with OML disabled, and the
measurements that would have been sent to OML will be printed on
stdout instead.

Next, add one or more measurement points. Pass the name of the measurement
point and a schema string to the start method. The schema string should
be in the format
``measurement_name1:measurement_type1 measurement_name2:measurement_type2``
for example::

    x.addmp("fft", "freq:long amplitude:double fft_val:double")

When you have set up all your measurement points, call start()::

    x.start()

When you have a measurement point to send to OML, store them in a
tuple Then pass the name of the measurement point and the tuple of
values to inject, as follows::

    x.inject("fft", (259888, 15, -38))

At the end of your program, call close to gracefully close the database::

    x.close()


Authors
-------

OML4Py was contributed by Fraida Fund, from NYU-Poly.


License
-------

Copyright 2012 NYU-Poly, Fraida Fund

Copyright 2012-2013 National ICT Australia (NICTA), Australia

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

[oml-text]: http://oml.mytestbed.net/projects/oml/wiki/Description_of_Text_protocol
[oml4py-pypi]: http://pypi.python.org/pypi/oml4py/
