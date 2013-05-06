#!/bin/env python
#
# Copyright (c) 2012-2013 NICTA, Olivier Mehani
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
#
# = oml4py-simple-example.py
#
# == Description
#
# A very simple straightforward example of OML4Py.
#
import oml4py
import time
import math

omlInst = oml4py.OMLBase("oml4PySimpleExample", "foo", "n1", "tcp:localhost:3003")

omlInst.addmp("SinMP", "label:string angle:int32 value:double")
omlInst.addmp("CosMP", "label:string value:double")

omlInst.start()

for i in range(5):
    time.sleep(0.5)
    angle = 15 * i
    omlInst.inject("SinMP", [ ("label_%s" % angle), angle, math.sin(angle) ])
    omlInst.inject("CosMP", [ ("label_%s" % angle), math.cos(angle) ])

omlInst.close()
