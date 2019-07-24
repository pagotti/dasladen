"""
Compat Module

Compatibility functions for run Python 2 and 3 versions

"""

import sys
import importlib
import io

PY2 = sys.version_info.major == 2
PY3 = sys.version_info.major == 3

def maketrans(from_str, to_str):
    if PY2:
        from string import maketrans
        return maketrans(from_str, to_str)
    else:
        return str.maketrans(from_str, to_str)

def reload_module(module_obj):
    if PY2:
        reload(module_obj)
    else:
        importlib.reload(module_obj)

def translate_unicode(str_input):
    if PY2:
        return str_input.encode('latin1') if isinstance(str_input, unicode) else str_input
    else:
        return str_input

def open(file, mode='r', buffering=-1, encoding=None):
    if buffering == 0 and (not 'b' in mode):
        buffering = 2
    return io.open(file, mode, buffering, encoding)
