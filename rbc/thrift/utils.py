import inspect
import os
import re
import functools
from . import types

include_match = re.compile(r'^\s*include\s+"(?P<filename>[\w._/\\]+)"',
                           flags=re.M).match
namespace_match = re.compile(
    r'^\s*namespace\s+(?P<scope>\w+)\s+(?P<identifier>[\w._]+)',
    flags=re.M).match


def resolve_includes(thrift_content, include_dirs,
                     _namespace_list=None,
                     _included=None):
    if _namespace_list is None:
        recursive = False
        _namespace_list = []
        _included = []
    else:
        recursive = True
    lines = []
    for line in thrift_content.splitlines():
        m = include_match(line)
        if m is not None:
            filename = m.group('filename')
            for p in include_dirs:
                fn = os.path.join(p, filename)
                if os.path.isfile(fn):
                    # print('including {}'.format(fn))
                    afn = os.path.abspath(fn)
                    if afn in _included:
                        break
                    _included.append(afn)
                    content = open(fn).read()
                    d = os.path.abspath(os.path.dirname(fn))
                    content = resolve_includes(content, [d] + include_dirs,
                                               _namespace_list,
                                               _included)
                    lines.append(content)
                    break
            else:
                raise FileNotFoundError('could not find {} in {}'.format(
                    filename, os.pathsep.join(include_dirs)))
            continue
        m = namespace_match(line)
        if m is not None:
            scope_and_identifier = m.group('scope'), m.group('identifier')
            if scope_and_identifier not in _namespace_list:
                _namespace_list.append(scope_and_identifier)
            continue
        lines.append(line)
    if not recursive:
        for scope_and_identifier in _namespace_list:
            lines.insert(0, 'namespace %s %s' % scope_and_identifier)
    content = '\n'.join(lines)
    return content


def dispatchermethod(mth):
    """Decorator for dispatcher methods.

    Method annotations are used to transform the arguments and the
    return values of dispatcher methods to required types.
    """
    signature = inspect.signature(mth)

    @functools.wraps(mth)
    def wrapper(*args):
        thrift = args[0].thrift
        new_args = []
        for arg, (argname, sig) in zip(args, signature.parameters.items()):
            cls = sig.annotation
            if cls == sig.empty:
                if isinstance(arg, thrift.Data):
                    cls = None
                else:
                    cls = type(arg)
            arg = types.toobject(thrift, arg, cls=cls)
            new_args.append(arg)
        r = mth(*new_args)
        tcls = signature.return_annotation
        if tcls == sig.empty:
            if isinstance(r, (int, float, list, set, dict, str, bytes)):
                # types supported by thriftpy2
                tcls = type(r)
            else:
                tcls = thrift.Data
        return types.fromobject(thrift, tcls, r)
    wrapper.signature = signature
    return wrapper
