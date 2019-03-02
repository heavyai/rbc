import inspect
import os
import re
from . import types

include_match = re.compile(r'^\s*include\s+"(?P<filename>[\w./\\]+)"',
                           flags=re.M).match


def resolve_includes(thrift_content, include_dirs):
    lines = []
    for line in thrift_content.splitlines():
        m = include_match(line)
        if m is not None:
            filename = m.group('filename')
            for p in include_dirs:
                fn = os.path.join(p, filename)
                if os.path.isfile(fn):
                    # print('including {}'.format(fn))
                    d = os.path.abspath(os.path.dirname(fn))
                    content = open(fn).read()
                    resolve_includes(content, [d] + include_dirs)
                    lines.append(content)
                    break
            else:
                raise FileNotFoundError('could not find {} in {}'.format(
                    filename, os.pathsep.join(include_dirs)))
        else:
            lines.append(line)
    return '\n'.join(lines)


def dispatchermethod(mth):
    """Decorator for dispatcher methods.

    Method annotations are used to transform the arguments and the
    return values of dispatcher methods to required types.
    """
    signature = inspect.signature(mth)

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
