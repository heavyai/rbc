"""Collection of helper functions
"""

import dis
import socket
import subprocess
import inspect
import ipaddress
import netifaces
import re
import uuid
import ctypes
import llvmlite.binding as llvm
import warnings


def get_version(package):
    """Return a package version as a 3-tuple of integers.
    """
    if package == 'numba':
        import numba
        v = numba.__version__.replace('rc', '.').replace('dev', '.').replace('+', '.').split('.')
        return tuple(map(int, v[:3]))
    raise NotImplementedError(f'get version of package {package}')


def version_date(version):
    """Return date from version dev part as an integer containing digits
    `yyyymmdd`. Return 0 if date information is not available.
    """
    if version and isinstance(version[-1], str):
        m = re.match(r'.*([12]\d\d\d[01]\d[0123]\d)', version[-1])
        if m is not None:
            return int(m.groups()[0])
    return 0


def version_hash(version):
    """Return hash from version dev part as string. Return None if hash
    information is not available.
    """
    if version and isinstance(version[-1], str):
        m = re.match(r'.*([12]\d\d\d[01]\d[0123]\d)[-](\w{10,10}\b)', version[-1])
        if m is not None:
            return m.groups()[1]
    return None


def parse_version(version):
    """Return parsed version tuple from version string.

    For instance:

    >>> parse_version('1.2.3dev4')
    (1, 2, 3, 'dev4')
    """

    m = re.match(r'(\d+)[.](\d+)[.](\d+)(.*)', version)
    if m is not None:
        major, minor, micro, dev = m.groups()
        if not dev:
            return (int(major), int(minor), int(micro))
        return (int(major), int(minor), int(micro), dev)

    m = re.match(r'(\d+)[.](\d+)(.*)', version)
    if m is not None:
        major, minor, dev = m.groups()
        if not dev:
            return (int(major), int(minor))
        return (int(major), int(minor), dev)

    m = re.match(r'(\d+)(.*)', version)
    if m is not None:
        major, dev = m.groups()
        if not dev:
            return int(major),
        return (int(major), dev)

    if version:
        return version,
    return ()


def get_local_ip():
    """Return localhost IP.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = socket.gethostbyname(socket.gethostname())
    finally:
        s.close()
    return IP


def is_localhost(ip):
    """Check if ip is the IP of a localhost.
    """
    a = ipaddress.ip_address(ip)
    if a.is_loopback:
        return True
    interfaces = netifaces.interfaces()
    mac_list = []
    AF_PACKET = getattr(netifaces, 'AF_PACKET', None)
    if AF_PACKET is None:
        # netifaces version < 0.10.8:
        AF_PACKET = getattr(netifaces, 'AF_LINK', None)
    for i in interfaces:
        ifaddrs = netifaces.ifaddresses(i)
        if netifaces.AF_INET in ifaddrs:
            for ifaddr in ifaddrs[netifaces.AF_INET]:
                if ifaddr.get('addr') == ip:
                    return True
        if AF_PACKET in ifaddrs:
            for ifaddr in ifaddrs[AF_PACKET]:
                mac = ifaddr.get('addr')
                if mac is not None:
                    mac_list.append(mac.replace(':', ''))
    local_mac = hex(uuid.getnode())[2:]
    return local_mac in mac_list


def runcommand(*cmd):
    """Run a command with arguments and return stdout messages.

    The results are cached.
    """
    if cmd not in runcommand.cache:
        result = subprocess.run(cmd, stdout=subprocess.PIPE)
        runcommand.cache[cmd] = result.stdout.decode()
    return runcommand.cache[cmd]


runcommand.cache = {}


def get_datamodel():
    """Return the data model of a host system.
    """
    short_sizeof = ctypes.sizeof(ctypes.c_short()) * 8
    int_sizeof = ctypes.sizeof(ctypes.c_int()) * 8
    long_sizeof = ctypes.sizeof(ctypes.c_long()) * 8
    ptr_sizeof = ctypes.sizeof(ctypes.c_voidp()) * 8
    longlong_sizeof = ctypes.sizeof(ctypes.c_longlong()) * 8
    return {
        (0,  16,  0, 16,  0): 'IP16',     # PDP-11 Unix
        (16, 16, 32, 16,  0): 'IP16L32',  # PDP-11 Unix
        (16, 16, 32, 32,  0): 'I16LP32',  # MC68000, AppleMac68K, MS x86
        (16, 32, 32, 32,  0): 'ILP32',    # IBM 370, VAX Unix, workstations
        (16, 32, 32, 32, 64): 'ILP32LL',  # MS Win32
        (16, 32, 32, 64, 64): 'LLP64',    # MS Win64
        (16, 32, 64, 64, 64): 'LP64',     # Most UNIX systems
        (16, 64, 64, 64, 64): 'ILP64',    # HAL
        (64, 64, 64, 64, 64): 'SILP64',   # UNICOS
    }[short_sizeof, int_sizeof, long_sizeof, ptr_sizeof, longlong_sizeof]


def triple_split(triple):
    """Split target triple into parts.
    """
    arch, vendor, os = triple.split('-', 2)
    if '-' in os:
        os, env = os.split('-', 1)
    else:
        env = ''
    return arch, vendor, os, env


def triple_matches(triple, other):
    """Check if target triples match.
    """
    if triple == other:
        return True
    if triple == 'cuda':
        return triple_matches('nvptx64-nvidia-cuda', other)
    if triple == 'cuda32':
        return triple_matches('nvptx-nvidia-cuda', other)
    if triple == 'host':
        return triple_matches(llvm.get_process_triple(), other)
    if other in ['cuda', 'cuda32', 'host']:
        return triple_matches(other, triple)
    arch1, vendor1, os1, env1 = triple_split(triple)
    arch2, vendor2, os2, env2 = triple_split(other)
    if os1 == os2 == 'linux':
        return (arch1, env1) == (arch2, env2)
    return (arch1, vendor1, os1, env1) == (arch2, vendor2, os2, env2)


def get_function_source(func):
    """Get function source code with intent fixes.

    Warning: not all function objects have source code available.
    """
    source = ''
    intent = None
    for line in inspect.getsourcelines(func)[0]:
        if intent is None:
            intent = len(line) - len(line.lstrip())
        source += line[intent:]
    return source


def check_returns_none(func):
    """Return True if function return value is always None.

    Warning: the result of the check may be false-negative. For instance:

    .. code-block:: python

        def foo():
            a = None
            return a

        check_returns_none(foo) == false

    """
    last_instr = None
    for instr in dis.Bytecode(func):
        if instr.opname == 'RETURN_VALUE':
            if last_instr.opname in ['LOAD_CONST', 'LOAD_FAST', 'LOAD_ATTR', 'LOAD_GLOBAL']:
                if last_instr.argval is None:
                    continue
                return False
            else:
                if any(map(last_instr.opname.startswith,
                           ['UNARY_', 'BINARY_', 'CALL_', 'COMPARE_'])):
                    return False
                warnings.warn(
                    'check_returns_none: assuming non-None return'
                    f' from last_instr={last_instr} (FIXME)')
                return False
        last_instr = instr

    return True


DEFAULT = object()
UNSPECIFIED = object()
