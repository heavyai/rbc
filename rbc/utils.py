import socket
import subprocess
import ipaddress
import netifaces
import uuid


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
    for i in interfaces:
        ifaddrs = netifaces.ifaddresses(i)
        if netifaces.AF_INET in ifaddrs:
            for ifaddr in ifaddrs[netifaces.AF_INET]:
                if ifaddr.get('addr') == ip:
                    return True
        if netifaces.AF_PACKET in ifaddrs:
            for ifaddr in ifaddrs[netifaces.AF_PACKET]:
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
