import subprocess


def runcommand(*cmd):
    """Run a command with arguments and return stdout messages.

    The results are cached.
    """
    if cmd not in runcommand.cache:
        result = subprocess.run(cmd, stdout=subprocess.PIPE)
        runcommand.cache[cmd] = result.stdout
    return runcommand.cache[cmd]


runcommand.cache = {}


def parse_proc_cpuinfo(content):
    info = {}
    processor_count = 0
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        key, value = line.split(':', 1)
        key = key.strip()
        if key == 'processor':
            processor_count += 1
        else:
            if key in info:
                assert info[key] == value, repr((key, info[key], value))
            else:
                info[key] = value
    info['processors'] = str(processor_count)
    return info
