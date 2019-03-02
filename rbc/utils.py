import subprocess


def runcommand(*cmd):
    """Run a command with arguments and return stdout messages.

    The results are cached.
    """
    if cmd not in runcommand.cache:
        result = subprocess.run(cmd, stdout=subprocess.PIPE)
        runcommand.cache[cmd] = result.stdout.decode()
    return runcommand.cache[cmd]


runcommand.cache = {}
