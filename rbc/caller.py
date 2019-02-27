# Author: Pearu Peterson
# Created: February 2019


class Caller(object):
    """Remote JIT caller
    """

    def __init__(self, server, signatures, func):
        self.server = server
        self.signatures = signatures
        self.func = func
        self.target = 'host'

    def target(self, target):
        self.target = target
        return self

    def __call__(self, *arguments):
        """Return the result of a remote JIT function call.

        Design
        ------

        1. Establish a connection to server and ask if it supports
        jitting the function for a particular target and if the
        function (with local ID) has been compiled in the server. If
        so, the server sends also any required parameters for
        processing the Python function to a IR representation or the
        remote ID of the function. Otherwise, raise an error.

        2. Compile Python function into IR string representation.

        3. Send the IR string together with signature information to
        server where it will be cached. As a response, recieve the
        remote ID of the function.

        4. Send the arguments together with the remote ID to server
        where the IR will be compiled to machine code (unless done
        eariler already), arguments are processed, and remote function
        will be called. The result will be returned in a response.
        """
        pass
