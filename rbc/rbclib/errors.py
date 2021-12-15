class DebugAllocatorError(Exception):
    pass


class InvalidFreeError(DebugAllocatorError):
    pass


class MemoryLeakError(DebugAllocatorError):

    def __init__(self, leaks):
        lines = [f'Found {len(leaks)} memory leaks:']
        for addr, seq in leaks:
            lines.append(f'    {addr} (seq = {seq})')
        message = '\n'.join(lines)
        super().__init__(message)
        self.leaks = leaks
