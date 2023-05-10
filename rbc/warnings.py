
class LLVMVersionMismatchWarning(UserWarning):
    """
    Raised when Numba and HeavyDB uses different LLVM version which is known to
    be incompatible/problematic.
    """
    pass
