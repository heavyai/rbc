__all__ = ['has_ccompiler', 'ccompile']

import os
import tempfile


def has_ccompiler(_cache=[]):
    """Check for the availability of clang compiler.
    """
    if not _cache:
        cmd = 'clang --version'
        s = os.system(cmd)
        _cache.append(s == 0)
    return _cache[0]


def ccompile(src, include_dirs=[]):
    """Compile C source code to LLVM IR module.

    Parameters
    ----------
    src : str
      C source code as text.
    include_dirs: list
      Include directories

    Returns
    -------
    llvmir : str
      LLVM IR as text.
    """
    f = tempfile.NamedTemporaryFile(mode='w', suffix='.c', delete=False)
    fout = tempfile.NamedTemporaryFile(mode='r', suffix='.ll', delete=False)
    fout.close()
    f.write(src)
    f.close()
    cmd = f'clang -O0 -S -emit-llvm {" -I".join([""] + include_dirs)} {f.name} -o {fout.name}'
    s = os.system(cmd)
    assert s == 0, cmd
    os.unlink(f.name)

    f = open(fout.name)
    llvm_ir = f.read()
    f.close()
    os.unlink(fout.name)

    return llvm_ir
