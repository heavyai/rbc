__all__ = ['Compiler']

import os
import re
import tempfile
import subprocess
import shutil
import warnings


def run(cmd, *args):
    new_args = [cmd]
    for a in args:
        if isinstance(a, str):
            new_args.append(a)
        elif isinstance(a, (tuple, list)):
            new_args.extend(a)
        else:
            raise TypeError(f'expected str|list|tuple argument, got {type(a)}')
    r = subprocess.run(new_args, capture_output=True)
    return -r.returncode, r.stdout.decode("utf-8") + '\n' + r.stderr.decode("utf-8")


class Compiler:
    """Compilers that can generate LLVM IR
    """

    def __init__(self, compiler_exe, flags, suffix, version):
        assert isinstance(compiler_exe, str)
        assert isinstance(flags, list)
        assert isinstance(suffix, str)
        assert isinstance(version, tuple)
        self.compiler_exe = compiler_exe
        self.flags = flags
        self.suffix = suffix
        self.version = version

    def __repr__(self):
        return (f'{type(self).__name__}({self.compiler_exe!r}, {self.flags!r},'
                f' {self.suffix!r}, {self.version!r})')

    @staticmethod
    def _get_compilers(_cache=[]):
        if not _cache:
            # TODO: cuda support
            for xmode, clang, suffix in [('c++', 'clang++', '.cpp'), ('c', 'clang', '.c')]:
                exe = shutil.which(clang)
                if exe is not None:
                    s, o = run(exe, '--version')
                    m = re.search(r'(\d+[.]\d+[.]\d+)([^\s]*)', o)
                    if m is None:
                        warnings.warn(f'Failed to find version from output {o}')
                        continue
                    version = tuple(map(int, m.group(1).split('.'))) + (m.group(2),)
                    lang_stds = []
                    s, o = run(exe, '-x', xmode, '-std=', '-E', '-')
                    for line in o.splitlines():
                        if line.startswith('note: use'):
                            lang_stds.extend(re.findall(r"[']([\w\d][\w\d:+]*)[']",
                                                        line.split('for', 1)[0]))
                    flags = ['-x', xmode]
                    _cache.append((exe, version, lang_stds, flags, suffix))
                else:
                    _cache.append(None)

        for data in _cache:
            if data is not None:
                yield data

    @classmethod
    def get(cls, std='C++'):
        """Construct a compiler instance using the desired language standard.

        Returns None when no compiler is found in the
        environment. Otherwise, return Compiler instance with flags
        supporting the desired language standard.
        """
        std = std.lower()
        std = {'c': 'c11', 'c++': 'c++11', 'cxx': 'c++11'}.get(std, std)
        flags = ['-c', '-O0', '-S', '-emit-llvm']
        all_stds = []
        for exe, version, lang_stds, c_flags, suffix in cls._get_compilers():
            all_stds.extend(lang_stds)
            if std in lang_stds:
                return cls(exe, c_flags + ['-std='+std] + flags, suffix, version)
        if all_stds:
            raise ValueError(f'Language standard {std} not supported.'
                             f' Supported standards: {", ".join(all_stds)}')
        return None

    def _compile(self, source, output, flags=[]):
        s, o = run(self.compiler_exe, self.flags, flags, source, '-o', output)
        if s == 0:
            f = open(output)
            out = f.read()
            f.close()
            return out
        raise RuntimeError(f'compilation failed:\n{o}')

    def __call__(self, source, flags=[]):
        """Compile the source code and return LLVM IR string.
        """
        f = tempfile.NamedTemporaryFile(mode='w', suffix=self.suffix, delete=False)
        f.write(source)
        f.close()
        fout = tempfile.NamedTemporaryFile(mode='r', suffix='.ll', delete=False)
        fout.close()
        try:
            out = self._compile(f.name, fout.name, flags=flags)
        finally:
            os.unlink(f.name)
            os.unlink(fout.name)
        # Eliminate clang inserted attributes that may cause problems
        # when server is parsing the LLVM IR string.
        # TODO: check if the attributes can be removed in irtools
        out = out.replace(') #0 {', ') {')
        return out
