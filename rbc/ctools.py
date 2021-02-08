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
    return -r.returncode, r.stdout.decode("utf-8")


class Compiler:
    """Compilers that can generate LLVM IR
    """

    def __init__(self, compiler_exe, flags, suffix, language, version):
        assert isinstance(compiler_exe, str)
        assert isinstance(flags, list)
        assert isinstance(suffix, str)
        assert isinstance(language, str)
        assert isinstance(version, tuple)
        self.compiler_exe = compiler_exe
        self.flags = flags
        self.suffix = suffix
        self.language = language
        self.version = version

    def version(self):
        s, o = run(self.compiler_exe, '--version')

    @staticmethod
    def compilers():
        """
        Iterator of compiler executables and supported languages
        """
        yield 'clang++', ['-c', '-O0', '-S', '-emit-llvm'], '.cpp', ['C++', 'C']
        yield 'clang', ['-c', '-O0', '-S', '-emit-llvm'], '.c', ['C']

    @classmethod
    def find(cls, language='C++', _cache={}):
        if language not in _cache:
            for c, flags, suffix, languages in cls.compilers():
                if language in languages:
                    exe = shutil.which(c)
                    if exe is not None:
                        s, o = run(exe, '--version')
                        m = re.search(r'(\d+[.]\d+[.]\d+)([^\s]*)', o)
                        if m is None:
                            warnings.warn(f'Failed to find version from output {o}')
                            continue
                        version = tuple(map(int, m.group(1).split('.'))) + (m.group(2),)
                        print(f'Found {language} compiler at {exe}, version={m.group(0)}')
                        _cache[language] = cls(exe, flags, suffix, language, version)
                        break
        return _cache.get(language)

    def compile(self, source, output, flags=[]):
        s, o = run(self.compiler_exe, self.flags, flags, source, '-o', output)
        if s == 0:
            f = open(output)
            out = f.read()
            f.close()
            return out
        print('COMPILATION OUTPUT:')
        print(o)
        raise RuntimeError('compilation failed')

    def __call__(self, source, flags=[]):
        f = tempfile.NamedTemporaryFile(mode='w', suffix=self.suffix, delete=False)
        f.write(source)
        f.close()
        fout = tempfile.NamedTemporaryFile(mode='r', suffix='.ll', delete=False)
        fout.close()
        try:
            out = self.compile(f.name, fout.name, flags=flags)
        finally:
            os.unlink(f.name)
            os.unlink(fout.name)
        # Eliminate clang inserted attributes that may cause problems
        # when server is parsing the LLVM IR string.
        out = out.replace(') #0 {', ') {')
        return out
