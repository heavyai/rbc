import warnings
from contextlib import contextmanager
from functools import partial

import llvmlite.binding as llvm
from numba import _dynfunc, njit
from numba.core import (base, callconv, codegen, compiler_lock, cpu,
                        descriptors, dispatcher, imputils, options, typing,
                        utils)
from numba.core.target_extension import (Generic, dispatcher_registry,
                                         jit_registry, target_registry)
from numba.cuda.target import CUDATypingContext
from numba.np import ufunc_db

from rbc.targetinfo import TargetInfo


class HeavyDB_CPU(Generic):
    """Mark the target as HeavyDB CPU
    """


class HeavyDB_GPU(Generic):
    """Mark the target as HeavyDB GPU
    """


target_registry['heavydb_cpu'] = HeavyDB_CPU
target_registry['heavydb_gpu'] = HeavyDB_GPU


def custom_jit(*args, target=None, **kwargs):
    assert 'target' not in kwargs
    assert '_target' not in kwargs
    return njit(*args, _target=target, **kwargs)


jit_registry[target_registry['heavydb_cpu']] = partial(custom_jit, target='heavydb_cpu')
jit_registry[target_registry['heavydb_gpu']] = partial(custom_jit, target='heavydb_gpu')


# heavydb_cpu_registry = imputils.Registry(name='heavydb_cpu_registry')
# heavydb_gpu_registry = imputils.Registry(name='heavydb_gpu_registry')


class _NestedContext(object):
    _typing_context = None
    _target_context = None

    @contextmanager
    def nested(self, typing_context, target_context):
        old_nested = self._typing_context, self._target_context
        try:
            self._typing_context = typing_context
            self._target_context = target_context
            yield
        finally:
            self._typing_context, self._target_context = old_nested


_options_mixin = options.include_default_options(
    "no_rewrites",
    "no_cpython_wrapper",
    "no_cfunc_wrapper",
    "fastmath",
    "inline",
    "boundscheck",
    "nopython",
    # Add "target_backend" as a accepted option for the CPU in @jit(...)
    "target_backend",
)


class HeavyDBTargetOptions(_options_mixin, options.TargetOptions):
    def finalize(self, flags, options):
        flags.enable_pyobject = False
        flags.enable_looplift = False
        flags.nrt = False
        flags.debuginfo = False
        flags.boundscheck = False
        flags.enable_pyobject_looplift = False
        flags.no_rewrites = True
        flags.auto_parallel = cpu.ParallelOptions(False)
        flags.inherit_if_not_set("fastmath")
        flags.inherit_if_not_set("error_model", default="python")
        # Add "target_backend" as a option that inherits from the caller
        flags.inherit_if_not_set("target_backend")


class HeavyDBTarget(descriptors.TargetDescriptor):
    options = HeavyDBTargetOptions
    _nested = _NestedContext()

    @utils.cached_property
    def _toplevel_target_context(self):
        # Lazily-initialized top-level target context, for all threads
        return JITRemoteTargetContext(self.typing_context, self._target_name)

    @utils.cached_property
    def _toplevel_typing_context(self):
        # Lazily-initialized top-level typing context, for all threads
        return {'heavydb_cpu': JITRemoteCPUTypingContext,
                'heavydb_gpu': JITRemoteGPUTypingContext}[self._target_name]()

    @property
    def target_context(self):
        """
        The target context for CPU/GPU targets.
        """
        nested = self._nested._target_context
        if nested is not None:
            return nested
        else:
            return self._toplevel_target_context

    @property
    def typing_context(self):
        """
        The typing context for CPU targets.
        """
        nested = self._nested._typing_context
        if nested is not None:
            return nested
        else:
            return self._toplevel_typing_context

    def nested_context(self, typing_context, target_context):
        """
        A context manager temporarily replacing the contexts with the
        given ones, for the current thread of execution.
        """
        return self._nested.nested(typing_context, target_context)


# Create a target instance
heavydb_cpu_target = HeavyDBTarget("heavydb_cpu")
heavydb_gpu_target = HeavyDBTarget("heavydb_gpu")


# Declare a dispatcher for the CPU/GPU targets
class HeavyDBCPUDispatcher(dispatcher.Dispatcher):
    targetdescr = heavydb_cpu_target


class HeavyDBGPUDispatcher(dispatcher.Dispatcher):
    targetdescr = heavydb_gpu_target


# Register a dispatcher for the target, a lot of the code uses this
# internally to work out what to do RE compilation
dispatcher_registry[target_registry["heavydb_cpu"]] = HeavyDBCPUDispatcher
dispatcher_registry[target_registry["heavydb_gpu"]] = HeavyDBGPUDispatcher


class JITRemoteCodeLibrary(codegen.JITCodeLibrary):
    """JITRemoteCodeLibrary was introduce to prevent numba from calling functions
    that checks if the module is final. See xnd-project/rbc issue #87.
    """

    def get_pointer_to_function(self, name):
        """We can return any random number here! This is just to prevent numba from
        trying to check if the symbol given by "name" is defined in the module.
        In cases were RBC is calling an external function (i.e. allocate_varlen_buffer)
        the symbol will not be defined in the module, resulting in an error.
        """
        return 0

    def _finalize_specific(self):
        """Same as codegen.JITCodeLibrary._finalize_specific but without
        calling _ensure_finalize at the end
        """
        self._codegen._scan_and_fix_unresolved_refs(self._final_module)


class JITRemoteCodegen(codegen.JITCPUCodegen):
    _library_class = JITRemoteCodeLibrary

    def _get_host_cpu_name(self):
        target_info = TargetInfo()
        return target_info.device_name

    def _get_host_cpu_features(self):
        target_info = TargetInfo()
        features = target_info.device_features
        server_llvm_version = target_info.llvm_version
        if server_llvm_version is None or target_info.is_gpu:
            return ''
        client_llvm_version = llvm.llvm_version_info

        # See https://github.com/xnd-project/rbc/issues/45
        remove_features = {
            (12, 12): [], (11, 11): [], (10, 10): [], (9, 9): [], (8, 8): [],
            (11, 8): ['tsxldtrk', 'amx-tile', 'amx-bf16', 'serialize', 'amx-int8',
                      'avx512vp2intersect', 'tsxldtrk', 'amx-tile', 'amx-bf16',
                      'serialize', 'amx-int8', 'avx512vp2intersect', 'tsxldtrk',
                      'amx-tile', 'amx-bf16', 'serialize', 'amx-int8',
                      'avx512vp2intersect', 'cx8', 'enqcmd', 'avx512bf16'],
            (11, 10): ['tsxldtrk', 'amx-tile', 'amx-bf16', 'serialize', 'amx-int8'],
            (9, 11): ['sse2', 'cx16', 'sahf', 'tbm', 'avx512ifma', 'sha',
                      'gfni', 'fma4', 'vpclmulqdq', 'prfchw', 'bmi2', 'cldemote',
                      'fsgsbase', 'ptwrite', 'xsavec', 'popcnt', 'mpx',
                      'avx512bitalg', 'movdiri', 'xsaves', 'avx512er',
                      'avx512vnni', 'avx512vpopcntdq', 'pconfig', 'clwb',
                      'avx512f', 'clzero', 'pku', 'mmx', 'lwp', 'rdpid', 'xop',
                      'rdseed', 'waitpkg', 'movdir64b', 'sse4a', 'avx512bw',
                      'clflushopt', 'xsave', 'avx512vbmi2', '64bit', 'avx512vl',
                      'invpcid', 'avx512cd', 'avx', 'vaes', 'cx8', 'fma', 'rtm',
                      'bmi', 'enqcmd', 'rdrnd', 'mwaitx', 'sse4.1', 'sse4.2', 'avx2',
                      'fxsr', 'wbnoinvd', 'sse', 'lzcnt', 'pclmul', 'prefetchwt1',
                      'f16c', 'ssse3', 'sgx', 'shstk', 'cmov', 'avx512vbmi',
                      'avx512bf16', 'movbe', 'xsaveopt', 'avx512dq', 'adx',
                      'avx512pf', 'sse3'],
            (9, 8): ['cx8', 'enqcmd', 'avx512bf16'],
        }.get((server_llvm_version[0], client_llvm_version[0]), [])
        if remove_features is None:
            warnings.warn(
                f'{type(self).__name__}._get_host_cpu_features: `remove_features` dictionary'
                ' requires an update: detected different LLVM versions in server '
                f'{server_llvm_version} and client {client_llvm_version}.'
                f' CPU features: {features}.')
        else:
            features += ','
            for f in remove_features:
                features = features.replace('+' + f + ',', '').replace('-' + f + ',', '')
            features.rstrip(',')
        return features

    def _customize_tm_options(self, options):
        super()._customize_tm_options(options)
        # fix reloc_model as the base method sets it using local target
        target_info = TargetInfo()
        if target_info.arch.startswith('x86'):
            reloc_model = 'static'
        else:
            reloc_model = 'default'
        options['reloc'] = reloc_model

    def set_env(self, env_name, env):
        return None


class JITRemoteCPUTypingContext(typing.Context):
    """JITRemote Typing Context
    """


class JITRemoteGPUTypingContext(CUDATypingContext):
    """JITRemote Typing Context
    """


class JITRemoteTargetContext(base.BaseContext):
    # Whether dynamic globals (CPU runtime addresses) is allowed
    allow_dynamic_globals = True  # should this be False?

    def __init__(self, typing_context, target):
        if target not in ('heavydb_cpu', 'heavydb_gpu'):
            raise ValueError(f'Target "{target}" not supported')
        super().__init__(typing_context, target)

    @compiler_lock.global_compiler_lock
    def init(self):
        target_info = TargetInfo()
        self.address_size = target_info.bits
        self.is32bit = (self.address_size == 32)
        self._internal_codegen = JITRemoteCodegen("numba.exec")
        self._target_data = llvm.create_target_data(target_info.datalayout)

    def refresh(self):
        # if self.target_name == 'heavydb_cpu':
        #     registry = heavydb_cpu_registry
        # else:
        #     registry = heavydb_gpu_registry

        # try:
        #     loader = self._registries[registry]
        # except KeyError:
        #     loader = imputils.RegistryLoader(registry)
        #     self._registries[registry] = loader

        # self.install_registry(registry)
        # Also refresh typing context, since @overload declarations can
        # affect it.
        # self.typing_context.refresh()
        super().refresh()

    def load_additional_registries(self):
        # Add implementations that work via import
        from numba.cpython import (builtins, charseq, enumimpl,  # noqa: F401
                                   hashing, heapq, iterators, listobj, numbers,
                                   rangeobj, setobj, slicing, tupleobj,
                                   unicode)

        self.install_registry(imputils.builtin_registry)

        # uncomment as needed!
        # from numba.core import optional
        # from numba.np import linalg, polynomial
        # from numba.typed import typeddict, dictimpl
        # from numba.typed import typedlist, listobject
        # from numba.experimental import jitclass, function_type
        # from numba.np import npdatetime
        # from numba.np import arraymath, arrayobj  # noqa: F401

        # from rbc.heavydb import mathimpl

        # Add target specific implementations
        from numba.cpython import mathimpl
        from numba.cuda import mathimpl as cuda_mathimpl
        from numba.np import npyimpl

        if self.target_name == 'heavydb_cpu':
            self.install_registry(npyimpl.registry)
            self.install_registry(mathimpl.registry)
        else:
            self.install_registry(cuda_mathimpl.registry)
        # from numba.cpython import cmathimpl, mathimpl, printimpl, randomimpl
        # from numba.misc import cffiimpl
        # from numba.experimental.jitclass.base import ClassBuilder as \
        #     jitclassimpl
        # self.install_registry(cmathimpl.registry)
        # self.install_registry(cffiimpl.registry)
        # self.install_registry(mathimpl.registry)
        # self.install_registry(printimpl.registry)
        # self.install_registry(randomimpl.registry)
        # self.install_registry(jitclassimpl.class_impl_registry)

    def codegen(self):
        return self._internal_codegen

    @utils.cached_property
    def call_conv(self):
        return callconv.CPUCallConv(self)

    @property
    def target_data(self):
        return self._target_data

    def create_cpython_wrapper(self,
                               library,
                               fndesc,
                               env,
                               call_helper,
                               release_gil=False):
        # There's no cpython wrapper on HeavyDB
        pass

    def create_cfunc_wrapper(self,
                             library,
                             fndesc,
                             env,
                             call_helper,
                             release_gil=False):
        # There's no cfunc wrapper on HeavyDB
        pass

    def get_executable(self, library, fndesc, env):
        """
        Returns
        -------
        (cfunc, fnptr)

        - cfunc
            callable function (Can be None)
        - fnptr
            callable function address
        - env
            an execution environment (from _dynfunc)
        """
        # although we don't use this function, it seems to be required
        # by some parts of codegen in Numba.

        # Code generation
        fnptr = library.get_pointer_to_function(
            fndesc.llvm_cpython_wrapper_name
        )

        # Note: we avoid reusing the original docstring to avoid encoding
        # issues on Python 2, see issue #1908
        doc = "compiled wrapper for %r" % (fndesc.qualname,)
        cfunc = _dynfunc.make_function(
            fndesc.lookup_module(),
            fndesc.qualname.rsplit(".", 1)[-1],
            doc,
            fnptr,
            env,
            # objects to keepalive with the function
            (library,),
        )
        library.codegen.set_env(self.get_env_name(fndesc), env)
        return cfunc

    def post_lowering(self, mod, library):
        pass

    # Overrides
    def get_ufunc_info(self, ufunc_key):
        return ufunc_db.get_ufunc_info(ufunc_key)
