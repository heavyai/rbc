from contextlib import contextmanager
import llvmlite.binding as llvm
from rbc.targetinfo import TargetInfo
from numba.np import ufunc_db
from numba import _dynfunc
from numba.core import (
    codegen, compiler_lock, typing,
    base, cpu, utils, descriptors,
    dispatcher, callconv, imputils,
    options,)
from numba.core.target_extension import (
    Generic,
    target_registry,
    dispatcher_registry,
)


class OmniSciDB_CPU(Generic):
    """Mark the target as OmniSciDB CPU
    """


class OmniSciDB_GPU(Generic):
    """Mark the target as OmniSciDB GPU
    """


target_registry['omniscidb_cpu'] = OmniSciDB_CPU
target_registry['omniscidb_gpu'] = OmniSciDB_GPU

omnisci_cpu_registry = imputils.Registry(name='omnisci_cpu_registry')
omnisci_gpu_registry = imputils.Registry(name='omnisci_gpu_registry')


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


class OmnisciTargetOptions(_options_mixin, options.TargetOptions):
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


class OmnisciTarget(descriptors.TargetDescriptor):
    options = OmnisciTargetOptions
    _nested = _NestedContext()

    @utils.cached_property
    def _toplevel_target_context(self):
        # Lazily-initialized top-level target context, for all threads
        return JITRemoteTargetContext(self.typing_context, self._target_name)

    @utils.cached_property
    def _toplevel_typing_context(self):
        # Lazily-initialized top-level typing context, for all threads
        return JITRemoteTypingContext()

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
omniscidb_cpu_target = OmnisciTarget("omniscidb_cpu")
omniscidb_gpu_target = OmnisciTarget("omniscidb_gpu")


# Declare a dispatcher for the CPU/GPU targets
class OmnisciCPUDispatcher(dispatcher.Dispatcher):
    targetdescr = omniscidb_cpu_target


class OmnisciGPUDispatcher(dispatcher.Dispatcher):
    targetdescr = omniscidb_gpu_target


# Register a dispatcher for the target, a lot of the code uses this
# internally to work out what to do RE compilation
dispatcher_registry[target_registry["omniscidb_cpu"]] = OmnisciCPUDispatcher
dispatcher_registry[target_registry["omniscidb_gpu"]] = OmnisciGPUDispatcher


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
            (11, 8): ['tsxldtrk', 'amx-tile', 'amx-bf16', 'serialize', 'amx-int8',
                      'avx512vp2intersect', 'tsxldtrk', 'amx-tile', 'amx-bf16',
                      'serialize', 'amx-int8', 'avx512vp2intersect', 'tsxldtrk',
                      'amx-tile', 'amx-bf16', 'serialize', 'amx-int8',
                      'avx512vp2intersect', 'cx8', 'enqcmd', 'avx512bf16'],
            (11, 10): ['tsxldtrk', 'amx-tile', 'amx-bf16', 'serialize', 'amx-int8'],
            (9, 8): ['cx8', 'enqcmd', 'avx512bf16'],
        }.get((server_llvm_version[0], client_llvm_version[0]), [])
        for f in remove_features:
            features = features.replace('+' + f, '').replace('-' + f, '')
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


class JITRemoteTypingContext(typing.Context):
    """JITRemote Typing Context
    """

    def load_additional_registries(self):
        from rbc.omnisci_backend import mathimpl
        self.install_registry(mathimpl.registry)
        return super().load_additional_registries()


class JITRemoteTargetContext(base.BaseContext):
    # Whether dynamic globals (CPU runtime addresses) is allowed
    allow_dynamic_globals = True

    def __init__(self, typing_context, target):
        if target not in ('omniscidb_cpu', 'omniscidb_gpu'):
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
        if self.target_name == 'omniscidb_cpu':
            registry = omnisci_cpu_registry
        else:
            registry = omnisci_gpu_registry

        try:
            loader = self._registries[registry]
        except KeyError:
            loader = imputils.RegistryLoader(registry)
            self._registries[registry] = loader

        self.install_registry(registry)
        # Also refresh typing context, since @overload declarations can
        # affect it.
        self.typing_context.refresh()
        super().refresh()

    def load_additional_registries(self):
        # Add implementations that work via import
        from numba.cpython import (builtins, charseq, enumimpl, hashing, heapq,  # noqa: F401
                                   iterators, listobj, numbers, rangeobj,
                                   setobj, slicing, tupleobj, unicode,)

        self.install_registry(imputils.builtin_registry)

        # uncomment as needed!
        # from numba.core import optional
        # from numba.np import linalg, polynomial, arraymath, arrayobj  # noqa: F401
        # from numba.typed import typeddict, dictimpl
        # from numba.typed import typedlist, listobject
        # from numba.experimental import jitclass, function_type
        # from numba.np import npdatetime

        # Add target specific implementations
        from numba.np import npyimpl
        from numba.cpython import mathimpl
        # from numba.cpython import cmathimpl, mathimpl, printimpl, randomimpl
        # from numba.misc import cffiimpl
        # from numba.experimental.jitclass.base import ClassBuilder as \
        #     jitclassimpl
        # self.install_registry(cmathimpl.registry)
        # self.install_registry(cffiimpl.registry)
        self.install_registry(mathimpl.registry)
        self.install_registry(npyimpl.registry)
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
        # There's no cpython wrapper on omniscidb
        pass

    def create_cfunc_wrapper(self,
                             library,
                             fndesc,
                             env,
                             call_helper,
                             release_gil=False):
        # There's no cfunc wrapper on omniscidb
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
            fndesc.qualname.split(".")[-1],
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
