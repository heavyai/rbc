from contextlib import contextmanager
import llvmlite.binding as llvm
from rbc.targetinfo import TargetInfo
from numba.np import ufunc_db
from numba.core import (
    codegen, compiler_lock, typing,
    base, cpu, utils, registry,
    dispatcher, callconv, imputils,)
from numba.core.target_extension import (
    Generic,
    target_registry,
    dispatcher_registry,
)


class OmniSciDB_CPU(Generic):
    """Mark the target as OmniSciDB CPU
    """

class OmniSciDB_GPU(Generic):
    """Mark the target as OmniSciDB CPU
    """


target_registry['omniscidb_cpu'] = OmniSciDB_CPU
target_registry['omniscidb_gpu'] = OmniSciDB_GPU

omnisci_generic_registry = imputils.Registry()
omnisci_cpu_registry = imputils.Registry()
omnisci_gpu_registry = imputils.Registry()


class OmnisciTarget(registry.CPUTarget):
    # Create a target for the omniscidb backend
    pass

# Create a target instance
omniscidb_target = OmnisciTarget("omniscidb_target")

# Declare a dispatcher for the DPU target
class OmnisciDispatcher(dispatcher.Dispatcher):
    targetdescr =omniscidb_target

# Register a dispatcher for the target, a lot of the code uses this
# internally to work out what to do RE compilation
dispatcher_registry[target_registry["omniscidb_cpu"]] = OmnisciDispatcher
dispatcher_registry[target_registry["omniscidb_gpu"]] = OmnisciDispatcher


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
        elif self.target_name == 'omniscidb_gpu':
            registry = omnisci_gpu_registry
        else:
            raise ValueError(f'Unknown target {self.target_name}')
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
        return None

    def post_lowering(self, mod, library):
        pass

    # Overrides
    def get_ufunc_info(self, ufunc_key):
        return ufunc_db.get_ufunc_info(ufunc_key)
