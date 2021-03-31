JITRemoteTargetContext
======================

.. currentmodule:: rbc.irtools

.. autoclass:: JITRemoteTargetContext

   
   
   .. rubric:: Attributes
   .. autosummary::
      :toctree:
   
      JITRemoteTargetContext.active_code_library
   
      JITRemoteTargetContext.allow_dynamic_globals
   
      JITRemoteTargetContext.aot_mode
   
      JITRemoteTargetContext.auto_parallel
   
      JITRemoteTargetContext.call_conv
   
      JITRemoteTargetContext.enable_boundscheck
   
      JITRemoteTargetContext.enable_debuginfo
   
      JITRemoteTargetContext.enable_nrt
   
      JITRemoteTargetContext.environment
   
      JITRemoteTargetContext.error_model
   
      JITRemoteTargetContext.fastmath
   
      JITRemoteTargetContext.fndesc
   
      JITRemoteTargetContext.implement_pow_as_math_call
   
      JITRemoteTargetContext.implement_powi_as_math_call
   
      JITRemoteTargetContext.nonconst_module_attrs
   
      JITRemoteTargetContext.nrt
   
      JITRemoteTargetContext.strict_alignment
   
      JITRemoteTargetContext.target_data
   
   
   

   
   
   .. rubric:: Methods
   .. autosummary::
      :toctree:
   
      JITRemoteTargetContext.__init__
   
      JITRemoteTargetContext.add_dynamic_addr
   
      JITRemoteTargetContext.add_linking_libs
   
      JITRemoteTargetContext.add_user_function
   
      JITRemoteTargetContext.build_list
   
      JITRemoteTargetContext.build_map
   
      JITRemoteTargetContext.build_set
   
      JITRemoteTargetContext.calc_array_sizeof
   
      JITRemoteTargetContext.call_external_function
   
      JITRemoteTargetContext.call_function_pointer
   
      JITRemoteTargetContext.call_internal
   
      JITRemoteTargetContext.call_internal_no_propagate
   
      JITRemoteTargetContext.call_unresolved
   
      JITRemoteTargetContext.cast
   
      JITRemoteTargetContext.codegen
   
      JITRemoteTargetContext.compile_internal
   
      JITRemoteTargetContext.compile_subroutine
   
      JITRemoteTargetContext.create_cfunc_wrapper
   
      JITRemoteTargetContext.create_cpython_wrapper
   
      JITRemoteTargetContext.create_module
   
      JITRemoteTargetContext.debug_print
   
      JITRemoteTargetContext.declare_env_global
   
      JITRemoteTargetContext.declare_external_function
   
      JITRemoteTargetContext.declare_function
   
      JITRemoteTargetContext.generic_compare
   
      JITRemoteTargetContext.get_abi_alignment
   
      JITRemoteTargetContext.get_abi_sizeof
   
      JITRemoteTargetContext.get_arg_packer
   
      JITRemoteTargetContext.get_argument_type
   
      JITRemoteTargetContext.get_argument_value
   
      JITRemoteTargetContext.get_bound_function
   
      JITRemoteTargetContext.get_c_value
   
      JITRemoteTargetContext.get_constant
   
      JITRemoteTargetContext.get_constant_generic
   
      JITRemoteTargetContext.get_constant_null
   
      JITRemoteTargetContext.get_constant_undef
   
      JITRemoteTargetContext.get_data_as_value
   
      JITRemoteTargetContext.get_data_packer
   
      JITRemoteTargetContext.get_data_type
   
      JITRemoteTargetContext.get_dummy_type
   
      JITRemoteTargetContext.get_dummy_value
   
      JITRemoteTargetContext.get_env_body
   
      JITRemoteTargetContext.get_env_manager
   
      JITRemoteTargetContext.get_env_name
   
      JITRemoteTargetContext.get_executable
   
      JITRemoteTargetContext.get_external_function_type
   
      JITRemoteTargetContext.get_function
   
      JITRemoteTargetContext.get_function_pointer_type
   
      JITRemoteTargetContext.get_generator_desc
   
      JITRemoteTargetContext.get_generator_impl
   
      JITRemoteTargetContext.get_generator_state
   
      JITRemoteTargetContext.get_getattr
   
      JITRemoteTargetContext.get_helper_class
   
      JITRemoteTargetContext.get_preferred_array_alignment
   
      JITRemoteTargetContext.get_python_api
   
      JITRemoteTargetContext.get_return_type
   
      JITRemoteTargetContext.get_return_value
   
      JITRemoteTargetContext.get_returned_value
   
      JITRemoteTargetContext.get_setattr
   
      JITRemoteTargetContext.get_struct_type
   
      JITRemoteTargetContext.get_value_as_argument
   
      JITRemoteTargetContext.get_value_as_data
   
      JITRemoteTargetContext.get_value_type
   
      JITRemoteTargetContext.init
   
      JITRemoteTargetContext.insert_const_bytes
   
      JITRemoteTargetContext.insert_const_string
   
      JITRemoteTargetContext.insert_func_defn
   
      JITRemoteTargetContext.insert_generator
   
      JITRemoteTargetContext.insert_unique_const
   
      JITRemoteTargetContext.insert_user_function
   
      JITRemoteTargetContext.install_registry
   
      JITRemoteTargetContext.is_true
   
      JITRemoteTargetContext.load_additional_registries
   
      JITRemoteTargetContext.make_array
   
      JITRemoteTargetContext.make_complex
   
      JITRemoteTargetContext.make_constant_array
   
      JITRemoteTargetContext.make_data_helper
   
      JITRemoteTargetContext.make_helper
   
      JITRemoteTargetContext.make_optional_none
   
      JITRemoteTargetContext.make_optional_value
   
      JITRemoteTargetContext.make_tuple
   
      JITRemoteTargetContext.mangler
   
      JITRemoteTargetContext.pack_value
   
      JITRemoteTargetContext.pair_first
   
      JITRemoteTargetContext.pair_second
   
      JITRemoteTargetContext.populate_array
   
      JITRemoteTargetContext.post_lowering
   
      JITRemoteTargetContext.print_string
   
      JITRemoteTargetContext.printf
   
      JITRemoteTargetContext.push_code_library
   
      JITRemoteTargetContext.refresh
   
      JITRemoteTargetContext.remove_user_function
   
      JITRemoteTargetContext.sentry_record_alignment
   
      JITRemoteTargetContext.subtarget
   
      JITRemoteTargetContext.unpack_value
   
      JITRemoteTargetContext.with_aot_codegen
   
   
   