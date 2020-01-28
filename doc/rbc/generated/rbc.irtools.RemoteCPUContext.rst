rbc.irtools.RemoteCPUContext
============================

.. currentmodule:: rbc.irtools

.. autoclass:: RemoteCPUContext

   
   .. automethod:: __init__

   
   .. rubric:: Methods

   .. autosummary::
   
      ~RemoteCPUContext.__init__
      ~RemoteCPUContext.add_dynamic_addr
      ~RemoteCPUContext.add_linking_libs
      ~RemoteCPUContext.add_user_function
      ~RemoteCPUContext.build_list
      ~RemoteCPUContext.build_map
      ~RemoteCPUContext.build_set
      ~RemoteCPUContext.calc_array_sizeof
      ~RemoteCPUContext.call_external_function
      ~RemoteCPUContext.call_function_pointer
      ~RemoteCPUContext.call_internal
      ~RemoteCPUContext.call_unresolved
      ~RemoteCPUContext.cast
      ~RemoteCPUContext.codegen
      ~RemoteCPUContext.compile_internal
      ~RemoteCPUContext.compile_subroutine
      ~RemoteCPUContext.create_cpython_wrapper
      ~RemoteCPUContext.create_module
      ~RemoteCPUContext.debug_print
      ~RemoteCPUContext.declare_env_global
      ~RemoteCPUContext.declare_external_function
      ~RemoteCPUContext.declare_function
      ~RemoteCPUContext.generic_compare
      ~RemoteCPUContext.get_abi_alignment
      ~RemoteCPUContext.get_abi_sizeof
      ~RemoteCPUContext.get_arg_packer
      ~RemoteCPUContext.get_argument_type
      ~RemoteCPUContext.get_argument_value
      ~RemoteCPUContext.get_bound_function
      ~RemoteCPUContext.get_c_value
      ~RemoteCPUContext.get_constant
      ~RemoteCPUContext.get_constant_generic
      ~RemoteCPUContext.get_constant_null
      ~RemoteCPUContext.get_constant_undef
      ~RemoteCPUContext.get_data_as_value
      ~RemoteCPUContext.get_data_packer
      ~RemoteCPUContext.get_data_type
      ~RemoteCPUContext.get_dummy_type
      ~RemoteCPUContext.get_dummy_value
      ~RemoteCPUContext.get_env_body
      ~RemoteCPUContext.get_env_manager
      ~RemoteCPUContext.get_env_name
      ~RemoteCPUContext.get_executable
      ~RemoteCPUContext.get_external_function_type
      ~RemoteCPUContext.get_function
      ~RemoteCPUContext.get_function_pointer_type
      ~RemoteCPUContext.get_generator_desc
      ~RemoteCPUContext.get_generator_impl
      ~RemoteCPUContext.get_generator_state
      ~RemoteCPUContext.get_getattr
      ~RemoteCPUContext.get_helper_class
      ~RemoteCPUContext.get_preferred_array_alignment
      ~RemoteCPUContext.get_python_api
      ~RemoteCPUContext.get_return_type
      ~RemoteCPUContext.get_return_value
      ~RemoteCPUContext.get_returned_value
      ~RemoteCPUContext.get_setattr
      ~RemoteCPUContext.get_struct_type
      ~RemoteCPUContext.get_value_as_argument
      ~RemoteCPUContext.get_value_as_data
      ~RemoteCPUContext.get_value_type
      ~RemoteCPUContext.init
      ~RemoteCPUContext.insert_const_bytes
      ~RemoteCPUContext.insert_const_string
      ~RemoteCPUContext.insert_func_defn
      ~RemoteCPUContext.insert_generator
      ~RemoteCPUContext.insert_unique_const
      ~RemoteCPUContext.insert_user_function
      ~RemoteCPUContext.install_registry
      ~RemoteCPUContext.is_true
      ~RemoteCPUContext.load_additional_registries
      ~RemoteCPUContext.make_array
      ~RemoteCPUContext.make_complex
      ~RemoteCPUContext.make_constant_array
      ~RemoteCPUContext.make_data_helper
      ~RemoteCPUContext.make_helper
      ~RemoteCPUContext.make_optional_none
      ~RemoteCPUContext.make_optional_value
      ~RemoteCPUContext.make_tuple
      ~RemoteCPUContext.mangler
      ~RemoteCPUContext.pack_value
      ~RemoteCPUContext.pair_first
      ~RemoteCPUContext.pair_second
      ~RemoteCPUContext.populate_array
      ~RemoteCPUContext.post_lowering
      ~RemoteCPUContext.print_string
      ~RemoteCPUContext.printf
      ~RemoteCPUContext.push_code_library
      ~RemoteCPUContext.refresh
      ~RemoteCPUContext.remove_user_function
      ~RemoteCPUContext.sentry_record_alignment
      ~RemoteCPUContext.subtarget
      ~RemoteCPUContext.unpack_value
      ~RemoteCPUContext.with_aot_codegen
   
   

   
   
   .. rubric:: Attributes

   .. autosummary::
   
      ~RemoteCPUContext.active_code_library
      ~RemoteCPUContext.allow_dynamic_globals
      ~RemoteCPUContext.aot_mode
      ~RemoteCPUContext.auto_parallel
      ~RemoteCPUContext.call_conv
      ~RemoteCPUContext.enable_boundscheck
      ~RemoteCPUContext.enable_debuginfo
      ~RemoteCPUContext.enable_nrt
      ~RemoteCPUContext.environment
      ~RemoteCPUContext.error_model
      ~RemoteCPUContext.fastmath
      ~RemoteCPUContext.fndesc
      ~RemoteCPUContext.implement_pow_as_math_call
      ~RemoteCPUContext.implement_powi_as_math_call
      ~RemoteCPUContext.nrt
      ~RemoteCPUContext.strict_alignment
      ~RemoteCPUContext.target_data
   
   