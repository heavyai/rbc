.. currentmodule:: rbc

*************
API Reference
*************

.. currentmodule:: rbc


RemoteJIT
----------

These methods are in the ``rbc.remotejit`` module namespace

.. currentmodule:: rbc.remotejit

.. autosummary:: 
  :toctree: generated

  Signature
  Caller
  RemoteJIT
  DispatcherRJIT
  LocalClient


OmnisciDB
----------

These methods are in the ``rbc.ominscidb`` module namespace

.. currentmodule:: rbc.omniscidb

.. autosummary::
  :toctree: generated

  get_client_config
  RemoteOmnisci


Omnisci Array
--------------

These methods are in the ``rbc.omnisci_array`` module namespace

.. currentmodule:: rbc.omnisci_array

.. autosummary::
  :toctree: generated

  ArrayPointer
  ArrayPointerModel
  omnisci_array_len
  omnisci_array_getitem
  array_type_converter

RemoteNumba
------------

These methods are in the ``utils.remotenumba`` module namespace

.. currentmodule:: utils.remotenumba

.. autosummary:: 
  :toctree: generated 

  initialize_llvm
  annotation_to_numba_type
  get_numba_signature
  get_llvm_ir
  get_triple
  create_execution_engine
  compile_ir
  compile 


TargetInfo
-----------

These methods are in the ``rbc.targetinfo`` module namespace

.. currentmodule:: rbc.targetinfo

.. autosummary:: 
  :toctree: generated 

  TargetInfo


Type System
------------

These methods are in the ``rbc.typesystem`` module namespace

.. currentmodule:: rbc.typesystem

.. autosummary::
  :toctree: generated
  
  make_numba_struct
  TypeParseError
  Type
  Complex64
  Complex128


IR Tools
---------

These methods are in the ``rbc.irtools`` module namespace

.. currentmodule:: rbc.irtools

.. autosummary::
  :toctree: generated
 
  get_function_dependencies
  JITRemoteCPUCodegen
  RemoteCPUContext
  compile_to_LLVM
  compile_IR


Thrift
-------

These methods are in the ``rbc.thrift`` module namespace

.. currentmodule:: rbc.thrift

.. autosummary::
  :toctree: generated
 
  Buffer
  Client
  Data
  Dispatcher
  NDArray
  Server


Utils
------

These methods are in the ``rbc.utils`` module namespace

.. currentmodule:: rbc.utils

.. autosummary::
  :toctree: generated
  
  get_local_ip
  is_localhost
  runcommand
  get_datamodel
  triple_split
  triple_matches
