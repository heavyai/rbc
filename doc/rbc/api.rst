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
