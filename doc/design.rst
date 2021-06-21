.. meta::
   :robots: index,follow
   :description: rbc documentation

.. sectionauthor:: Pearu Peterson <pearu.peterson at quansight.com>

.. default-domain:: python

*RBC* design principles
=======================

The basic idea of the *Remote Backend Compiler* is that the frontend
and backend components of a compiler run on two different hosts.

The input to the compiler frontend is a program source code and the
output is so-called Intermediate Representation (IR) that consists of
instructions that do not depend on the programming language used in
the program source code. Therefore, the IR form of the program code is
suitable for applying any optimization transformations prior
converting it to a machine code. The frontend component of a compiler
is run on a client machine.

The input to the compiler backend is the IR representation of the
program source code and the output is the machine code consisting of
instructions that a target processing unit (CPU or some accelerator
device such as GPU) will be executing.  The backend component of a
compiler is run on a server machine.

By IR we refer to LLVM IR or some of its clones (such as NVVM IR).
The frontend of the compiler needs to know what will be the target
processing unit. Hence, as a first step, the client must make a query
to the server for retrieving the information about the available
targets.

The backend of the compiler is built in to the server application
program that will compile the program in IR form recieved from the
client to machine code which will be loaded to server memory so that
the server process can execute the newly loaded programs. All this is
carried out in server application runtime.

The two tasks of compiling the client programs in IR form and
executing the loaded machine codes are considered as separate
tasks. In principle, there could be many clients: some clients submit
new programs to the server that will compile and load these to server
memory, and other clients make RPC calls to these new programs.

The following diagram summarizes the basic workflow of a RBC usage::

  +-------------------------------------------+
  | Client 1                                  |
  +-------------------------------------------+
  | 1. define a task program:                 |
  |         int foo(int a) {...}              |
  | 2. request target information from Server |
  | 3. compile the program to IR module:      |
  |         i32 %foo(i32 %a) {...}            |
  | 4. send the IR module to Server           |
  +-------------------------------------------+
  
                +---------------------------------------------------+
                | Server                                            |
                +---------------------------------------------------+
                | 5.  recieve the IR module from Client 1           |
                | 6.  merge the IR module to a service IR module    |
                | 7.  compile the service IR module to machine code |
                | 8.  load the service machine code to memory       |
                | 9.  wait for clients tasks                        |
                |     ...                                           |
                | 12. recieve task parameters from Client 2         |
                |     and execute service instructions              |
                | 13. respond to Client 2 with service results      |
                +---------------------------------------------------+
  
         +------------------------------------------+
         | Client 2                                 |
         +------------------------------------------+
         | 10. define task parameter `a`            |
         | 11. send task (via RPC) to Server        |
         |     ...                                  |
         | 14. recieve service results from Server  |
         | 15. process the results..                |
         +------------------------------------------+

RBC prototype implementation
----------------------------
         
To implement a prototype of a RBC application, we'll use Python
language. The client task functions are Python functions. For LLVM IR
and machine code generation, we'll use numba and llvmlite Python
packages. For RPC, we'll use thriftpy2 package.

Example usage
-------------

First, we'll run the Server:

.. code-block:: python

   # File: rbc_server.py
   import rbc

   class Dispatcher(rbc.thrift.Dispatcher):

       def compile(self, task_prototype, task_ir):
           ...
           self.engine = ...

       def call(self, task_name, task_arguments):
           task_ptr = self.engine.get_function_address(task_name)
           return self.apply(task_ptr, task_arguments)
           
   rbc.thrift.Server.run(Dispatcher,
                         '/path/to/rbc.thrift',
                         host=..., port=...).run()

Next, for simplicity, we implement Client 1 and Client 2
functionalities in one Python script:

.. code-block:: python

   # File: rbc_client.py

   import rbc

   # Client 1 functionality:
   
   @rbc.jit(host=..., port=...)
   def foo(a : int) -> int:
       return a + 1

   # Client 2 functionality:

   a = 5
   r = foo(a)

   # or
   
   foo = rbc.connect('foo', host=..., port=...)
   a = 5
   r = foo(a)

   print(f'foo({a}) -> {r}')
