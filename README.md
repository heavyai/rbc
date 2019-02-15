# RBC - Remote Backend Compiler

## Introduction

A [LLVM](http://www.llvm.org/)-based compilation contains three
components: frontend, optimizer, backend. The frontend parses source
code and produces an abstract syntax tree (AST) that is specific to
the used programming language. The AST is translated to an
intermediate representation (IR) that contains language independent
instructions (in the case of LLVM these are
[SSA](https://en.wikipedia.org/wiki/Static_single_assignment_form)
instructions). One can apply various optimizations (e.g. eliminating
of redundant instructions, symbolic transformations, etc) to the
program in IR form. Finally, the backend will transform the IR to machine
code (represented in
[asm](https://en.wikipedia.org/wiki/Assembly_language) language, for instance)
that will be specific to the computer architecture on which the program
instructions will be executed. The following schema summarizes this
compilation process:
```
       +----------+       +-----------+       +----------+
       | Frontend |------>| Optimizer |------>| Backend  |
       +----------+       +-----------+       +----------+
       /                                              \
      /    AST                                   ASM   \
     /                                                  \
source code      initial IR        transformed IR    machine code
```

Usually, the transformation of a computer program from its source code
to machine code is carried out as a single compilation step on a host
computer with a compiler. Programming language interpreters may also
use JIT compilers where the source code->machine code transformation
is executed in runtime (but again, on the host computer).

The aim of the *Remote Backend Compiler* (*RBC*) project is to
distribute the tasks of a program JIT compilation process to separate
computer systems using the client-server model. The frontend of the
compiler would run on the client and the backend would run on the
server. The client (compiler frontend) will send the program code to
server (compiler backend) in IR form. So, the optimizer can run either
in client or server.

The RBC model may be advantageous in applications where a user program
(running on a client computer) carries out computations on data that
is stored on a server computer while the size of the data would be so
large that copying it to client computer would not be feasible: the
client computer does not have enough RAM, network bandwidth is too
small, server computer contains accelerator hardware, and so on.

The prototype of a RBC model is implemented in Python using
[Numba](https://numba.pydata.org/) and
[llvmlite](http://llvmlite.pydata.org/en/latest/) tools.

