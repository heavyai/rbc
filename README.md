# RBC - Remote Backend Compiler

[![GitHub Actions](https://github.com/xnd-project/rbc/actions/workflows/rbc_test.yml/badge.svg)](https://github.com/xnd-project/rbc/actions) [![Documentation Status](https://readthedocs.org/projects/rbc/badge/?version=latest)](https://rbc.readthedocs.io/en/latest/?badge=latest)

## Introduction - test

A [LLVM](http://www.llvm.org/)-based compilation contains three components: frontend, optimizer, backend. The frontend parses source code and produces an abstract syntax tree \(AST\) that is specific to the used programming language. The AST is translated to an intermediate representation \(IR\) that contains language independent instructions \(in the case of LLVM these are [SSA](https://en.wikipedia.org/wiki/Static_single_assignment_form) instructions\). One can apply various optimizations \(e.g. eliminating of redundant instructions, symbolic transformations, etc\) to the program in IR form. Finally, the backend will transform the IR to machine code \(represented in [asm](https://en.wikipedia.org/wiki/Assembly_language) language, for instance\) that will be specific to the computer architecture on which the program instructions will be executed. The following schema summarizes this compilation process:

```text
       +----------+       +-----------+       +----------+
       | Frontend |------>| Optimizer |------>| Backend  |
       +----------+       +-----------+       +----------+
       /                                              \
      /    AST                                   ASM   \
     /                                                  \
source code      initial IR        transformed IR    machine code
```

Usually, the transformation of a computer program from its source code to machine code is carried out as a single compilation step on a host computer with a compiler. Programming language interpreters may also use JIT compilers where the source code-&gt;machine code transformation is executed in runtime \(but again, on the host computer\).

The aim of the _Remote Backend Compiler_ \(_RBC_\) project is to distribute the tasks of a program JIT compilation process to separate computer systems using the client-server model. The frontend of the compiler would run on the client and the backend would run on the server. The client \(compiler frontend\) will send the program code to server \(compiler backend\) in IR form. So, the optimizer can run either in client or server.

The RBC model may be advantageous in applications where a user program \(running on a client computer\) carries out computations on data that is stored on a server computer while the size of the data would be so large that copying it to client computer would not be feasible: the client computer does not have enough RAM, network bandwidth is too small, server computer contains accelerator hardware, and so on.

The prototype of a RBC model is implemented in Python using [Numba](https://numba.pydata.org/) and [llvmlite](http://llvmlite.pydata.org/en/latest/) tools.

## Installation

### conda

```text
conda install -c conda-forge rbc
```

See [RBC conda-forge package](https://github.com/conda-forge/rbc-feedstock#about-rbc) for more information.

### pip

```text
pip install rbc-project
```

## Testing

```text
pytest -sv --pyargs rbc
```

## Usage

### Case 1: Compile and evaluate a function in a background process

```text
from rbc import RemoteJIT
rjit = RemoteJIT()
rjit.start_server(background=True)

@rjit('double(double, double)', 'int(int, int)')
def add(x, y):
    return x + y

assert add(1, 2) == 3
assert add(1.2, 2.3) == 3.5

rjit.stop_server()
```

### Case 2: Compile and evaluate a function in a remote server

In remote server \(here using localhost for example\), start the RemoteJIT process

```text
import rbc
rjit = rbc.RemoteJIT(host='localhost', port=23678)
rjit.start_server()  # this will run the server loop
```

In a client computer, use the remote servers RemoteJIT process for compilation and evaluation:

```text
from rbc import RemoteJIT
rjit = RemoteJIT(host='localhost', port=23678)
@rjit('double(double, double)', 'int(int, int)')
def add(x, y):
    return x + y
assert add(1, 2) == 3
assert add(1.2, 2.3) == 3.5
```

### Case 3: Define User Defined Function \(UDF\) in Python and register it in HeavyDB server

Assume that HeavyDB server is running in localhost, for instance.

In a client computer, register a UDF in HeavyDB server:

```text
from rbc.heavydb import RemoteHeavyDB
omni = RemoteHeavyDB()
@omni('i32(i32)')
def incr(x):
    return x + 1
omni.register()
```

In a client computer, use the UDF in a query to HeavyDB server:

```text
import ibis
con = ibis.heavydb.connect(...)
q = con.sql('select i, incr(i) from mytable').execute()
```

### More examples and usage cases

See [notebooks](https://github.com/xnd-project/rbc/tree/master/notebooks).

