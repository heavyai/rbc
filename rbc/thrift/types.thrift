

enum ExceptionKind {
    EXC_MESSAGE,    // plain error message
    EXC_TBLIB,      // tblib pickled sys.exc_info()
}

exception Exception {
    1: ExceptionKind kind;
    2: string message;
}

exception PythonException {
    1: string type;
    2: string message;
    3: list<string> arguments;
    4: string traceback;
}

struct Buffer {
    1: binary data;
}

struct NDArray {
    1: list<i32> shape;
    2: string typestr;
    3: binary data;
}

