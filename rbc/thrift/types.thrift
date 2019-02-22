

enum ExceptionKind {
    EXC_MESSAGE,    // plain error message
    EXC_TBLIB,      // tblib pickled sys.exc_info()
}

exception Exception {
    1: ExceptionKind kind;
    2: string message;
}

enum DataKind {
    DATA_RAW,       // data contains a raw data as bytes
    DATA_ENCODED,   // data contains encoded string
    DATA_PICKLED,    // data contains a Python pickled object
}

struct Data {
    1: DataKind kind;
    2: string info;  // any information needed for interpretting the data with given kind
    3: binary data;
}

struct Buffer {
    1: binary data;
}

struct NDArray {
    1: list<i32> shape;
    2: string typestr;
    3: binary data;
}
