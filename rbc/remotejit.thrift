include "thrift/info.thrift"
include "thrift/types.thrift"

service remotejit {
    map<string, string> targets() throws (1: Exception e),
    bool compile(1: string name, 2: string signatures, 3: string ir) throws (1: Exception e),
    Data call(1: string fullname, 2: Data arguments) throws (1: Exception e),
}
