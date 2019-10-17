include "../thrift/types.thrift"

enum MyEnum {
  MyONE,
  MyTWO
}

service test {
    string thrift_content(),
    Buffer test_buffer_transport(1: Buffer buf),
    NDArray test_ndarray_transport(1: NDArray arr),
    Data test_ndarray_transport2(1: Data arr),
    Data test_Data_transport(1: Data data),
    Data test_Data_transport2(1: Data data),
    string test_str_transport(1: string s),
    byte test_byte_transport(1: byte s),
    i16 test_int16_transport(1: i16 s),
    i32 test_int32_transport(1: i32 s),
    i64 test_int64_transport(1: i64 s),
    i64 test_int64_transport2(1: i64 s),
    bool test_bool_transport(1: bool s),
    double test_double_transport(1: double s),
    map<i32,string> test_map_transport(1: map<i32,string> s),
    set<string> test_set_transport(1: set<string> s),
    list<i32> test_list_transport(1: list<i32> s),
    void test_void(),
    void test_exception() throws (1: Exception e),
    MyEnum test_myenum_transport(1: MyEnum s),
}