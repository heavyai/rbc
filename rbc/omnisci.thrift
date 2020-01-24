/*
  Stripped version of mapd.thrift
*/
include "common.thrift"
include "extension_functions.thrift"
typedef string TSessionId

exception TMapDException {
  1: string error_msg
}

/* union */ struct TDatumVal {
  1: i64 int_val,
  2: double real_val,
  3: string str_val,
  4: list<TDatum> arr_val
}

struct TDatum {
  1: TDatumVal val,
  2: bool is_null
}

struct TColumnType {
  1: string col_name,
  2: common.TTypeInfo col_type,
  3: bool is_reserved_keyword,
  4: string src_name,
  5: bool is_system,
  6: bool is_physical,
  7: i64 col_id
}

typedef list<TColumnType> TRowDescriptor

/* union */ struct TColumnData {
  1: list<i64> int_col,
  2: list<double> real_col,
  3: list<string> str_col,
  4: list<TColumn> arr_col
}

struct TColumn {
  1: TColumnData data,
  2: list<bool> nulls
}

struct TRow {
  1: list<TDatum> cols
}

struct TRowSet {
  1: TRowDescriptor row_desc
  2: list<TRow> rows
  3: list<TColumn> columns
  4: bool is_columnar
}

struct TQueryResult {
  1: TRowSet row_set
  2: i64 execution_time_ms
  3: i64 total_time_ms
  4: string nonce
}

service Omnisci {
  string get_version() throws (1: TMapDException e)
  TSessionId connect(1: string user, 2: string passwd, 3: string dbname) throws (1: TMapDException e)
  TQueryResult sql_execute(1: TSessionId session, 2: string query 3: bool column_format, 4: string nonce, 5: i32 first_n = -1, 6: i32 at_most_n = -1) throws (1: TMapDException e)
  map<string, string> get_device_parameters(1: TSessionId session) throws (1: TMapDException e)
  void register_runtime_extension_functions(1: TSessionId session, 2: list<extension_functions.TUserDefinedFunction> udfs, 3: list<extension_functions.TUserDefinedTableFunction> udtfs, 4: map<string, string> device_ir_map) throws (1: TMapDException e)
}