namespace java com.mapd.thrift.server
namespace py omnisci.common

enum TDeviceType {
  CPU,
  GPU
}

enum TDatumType {
  SMALLINT,
  INT,
  BIGINT,
  FLOAT,
  DECIMAL,
  DOUBLE,
  STR,
  TIME,
  TIMESTAMP,
  DATE,
  BOOL,
  INTERVAL_DAY_TIME,
  INTERVAL_YEAR_MONTH,
  POINT,
  LINESTRING,
  POLYGON,
  MULTIPOLYGON,
  TINYINT,
  GEOMETRY,
  GEOGRAPHY
}

enum TEncodingType {
  NONE,
  FIXED,
  RL,
  DIFF,
  DICT,
  SPARSE,
  GEOINT,
  DATE_IN_DAYS
}

struct TTypeInfo {
  1: TDatumType type,
  4: TEncodingType encoding,
  2: bool nullable,
  3: bool is_array,
  5: i32 precision,
  6: i32 scale,
  7: i32 comp_param,
  8: optional i32 size=-1
}

/* See QueryEngine/ExtensionFunctionsWhitelist.h for required values */
enum TExtArgumentType {
  Int8,
  Int16,
  Int32,
  Int64,
  Float,
  Double,
  Void,
  PInt8,
  PInt16,
  PInt32,
  PInt64,
  PFloat,
  PDouble,
  Bool,
  ArrayInt8,
  ArrayInt16,
  ArrayInt32,
  ArrayInt64,
  ArrayFloat,
  ArrayDouble,
  GeoPoint,
  Cursor
}

/* See QueryEngine/TableFunctions/TableFunctionsFactory.h for required values */
enum TOutputBufferSizeType {
  kUserSpecifiedConstantParameter,
  kUserSpecifiedRowMultiplier,
  kConstant
}