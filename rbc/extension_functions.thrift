namespace java com.mapd.thrift.calciteserver

/* See QueryEngine/ExtensionFunctionsWhitelist.h for required
values. It is ok if the following definition is out-of-date as
get_device_parameters will contain up-to-date mapping of type names
and enum values [OmnisciDB >= 5.2]. */

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
  PBool,
  Bool,
  ArrayInt8,
  ArrayInt16,
  ArrayInt32,
  ArrayInt64,
  ArrayFloat,
  ArrayDouble,
  ArrayBool,
  GeoPoint,
  GeoLineString,
  Cursor,
  GeoPolygon,
  GeoMultiPolygon,
  ColumnInt8,
  ColumnInt16,
  ColumnInt32,
  ColumnInt64,
  ColumnFloat,
  ColumnDouble,
  ColumnBool,
  TextEncodingNone,
  TextEncodingDict,
  ColumnListInt8,
  ColumnListInt16,
  ColumnListInt32,
  ColumnListInt64,
  ColumnListFloat,
  ColumnListDouble,
  ColumnListBool,
  ColumnTextEncodingDict,
  ColumnListTextEncodingDict,
}

/* See QueryEngine/TableFunctions/TableFunctionsFactory.h for required
values. Same comments apply as for TExtArgumentType in above. */

enum TOutputBufferSizeType {
  kConstant,
  kUserSpecifiedConstantParameter,
  kUserSpecifiedRowMultiplier,
  kTableFunctionSpecifiedParameter,
  kPreFlightParameter
}

struct TUserDefinedFunction {
  1: string name,
  2: list<TExtArgumentType> argTypes
  3: TExtArgumentType retType
}

struct TUserDefinedTableFunction {
  /* The signature of an UDTF is defined by the SQL extension function signature
     and the LLVM/NVVM IR function signature. The signature of SQL extension function is
       <name>(<input1>, <input2>, ..., <sizer parameter>, <inputN>, ..) -> table(<output1>, <output2>, ...)
     where input can be either cursor or literal type and are collected in sqlArgTypes.

     The signature of a LLVM IR function is
       int32 <name>(<inputArgTypes>, <outputArgTypes>)
     where
       inputArgTypes[sizerArgPos - 1] corresponds to sizer parameter
       inputArgTypes[-2] corresponds to input_row_count_ptr
       inputArgTypes[-1] corresponds to output_row_count_ptr
   */
  1: string name,
  2: TOutputBufferSizeType sizerType,
  3: i32 sizerArgPos,
  4: list<TExtArgumentType> inputArgTypes,
  5: list<TExtArgumentType> outputArgTypes,
  6: list<TExtArgumentType> sqlArgTypes,
  7: list<map<string, string>> annotations
}
