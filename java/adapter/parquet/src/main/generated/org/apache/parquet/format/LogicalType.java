/**
 * Autogenerated by Thrift Compiler (0.16.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.parquet.format;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
/**
 * LogicalType annotations to replace ConvertedType.
 * 
 * To maintain compatibility, implementations using LogicalType for a
 * SchemaElement must also set the corresponding ConvertedType from the
 * following table.
 */
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.16.0)", date = "2022-04-12")
public class LogicalType extends org.apache.thrift.TUnion<LogicalType, LogicalType._Fields> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("LogicalType");
  private static final org.apache.thrift.protocol.TField STRING_FIELD_DESC = new org.apache.thrift.protocol.TField("STRING", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField MAP_FIELD_DESC = new org.apache.thrift.protocol.TField("MAP", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField LIST_FIELD_DESC = new org.apache.thrift.protocol.TField("LIST", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField ENUM_FIELD_DESC = new org.apache.thrift.protocol.TField("ENUM", org.apache.thrift.protocol.TType.STRUCT, (short)4);
  private static final org.apache.thrift.protocol.TField DECIMAL_FIELD_DESC = new org.apache.thrift.protocol.TField("DECIMAL", org.apache.thrift.protocol.TType.STRUCT, (short)5);
  private static final org.apache.thrift.protocol.TField DATE_FIELD_DESC = new org.apache.thrift.protocol.TField("DATE", org.apache.thrift.protocol.TType.STRUCT, (short)6);
  private static final org.apache.thrift.protocol.TField TIME_FIELD_DESC = new org.apache.thrift.protocol.TField("TIME", org.apache.thrift.protocol.TType.STRUCT, (short)7);
  private static final org.apache.thrift.protocol.TField TIMESTAMP_FIELD_DESC = new org.apache.thrift.protocol.TField("TIMESTAMP", org.apache.thrift.protocol.TType.STRUCT, (short)8);
  private static final org.apache.thrift.protocol.TField INTEGER_FIELD_DESC = new org.apache.thrift.protocol.TField("INTEGER", org.apache.thrift.protocol.TType.STRUCT, (short)10);
  private static final org.apache.thrift.protocol.TField UNKNOWN_FIELD_DESC = new org.apache.thrift.protocol.TField("UNKNOWN", org.apache.thrift.protocol.TType.STRUCT, (short)11);
  private static final org.apache.thrift.protocol.TField JSON_FIELD_DESC = new org.apache.thrift.protocol.TField("JSON", org.apache.thrift.protocol.TType.STRUCT, (short)12);
  private static final org.apache.thrift.protocol.TField BSON_FIELD_DESC = new org.apache.thrift.protocol.TField("BSON", org.apache.thrift.protocol.TType.STRUCT, (short)13);
  private static final org.apache.thrift.protocol.TField UUID_FIELD_DESC = new org.apache.thrift.protocol.TField("UUID", org.apache.thrift.protocol.TType.STRUCT, (short)14);

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    STRING((short)1, "STRING"),
    MAP((short)2, "MAP"),
    LIST((short)3, "LIST"),
    ENUM((short)4, "ENUM"),
    DECIMAL((short)5, "DECIMAL"),
    DATE((short)6, "DATE"),
    TIME((short)7, "TIME"),
    TIMESTAMP((short)8, "TIMESTAMP"),
    INTEGER((short)10, "INTEGER"),
    UNKNOWN((short)11, "UNKNOWN"),
    JSON((short)12, "JSON"),
    BSON((short)13, "BSON"),
    UUID((short)14, "UUID");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // STRING
          return STRING;
        case 2: // MAP
          return MAP;
        case 3: // LIST
          return LIST;
        case 4: // ENUM
          return ENUM;
        case 5: // DECIMAL
          return DECIMAL;
        case 6: // DATE
          return DATE;
        case 7: // TIME
          return TIME;
        case 8: // TIMESTAMP
          return TIMESTAMP;
        case 10: // INTEGER
          return INTEGER;
        case 11: // UNKNOWN
          return UNKNOWN;
        case 12: // JSON
          return JSON;
        case 13: // BSON
          return BSON;
        case 14: // UUID
          return UUID;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.STRING, new org.apache.thrift.meta_data.FieldMetaData("STRING", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, StringType.class)));
    tmpMap.put(_Fields.MAP, new org.apache.thrift.meta_data.FieldMetaData("MAP", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, MapType.class)));
    tmpMap.put(_Fields.LIST, new org.apache.thrift.meta_data.FieldMetaData("LIST", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ListType.class)));
    tmpMap.put(_Fields.ENUM, new org.apache.thrift.meta_data.FieldMetaData("ENUM", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, EnumType.class)));
    tmpMap.put(_Fields.DECIMAL, new org.apache.thrift.meta_data.FieldMetaData("DECIMAL", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, DecimalType.class)));
    tmpMap.put(_Fields.DATE, new org.apache.thrift.meta_data.FieldMetaData("DATE", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, DateType.class)));
    tmpMap.put(_Fields.TIME, new org.apache.thrift.meta_data.FieldMetaData("TIME", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TimeType.class)));
    tmpMap.put(_Fields.TIMESTAMP, new org.apache.thrift.meta_data.FieldMetaData("TIMESTAMP", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TimestampType.class)));
    tmpMap.put(_Fields.INTEGER, new org.apache.thrift.meta_data.FieldMetaData("INTEGER", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, IntType.class)));
    tmpMap.put(_Fields.UNKNOWN, new org.apache.thrift.meta_data.FieldMetaData("UNKNOWN", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, NullType.class)));
    tmpMap.put(_Fields.JSON, new org.apache.thrift.meta_data.FieldMetaData("JSON", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, JsonType.class)));
    tmpMap.put(_Fields.BSON, new org.apache.thrift.meta_data.FieldMetaData("BSON", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, BsonType.class)));
    tmpMap.put(_Fields.UUID, new org.apache.thrift.meta_data.FieldMetaData("UUID", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, UUIDType.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(LogicalType.class, metaDataMap);
  }

  public LogicalType() {
    super();
  }

  public LogicalType(_Fields setField, java.lang.Object value) {
    super(setField, value);
  }

  public LogicalType(LogicalType other) {
    super(other);
  }
  public LogicalType deepCopy() {
    return new LogicalType(this);
  }

  public static LogicalType STRING(StringType value) {
    LogicalType x = new LogicalType();
    x.setSTRING(value);
    return x;
  }

  public static LogicalType MAP(MapType value) {
    LogicalType x = new LogicalType();
    x.setMAP(value);
    return x;
  }

  public static LogicalType LIST(ListType value) {
    LogicalType x = new LogicalType();
    x.setLIST(value);
    return x;
  }

  public static LogicalType ENUM(EnumType value) {
    LogicalType x = new LogicalType();
    x.setENUM(value);
    return x;
  }

  public static LogicalType DECIMAL(DecimalType value) {
    LogicalType x = new LogicalType();
    x.setDECIMAL(value);
    return x;
  }

  public static LogicalType DATE(DateType value) {
    LogicalType x = new LogicalType();
    x.setDATE(value);
    return x;
  }

  public static LogicalType TIME(TimeType value) {
    LogicalType x = new LogicalType();
    x.setTIME(value);
    return x;
  }

  public static LogicalType TIMESTAMP(TimestampType value) {
    LogicalType x = new LogicalType();
    x.setTIMESTAMP(value);
    return x;
  }

  public static LogicalType INTEGER(IntType value) {
    LogicalType x = new LogicalType();
    x.setINTEGER(value);
    return x;
  }

  public static LogicalType UNKNOWN(NullType value) {
    LogicalType x = new LogicalType();
    x.setUNKNOWN(value);
    return x;
  }

  public static LogicalType JSON(JsonType value) {
    LogicalType x = new LogicalType();
    x.setJSON(value);
    return x;
  }

  public static LogicalType BSON(BsonType value) {
    LogicalType x = new LogicalType();
    x.setBSON(value);
    return x;
  }

  public static LogicalType UUID(UUIDType value) {
    LogicalType x = new LogicalType();
    x.setUUID(value);
    return x;
  }


  @Override
  protected void checkType(_Fields setField, java.lang.Object value) throws java.lang.ClassCastException {
    switch (setField) {
      case STRING:
        if (value instanceof StringType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type StringType for field 'STRING', but got " + value.getClass().getSimpleName());
      case MAP:
        if (value instanceof MapType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type MapType for field 'MAP', but got " + value.getClass().getSimpleName());
      case LIST:
        if (value instanceof ListType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type ListType for field 'LIST', but got " + value.getClass().getSimpleName());
      case ENUM:
        if (value instanceof EnumType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type EnumType for field 'ENUM', but got " + value.getClass().getSimpleName());
      case DECIMAL:
        if (value instanceof DecimalType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type DecimalType for field 'DECIMAL', but got " + value.getClass().getSimpleName());
      case DATE:
        if (value instanceof DateType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type DateType for field 'DATE', but got " + value.getClass().getSimpleName());
      case TIME:
        if (value instanceof TimeType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type TimeType for field 'TIME', but got " + value.getClass().getSimpleName());
      case TIMESTAMP:
        if (value instanceof TimestampType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type TimestampType for field 'TIMESTAMP', but got " + value.getClass().getSimpleName());
      case INTEGER:
        if (value instanceof IntType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type IntType for field 'INTEGER', but got " + value.getClass().getSimpleName());
      case UNKNOWN:
        if (value instanceof NullType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type NullType for field 'UNKNOWN', but got " + value.getClass().getSimpleName());
      case JSON:
        if (value instanceof JsonType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type JsonType for field 'JSON', but got " + value.getClass().getSimpleName());
      case BSON:
        if (value instanceof BsonType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type BsonType for field 'BSON', but got " + value.getClass().getSimpleName());
      case UUID:
        if (value instanceof UUIDType) {
          break;
        }
        throw new java.lang.ClassCastException("Was expecting value of type UUIDType for field 'UUID', but got " + value.getClass().getSimpleName());
      default:
        throw new java.lang.IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected java.lang.Object standardSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TField field) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(field.id);
    if (setField != null) {
      switch (setField) {
        case STRING:
          if (field.type == STRING_FIELD_DESC.type) {
            StringType STRING;
            STRING = new StringType();
            STRING.read(iprot);
            return STRING;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case MAP:
          if (field.type == MAP_FIELD_DESC.type) {
            MapType MAP;
            MAP = new MapType();
            MAP.read(iprot);
            return MAP;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case LIST:
          if (field.type == LIST_FIELD_DESC.type) {
            ListType LIST;
            LIST = new ListType();
            LIST.read(iprot);
            return LIST;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case ENUM:
          if (field.type == ENUM_FIELD_DESC.type) {
            EnumType ENUM;
            ENUM = new EnumType();
            ENUM.read(iprot);
            return ENUM;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case DECIMAL:
          if (field.type == DECIMAL_FIELD_DESC.type) {
            DecimalType DECIMAL;
            DECIMAL = new DecimalType();
            DECIMAL.read(iprot);
            return DECIMAL;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case DATE:
          if (field.type == DATE_FIELD_DESC.type) {
            DateType DATE;
            DATE = new DateType();
            DATE.read(iprot);
            return DATE;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case TIME:
          if (field.type == TIME_FIELD_DESC.type) {
            TimeType TIME;
            TIME = new TimeType();
            TIME.read(iprot);
            return TIME;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case TIMESTAMP:
          if (field.type == TIMESTAMP_FIELD_DESC.type) {
            TimestampType TIMESTAMP;
            TIMESTAMP = new TimestampType();
            TIMESTAMP.read(iprot);
            return TIMESTAMP;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case INTEGER:
          if (field.type == INTEGER_FIELD_DESC.type) {
            IntType INTEGER;
            INTEGER = new IntType();
            INTEGER.read(iprot);
            return INTEGER;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case UNKNOWN:
          if (field.type == UNKNOWN_FIELD_DESC.type) {
            NullType UNKNOWN;
            UNKNOWN = new NullType();
            UNKNOWN.read(iprot);
            return UNKNOWN;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case JSON:
          if (field.type == JSON_FIELD_DESC.type) {
            JsonType JSON;
            JSON = new JsonType();
            JSON.read(iprot);
            return JSON;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case BSON:
          if (field.type == BSON_FIELD_DESC.type) {
            BsonType BSON;
            BSON = new BsonType();
            BSON.read(iprot);
            return BSON;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case UUID:
          if (field.type == UUID_FIELD_DESC.type) {
            UUIDType UUID;
            UUID = new UUIDType();
            UUID.read(iprot);
            return UUID;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        default:
          throw new java.lang.IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      return null;
    }
  }

  @Override
  protected void standardSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case STRING:
        StringType STRING = (StringType)value_;
        STRING.write(oprot);
        return;
      case MAP:
        MapType MAP = (MapType)value_;
        MAP.write(oprot);
        return;
      case LIST:
        ListType LIST = (ListType)value_;
        LIST.write(oprot);
        return;
      case ENUM:
        EnumType ENUM = (EnumType)value_;
        ENUM.write(oprot);
        return;
      case DECIMAL:
        DecimalType DECIMAL = (DecimalType)value_;
        DECIMAL.write(oprot);
        return;
      case DATE:
        DateType DATE = (DateType)value_;
        DATE.write(oprot);
        return;
      case TIME:
        TimeType TIME = (TimeType)value_;
        TIME.write(oprot);
        return;
      case TIMESTAMP:
        TimestampType TIMESTAMP = (TimestampType)value_;
        TIMESTAMP.write(oprot);
        return;
      case INTEGER:
        IntType INTEGER = (IntType)value_;
        INTEGER.write(oprot);
        return;
      case UNKNOWN:
        NullType UNKNOWN = (NullType)value_;
        UNKNOWN.write(oprot);
        return;
      case JSON:
        JsonType JSON = (JsonType)value_;
        JSON.write(oprot);
        return;
      case BSON:
        BsonType BSON = (BsonType)value_;
        BSON.write(oprot);
        return;
      case UUID:
        UUIDType UUID = (UUIDType)value_;
        UUID.write(oprot);
        return;
      default:
        throw new java.lang.IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected java.lang.Object tupleSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, short fieldID) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(fieldID);
    if (setField != null) {
      switch (setField) {
        case STRING:
          StringType STRING;
          STRING = new StringType();
          STRING.read(iprot);
          return STRING;
        case MAP:
          MapType MAP;
          MAP = new MapType();
          MAP.read(iprot);
          return MAP;
        case LIST:
          ListType LIST;
          LIST = new ListType();
          LIST.read(iprot);
          return LIST;
        case ENUM:
          EnumType ENUM;
          ENUM = new EnumType();
          ENUM.read(iprot);
          return ENUM;
        case DECIMAL:
          DecimalType DECIMAL;
          DECIMAL = new DecimalType();
          DECIMAL.read(iprot);
          return DECIMAL;
        case DATE:
          DateType DATE;
          DATE = new DateType();
          DATE.read(iprot);
          return DATE;
        case TIME:
          TimeType TIME;
          TIME = new TimeType();
          TIME.read(iprot);
          return TIME;
        case TIMESTAMP:
          TimestampType TIMESTAMP;
          TIMESTAMP = new TimestampType();
          TIMESTAMP.read(iprot);
          return TIMESTAMP;
        case INTEGER:
          IntType INTEGER;
          INTEGER = new IntType();
          INTEGER.read(iprot);
          return INTEGER;
        case UNKNOWN:
          NullType UNKNOWN;
          UNKNOWN = new NullType();
          UNKNOWN.read(iprot);
          return UNKNOWN;
        case JSON:
          JsonType JSON;
          JSON = new JsonType();
          JSON.read(iprot);
          return JSON;
        case BSON:
          BsonType BSON;
          BSON = new BsonType();
          BSON.read(iprot);
          return BSON;
        case UUID:
          UUIDType UUID;
          UUID = new UUIDType();
          UUID.read(iprot);
          return UUID;
        default:
          throw new java.lang.IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      throw new org.apache.thrift.protocol.TProtocolException("Couldn't find a field with field id " + fieldID);
    }
  }

  @Override
  protected void tupleSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case STRING:
        StringType STRING = (StringType)value_;
        STRING.write(oprot);
        return;
      case MAP:
        MapType MAP = (MapType)value_;
        MAP.write(oprot);
        return;
      case LIST:
        ListType LIST = (ListType)value_;
        LIST.write(oprot);
        return;
      case ENUM:
        EnumType ENUM = (EnumType)value_;
        ENUM.write(oprot);
        return;
      case DECIMAL:
        DecimalType DECIMAL = (DecimalType)value_;
        DECIMAL.write(oprot);
        return;
      case DATE:
        DateType DATE = (DateType)value_;
        DATE.write(oprot);
        return;
      case TIME:
        TimeType TIME = (TimeType)value_;
        TIME.write(oprot);
        return;
      case TIMESTAMP:
        TimestampType TIMESTAMP = (TimestampType)value_;
        TIMESTAMP.write(oprot);
        return;
      case INTEGER:
        IntType INTEGER = (IntType)value_;
        INTEGER.write(oprot);
        return;
      case UNKNOWN:
        NullType UNKNOWN = (NullType)value_;
        UNKNOWN.write(oprot);
        return;
      case JSON:
        JsonType JSON = (JsonType)value_;
        JSON.write(oprot);
        return;
      case BSON:
        BsonType BSON = (BsonType)value_;
        BSON.write(oprot);
        return;
      case UUID:
        UUIDType UUID = (UUIDType)value_;
        UUID.write(oprot);
        return;
      default:
        throw new java.lang.IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TField getFieldDesc(_Fields setField) {
    switch (setField) {
      case STRING:
        return STRING_FIELD_DESC;
      case MAP:
        return MAP_FIELD_DESC;
      case LIST:
        return LIST_FIELD_DESC;
      case ENUM:
        return ENUM_FIELD_DESC;
      case DECIMAL:
        return DECIMAL_FIELD_DESC;
      case DATE:
        return DATE_FIELD_DESC;
      case TIME:
        return TIME_FIELD_DESC;
      case TIMESTAMP:
        return TIMESTAMP_FIELD_DESC;
      case INTEGER:
        return INTEGER_FIELD_DESC;
      case UNKNOWN:
        return UNKNOWN_FIELD_DESC;
      case JSON:
        return JSON_FIELD_DESC;
      case BSON:
        return BSON_FIELD_DESC;
      case UUID:
        return UUID_FIELD_DESC;
      default:
        throw new java.lang.IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TStruct getStructDesc() {
    return STRUCT_DESC;
  }

  @Override
  protected _Fields enumForId(short id) {
    return _Fields.findByThriftIdOrThrow(id);
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }


  public StringType getSTRING() {
    if (getSetField() == _Fields.STRING) {
      return (StringType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'STRING' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setSTRING(StringType value) {
    setField_ = _Fields.STRING;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.STRING");
  }

  public MapType getMAP() {
    if (getSetField() == _Fields.MAP) {
      return (MapType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'MAP' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setMAP(MapType value) {
    setField_ = _Fields.MAP;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.MAP");
  }

  public ListType getLIST() {
    if (getSetField() == _Fields.LIST) {
      return (ListType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'LIST' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setLIST(ListType value) {
    setField_ = _Fields.LIST;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.LIST");
  }

  public EnumType getENUM() {
    if (getSetField() == _Fields.ENUM) {
      return (EnumType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'ENUM' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setENUM(EnumType value) {
    setField_ = _Fields.ENUM;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.ENUM");
  }

  public DecimalType getDECIMAL() {
    if (getSetField() == _Fields.DECIMAL) {
      return (DecimalType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'DECIMAL' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setDECIMAL(DecimalType value) {
    setField_ = _Fields.DECIMAL;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.DECIMAL");
  }

  public DateType getDATE() {
    if (getSetField() == _Fields.DATE) {
      return (DateType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'DATE' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setDATE(DateType value) {
    setField_ = _Fields.DATE;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.DATE");
  }

  public TimeType getTIME() {
    if (getSetField() == _Fields.TIME) {
      return (TimeType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'TIME' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setTIME(TimeType value) {
    setField_ = _Fields.TIME;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.TIME");
  }

  public TimestampType getTIMESTAMP() {
    if (getSetField() == _Fields.TIMESTAMP) {
      return (TimestampType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'TIMESTAMP' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setTIMESTAMP(TimestampType value) {
    setField_ = _Fields.TIMESTAMP;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.TIMESTAMP");
  }

  public IntType getINTEGER() {
    if (getSetField() == _Fields.INTEGER) {
      return (IntType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'INTEGER' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setINTEGER(IntType value) {
    setField_ = _Fields.INTEGER;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.INTEGER");
  }

  public NullType getUNKNOWN() {
    if (getSetField() == _Fields.UNKNOWN) {
      return (NullType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'UNKNOWN' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setUNKNOWN(NullType value) {
    setField_ = _Fields.UNKNOWN;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.UNKNOWN");
  }

  public JsonType getJSON() {
    if (getSetField() == _Fields.JSON) {
      return (JsonType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'JSON' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setJSON(JsonType value) {
    setField_ = _Fields.JSON;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.JSON");
  }

  public BsonType getBSON() {
    if (getSetField() == _Fields.BSON) {
      return (BsonType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'BSON' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setBSON(BsonType value) {
    setField_ = _Fields.BSON;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.BSON");
  }

  public UUIDType getUUID() {
    if (getSetField() == _Fields.UUID) {
      return (UUIDType)getFieldValue();
    } else {
      throw new java.lang.RuntimeException("Cannot get field 'UUID' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setUUID(UUIDType value) {
    setField_ = _Fields.UUID;
    value_ = java.util.Objects.requireNonNull(value,"_Fields.UUID");
  }

  public boolean isSetSTRING() {
    return setField_ == _Fields.STRING;
  }


  public boolean isSetMAP() {
    return setField_ == _Fields.MAP;
  }


  public boolean isSetLIST() {
    return setField_ == _Fields.LIST;
  }


  public boolean isSetENUM() {
    return setField_ == _Fields.ENUM;
  }


  public boolean isSetDECIMAL() {
    return setField_ == _Fields.DECIMAL;
  }


  public boolean isSetDATE() {
    return setField_ == _Fields.DATE;
  }


  public boolean isSetTIME() {
    return setField_ == _Fields.TIME;
  }


  public boolean isSetTIMESTAMP() {
    return setField_ == _Fields.TIMESTAMP;
  }


  public boolean isSetINTEGER() {
    return setField_ == _Fields.INTEGER;
  }


  public boolean isSetUNKNOWN() {
    return setField_ == _Fields.UNKNOWN;
  }


  public boolean isSetJSON() {
    return setField_ == _Fields.JSON;
  }


  public boolean isSetBSON() {
    return setField_ == _Fields.BSON;
  }


  public boolean isSetUUID() {
    return setField_ == _Fields.UUID;
  }


  public boolean equals(java.lang.Object other) {
    if (other instanceof LogicalType) {
      return equals((LogicalType)other);
    } else {
      return false;
    }
  }

  public boolean equals(LogicalType other) {
    return other != null && getSetField() == other.getSetField() && getFieldValue().equals(other.getFieldValue());
  }

  @Override
  public int compareTo(LogicalType other) {
    int lastComparison = org.apache.thrift.TBaseHelper.compareTo(getSetField(), other.getSetField());
    if (lastComparison == 0) {
      return org.apache.thrift.TBaseHelper.compareTo(getFieldValue(), other.getFieldValue());
    }
    return lastComparison;
  }


  @Override
  public int hashCode() {
    java.util.List<java.lang.Object> list = new java.util.ArrayList<java.lang.Object>();
    list.add(this.getClass().getName());
    org.apache.thrift.TFieldIdEnum setField = getSetField();
    if (setField != null) {
      list.add(setField.getThriftFieldId());
      java.lang.Object value = getFieldValue();
      if (value instanceof org.apache.thrift.TEnum) {
        list.add(((org.apache.thrift.TEnum)getFieldValue()).getValue());
      } else {
        list.add(value);
      }
    }
    return list.hashCode();
  }
  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }


  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }


}