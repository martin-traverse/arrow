/**
 * Autogenerated by Thrift Compiler (0.16.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.parquet.format;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
/**
 * Time logical type annotation
 * 
 * Allowed for physical types: INT32 (millis), INT64 (micros, nanos)
 */
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.16.0)", date = "2022-04-12")
public class TimeType implements org.apache.thrift.TBase<TimeType, TimeType._Fields>, java.io.Serializable, Cloneable, Comparable<TimeType> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TimeType");

  private static final org.apache.thrift.protocol.TField IS_ADJUSTED_TO_UTC_FIELD_DESC = new org.apache.thrift.protocol.TField("isAdjustedToUTC", org.apache.thrift.protocol.TType.BOOL, (short)1);
  private static final org.apache.thrift.protocol.TField UNIT_FIELD_DESC = new org.apache.thrift.protocol.TField("unit", org.apache.thrift.protocol.TType.STRUCT, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new TimeTypeStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new TimeTypeTupleSchemeFactory();

  public boolean isAdjustedToUTC; // required
  public @org.apache.thrift.annotation.Nullable TimeUnit unit; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    IS_ADJUSTED_TO_UTC((short)1, "isAdjustedToUTC"),
    UNIT((short)2, "unit");

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
        case 1: // IS_ADJUSTED_TO_UTC
          return IS_ADJUSTED_TO_UTC;
        case 2: // UNIT
          return UNIT;
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

  // isset id assignments
  private static final int __ISADJUSTEDTOUTC_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.IS_ADJUSTED_TO_UTC, new org.apache.thrift.meta_data.FieldMetaData("isAdjustedToUTC", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.UNIT, new org.apache.thrift.meta_data.FieldMetaData("unit", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TimeUnit.class)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TimeType.class, metaDataMap);
  }

  public TimeType() {
  }

  public TimeType(
    boolean isAdjustedToUTC,
    TimeUnit unit)
  {
    this();
    this.isAdjustedToUTC = isAdjustedToUTC;
    setIsAdjustedToUTCIsSet(true);
    this.unit = unit;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TimeType(TimeType other) {
    __isset_bitfield = other.__isset_bitfield;
    this.isAdjustedToUTC = other.isAdjustedToUTC;
    if (other.isSetUnit()) {
      this.unit = new TimeUnit(other.unit);
    }
  }

  public TimeType deepCopy() {
    return new TimeType(this);
  }

  @Override
  public void clear() {
    setIsAdjustedToUTCIsSet(false);
    this.isAdjustedToUTC = false;
    this.unit = null;
  }

  public boolean isIsAdjustedToUTC() {
    return this.isAdjustedToUTC;
  }

  public TimeType setIsAdjustedToUTC(boolean isAdjustedToUTC) {
    this.isAdjustedToUTC = isAdjustedToUTC;
    setIsAdjustedToUTCIsSet(true);
    return this;
  }

  public void unsetIsAdjustedToUTC() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __ISADJUSTEDTOUTC_ISSET_ID);
  }

  /** Returns true if field isAdjustedToUTC is set (has been assigned a value) and false otherwise */
  public boolean isSetIsAdjustedToUTC() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __ISADJUSTEDTOUTC_ISSET_ID);
  }

  public void setIsAdjustedToUTCIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __ISADJUSTEDTOUTC_ISSET_ID, value);
  }

  @org.apache.thrift.annotation.Nullable
  public TimeUnit getUnit() {
    return this.unit;
  }

  public TimeType setUnit(@org.apache.thrift.annotation.Nullable TimeUnit unit) {
    this.unit = unit;
    return this;
  }

  public void unsetUnit() {
    this.unit = null;
  }

  /** Returns true if field unit is set (has been assigned a value) and false otherwise */
  public boolean isSetUnit() {
    return this.unit != null;
  }

  public void setUnitIsSet(boolean value) {
    if (!value) {
      this.unit = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case IS_ADJUSTED_TO_UTC:
      if (value == null) {
        unsetIsAdjustedToUTC();
      } else {
        setIsAdjustedToUTC((java.lang.Boolean)value);
      }
      break;

    case UNIT:
      if (value == null) {
        unsetUnit();
      } else {
        setUnit((TimeUnit)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case IS_ADJUSTED_TO_UTC:
      return isIsAdjustedToUTC();

    case UNIT:
      return getUnit();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case IS_ADJUSTED_TO_UTC:
      return isSetIsAdjustedToUTC();
    case UNIT:
      return isSetUnit();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof TimeType)
      return this.equals((TimeType)that);
    return false;
  }

  public boolean equals(TimeType that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_isAdjustedToUTC = true;
    boolean that_present_isAdjustedToUTC = true;
    if (this_present_isAdjustedToUTC || that_present_isAdjustedToUTC) {
      if (!(this_present_isAdjustedToUTC && that_present_isAdjustedToUTC))
        return false;
      if (this.isAdjustedToUTC != that.isAdjustedToUTC)
        return false;
    }

    boolean this_present_unit = true && this.isSetUnit();
    boolean that_present_unit = true && that.isSetUnit();
    if (this_present_unit || that_present_unit) {
      if (!(this_present_unit && that_present_unit))
        return false;
      if (!this.unit.equals(that.unit))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isAdjustedToUTC) ? 131071 : 524287);

    hashCode = hashCode * 8191 + ((isSetUnit()) ? 131071 : 524287);
    if (isSetUnit())
      hashCode = hashCode * 8191 + unit.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(TimeType other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetIsAdjustedToUTC(), other.isSetIsAdjustedToUTC());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIsAdjustedToUTC()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.isAdjustedToUTC, other.isAdjustedToUTC);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetUnit(), other.isSetUnit());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUnit()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.unit, other.unit);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("TimeType(");
    boolean first = true;

    sb.append("isAdjustedToUTC:");
    sb.append(this.isAdjustedToUTC);
    first = false;
    if (!first) sb.append(", ");
    sb.append("unit:");
    if (this.unit == null) {
      sb.append("null");
    } else {
      sb.append(this.unit);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'isAdjustedToUTC' because it's a primitive and you chose the non-beans generator.
    if (unit == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'unit' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TimeTypeStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TimeTypeStandardScheme getScheme() {
      return new TimeTypeStandardScheme();
    }
  }

  private static class TimeTypeStandardScheme extends org.apache.thrift.scheme.StandardScheme<TimeType> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TimeType struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // IS_ADJUSTED_TO_UTC
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.isAdjustedToUTC = iprot.readBool();
              struct.setIsAdjustedToUTCIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // UNIT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.unit = new TimeUnit();
              struct.unit.read(iprot);
              struct.setUnitIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetIsAdjustedToUTC()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'isAdjustedToUTC' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TimeType struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(IS_ADJUSTED_TO_UTC_FIELD_DESC);
      oprot.writeBool(struct.isAdjustedToUTC);
      oprot.writeFieldEnd();
      if (struct.unit != null) {
        oprot.writeFieldBegin(UNIT_FIELD_DESC);
        struct.unit.write(oprot);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TimeTypeTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public TimeTypeTupleScheme getScheme() {
      return new TimeTypeTupleScheme();
    }
  }

  private static class TimeTypeTupleScheme extends org.apache.thrift.scheme.TupleScheme<TimeType> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TimeType struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeBool(struct.isAdjustedToUTC);
      struct.unit.write(oprot);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TimeType struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.isAdjustedToUTC = iprot.readBool();
      struct.setIsAdjustedToUTCIsSet(true);
      struct.unit = new TimeUnit();
      struct.unit.read(iprot);
      struct.setUnitIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

