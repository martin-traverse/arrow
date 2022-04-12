/**
 * Autogenerated by Thrift Compiler (0.16.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.parquet.format;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.16.0)", date = "2022-04-12")
public class AesGcmV1 implements org.apache.thrift.TBase<AesGcmV1, AesGcmV1._Fields>, java.io.Serializable, Cloneable, Comparable<AesGcmV1> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("AesGcmV1");

  private static final org.apache.thrift.protocol.TField AAD_PREFIX_FIELD_DESC = new org.apache.thrift.protocol.TField("aad_prefix", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField AAD_FILE_UNIQUE_FIELD_DESC = new org.apache.thrift.protocol.TField("aad_file_unique", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField SUPPLY_AAD_PREFIX_FIELD_DESC = new org.apache.thrift.protocol.TField("supply_aad_prefix", org.apache.thrift.protocol.TType.BOOL, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new AesGcmV1StandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new AesGcmV1TupleSchemeFactory();

  /**
   * AAD prefix *
   */
  public @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer aad_prefix; // optional
  /**
   * Unique file identifier part of AAD suffix *
   */
  public @org.apache.thrift.annotation.Nullable java.nio.ByteBuffer aad_file_unique; // optional
  /**
   * In files encrypted with AAD prefix without storing it,
   * readers must supply the prefix *
   */
  public boolean supply_aad_prefix; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * AAD prefix *
     */
    AAD_PREFIX((short)1, "aad_prefix"),
    /**
     * Unique file identifier part of AAD suffix *
     */
    AAD_FILE_UNIQUE((short)2, "aad_file_unique"),
    /**
     * In files encrypted with AAD prefix without storing it,
     * readers must supply the prefix *
     */
    SUPPLY_AAD_PREFIX((short)3, "supply_aad_prefix");

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
        case 1: // AAD_PREFIX
          return AAD_PREFIX;
        case 2: // AAD_FILE_UNIQUE
          return AAD_FILE_UNIQUE;
        case 3: // SUPPLY_AAD_PREFIX
          return SUPPLY_AAD_PREFIX;
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
  private static final int __SUPPLY_AAD_PREFIX_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.AAD_PREFIX,_Fields.AAD_FILE_UNIQUE,_Fields.SUPPLY_AAD_PREFIX};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.AAD_PREFIX, new org.apache.thrift.meta_data.FieldMetaData("aad_prefix", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.AAD_FILE_UNIQUE, new org.apache.thrift.meta_data.FieldMetaData("aad_file_unique", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING        , true)));
    tmpMap.put(_Fields.SUPPLY_AAD_PREFIX, new org.apache.thrift.meta_data.FieldMetaData("supply_aad_prefix", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(AesGcmV1.class, metaDataMap);
  }

  public AesGcmV1() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public AesGcmV1(AesGcmV1 other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetAad_prefix()) {
      this.aad_prefix = org.apache.thrift.TBaseHelper.copyBinary(other.aad_prefix);
    }
    if (other.isSetAad_file_unique()) {
      this.aad_file_unique = org.apache.thrift.TBaseHelper.copyBinary(other.aad_file_unique);
    }
    this.supply_aad_prefix = other.supply_aad_prefix;
  }

  public AesGcmV1 deepCopy() {
    return new AesGcmV1(this);
  }

  @Override
  public void clear() {
    this.aad_prefix = null;
    this.aad_file_unique = null;
    setSupply_aad_prefixIsSet(false);
    this.supply_aad_prefix = false;
  }

  /**
   * AAD prefix *
   */
  public byte[] getAad_prefix() {
    setAad_prefix(org.apache.thrift.TBaseHelper.rightSize(aad_prefix));
    return aad_prefix == null ? null : aad_prefix.array();
  }

  public java.nio.ByteBuffer bufferForAad_prefix() {
    return org.apache.thrift.TBaseHelper.copyBinary(aad_prefix);
  }

  /**
   * AAD prefix *
   */
  public AesGcmV1 setAad_prefix(byte[] aad_prefix) {
    this.aad_prefix = aad_prefix == null ? (java.nio.ByteBuffer)null   : java.nio.ByteBuffer.wrap(aad_prefix.clone());
    return this;
  }

  public AesGcmV1 setAad_prefix(@org.apache.thrift.annotation.Nullable java.nio.ByteBuffer aad_prefix) {
    this.aad_prefix = org.apache.thrift.TBaseHelper.copyBinary(aad_prefix);
    return this;
  }

  public void unsetAad_prefix() {
    this.aad_prefix = null;
  }

  /** Returns true if field aad_prefix is set (has been assigned a value) and false otherwise */
  public boolean isSetAad_prefix() {
    return this.aad_prefix != null;
  }

  public void setAad_prefixIsSet(boolean value) {
    if (!value) {
      this.aad_prefix = null;
    }
  }

  /**
   * Unique file identifier part of AAD suffix *
   */
  public byte[] getAad_file_unique() {
    setAad_file_unique(org.apache.thrift.TBaseHelper.rightSize(aad_file_unique));
    return aad_file_unique == null ? null : aad_file_unique.array();
  }

  public java.nio.ByteBuffer bufferForAad_file_unique() {
    return org.apache.thrift.TBaseHelper.copyBinary(aad_file_unique);
  }

  /**
   * Unique file identifier part of AAD suffix *
   */
  public AesGcmV1 setAad_file_unique(byte[] aad_file_unique) {
    this.aad_file_unique = aad_file_unique == null ? (java.nio.ByteBuffer)null   : java.nio.ByteBuffer.wrap(aad_file_unique.clone());
    return this;
  }

  public AesGcmV1 setAad_file_unique(@org.apache.thrift.annotation.Nullable java.nio.ByteBuffer aad_file_unique) {
    this.aad_file_unique = org.apache.thrift.TBaseHelper.copyBinary(aad_file_unique);
    return this;
  }

  public void unsetAad_file_unique() {
    this.aad_file_unique = null;
  }

  /** Returns true if field aad_file_unique is set (has been assigned a value) and false otherwise */
  public boolean isSetAad_file_unique() {
    return this.aad_file_unique != null;
  }

  public void setAad_file_uniqueIsSet(boolean value) {
    if (!value) {
      this.aad_file_unique = null;
    }
  }

  /**
   * In files encrypted with AAD prefix without storing it,
   * readers must supply the prefix *
   */
  public boolean isSupply_aad_prefix() {
    return this.supply_aad_prefix;
  }

  /**
   * In files encrypted with AAD prefix without storing it,
   * readers must supply the prefix *
   */
  public AesGcmV1 setSupply_aad_prefix(boolean supply_aad_prefix) {
    this.supply_aad_prefix = supply_aad_prefix;
    setSupply_aad_prefixIsSet(true);
    return this;
  }

  public void unsetSupply_aad_prefix() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __SUPPLY_AAD_PREFIX_ISSET_ID);
  }

  /** Returns true if field supply_aad_prefix is set (has been assigned a value) and false otherwise */
  public boolean isSetSupply_aad_prefix() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __SUPPLY_AAD_PREFIX_ISSET_ID);
  }

  public void setSupply_aad_prefixIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __SUPPLY_AAD_PREFIX_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case AAD_PREFIX:
      if (value == null) {
        unsetAad_prefix();
      } else {
        if (value instanceof byte[]) {
          setAad_prefix((byte[])value);
        } else {
          setAad_prefix((java.nio.ByteBuffer)value);
        }
      }
      break;

    case AAD_FILE_UNIQUE:
      if (value == null) {
        unsetAad_file_unique();
      } else {
        if (value instanceof byte[]) {
          setAad_file_unique((byte[])value);
        } else {
          setAad_file_unique((java.nio.ByteBuffer)value);
        }
      }
      break;

    case SUPPLY_AAD_PREFIX:
      if (value == null) {
        unsetSupply_aad_prefix();
      } else {
        setSupply_aad_prefix((java.lang.Boolean)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case AAD_PREFIX:
      return getAad_prefix();

    case AAD_FILE_UNIQUE:
      return getAad_file_unique();

    case SUPPLY_AAD_PREFIX:
      return isSupply_aad_prefix();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case AAD_PREFIX:
      return isSetAad_prefix();
    case AAD_FILE_UNIQUE:
      return isSetAad_file_unique();
    case SUPPLY_AAD_PREFIX:
      return isSetSupply_aad_prefix();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof AesGcmV1)
      return this.equals((AesGcmV1)that);
    return false;
  }

  public boolean equals(AesGcmV1 that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_aad_prefix = true && this.isSetAad_prefix();
    boolean that_present_aad_prefix = true && that.isSetAad_prefix();
    if (this_present_aad_prefix || that_present_aad_prefix) {
      if (!(this_present_aad_prefix && that_present_aad_prefix))
        return false;
      if (!this.aad_prefix.equals(that.aad_prefix))
        return false;
    }

    boolean this_present_aad_file_unique = true && this.isSetAad_file_unique();
    boolean that_present_aad_file_unique = true && that.isSetAad_file_unique();
    if (this_present_aad_file_unique || that_present_aad_file_unique) {
      if (!(this_present_aad_file_unique && that_present_aad_file_unique))
        return false;
      if (!this.aad_file_unique.equals(that.aad_file_unique))
        return false;
    }

    boolean this_present_supply_aad_prefix = true && this.isSetSupply_aad_prefix();
    boolean that_present_supply_aad_prefix = true && that.isSetSupply_aad_prefix();
    if (this_present_supply_aad_prefix || that_present_supply_aad_prefix) {
      if (!(this_present_supply_aad_prefix && that_present_supply_aad_prefix))
        return false;
      if (this.supply_aad_prefix != that.supply_aad_prefix)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetAad_prefix()) ? 131071 : 524287);
    if (isSetAad_prefix())
      hashCode = hashCode * 8191 + aad_prefix.hashCode();

    hashCode = hashCode * 8191 + ((isSetAad_file_unique()) ? 131071 : 524287);
    if (isSetAad_file_unique())
      hashCode = hashCode * 8191 + aad_file_unique.hashCode();

    hashCode = hashCode * 8191 + ((isSetSupply_aad_prefix()) ? 131071 : 524287);
    if (isSetSupply_aad_prefix())
      hashCode = hashCode * 8191 + ((supply_aad_prefix) ? 131071 : 524287);

    return hashCode;
  }

  @Override
  public int compareTo(AesGcmV1 other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetAad_prefix(), other.isSetAad_prefix());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAad_prefix()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.aad_prefix, other.aad_prefix);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetAad_file_unique(), other.isSetAad_file_unique());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAad_file_unique()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.aad_file_unique, other.aad_file_unique);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.compare(isSetSupply_aad_prefix(), other.isSetSupply_aad_prefix());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSupply_aad_prefix()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.supply_aad_prefix, other.supply_aad_prefix);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("AesGcmV1(");
    boolean first = true;

    if (isSetAad_prefix()) {
      sb.append("aad_prefix:");
      if (this.aad_prefix == null) {
        sb.append("null");
      } else {
        org.apache.thrift.TBaseHelper.toString(this.aad_prefix, sb);
      }
      first = false;
    }
    if (isSetAad_file_unique()) {
      if (!first) sb.append(", ");
      sb.append("aad_file_unique:");
      if (this.aad_file_unique == null) {
        sb.append("null");
      } else {
        org.apache.thrift.TBaseHelper.toString(this.aad_file_unique, sb);
      }
      first = false;
    }
    if (isSetSupply_aad_prefix()) {
      if (!first) sb.append(", ");
      sb.append("supply_aad_prefix:");
      sb.append(this.supply_aad_prefix);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
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

  private static class AesGcmV1StandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public AesGcmV1StandardScheme getScheme() {
      return new AesGcmV1StandardScheme();
    }
  }

  private static class AesGcmV1StandardScheme extends org.apache.thrift.scheme.StandardScheme<AesGcmV1> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, AesGcmV1 struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // AAD_PREFIX
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.aad_prefix = iprot.readBinary();
              struct.setAad_prefixIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // AAD_FILE_UNIQUE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.aad_file_unique = iprot.readBinary();
              struct.setAad_file_uniqueIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SUPPLY_AAD_PREFIX
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.supply_aad_prefix = iprot.readBool();
              struct.setSupply_aad_prefixIsSet(true);
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
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, AesGcmV1 struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.aad_prefix != null) {
        if (struct.isSetAad_prefix()) {
          oprot.writeFieldBegin(AAD_PREFIX_FIELD_DESC);
          oprot.writeBinary(struct.aad_prefix);
          oprot.writeFieldEnd();
        }
      }
      if (struct.aad_file_unique != null) {
        if (struct.isSetAad_file_unique()) {
          oprot.writeFieldBegin(AAD_FILE_UNIQUE_FIELD_DESC);
          oprot.writeBinary(struct.aad_file_unique);
          oprot.writeFieldEnd();
        }
      }
      if (struct.isSetSupply_aad_prefix()) {
        oprot.writeFieldBegin(SUPPLY_AAD_PREFIX_FIELD_DESC);
        oprot.writeBool(struct.supply_aad_prefix);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class AesGcmV1TupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public AesGcmV1TupleScheme getScheme() {
      return new AesGcmV1TupleScheme();
    }
  }

  private static class AesGcmV1TupleScheme extends org.apache.thrift.scheme.TupleScheme<AesGcmV1> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, AesGcmV1 struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetAad_prefix()) {
        optionals.set(0);
      }
      if (struct.isSetAad_file_unique()) {
        optionals.set(1);
      }
      if (struct.isSetSupply_aad_prefix()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetAad_prefix()) {
        oprot.writeBinary(struct.aad_prefix);
      }
      if (struct.isSetAad_file_unique()) {
        oprot.writeBinary(struct.aad_file_unique);
      }
      if (struct.isSetSupply_aad_prefix()) {
        oprot.writeBool(struct.supply_aad_prefix);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, AesGcmV1 struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.aad_prefix = iprot.readBinary();
        struct.setAad_prefixIsSet(true);
      }
      if (incoming.get(1)) {
        struct.aad_file_unique = iprot.readBinary();
        struct.setAad_file_uniqueIsSet(true);
      }
      if (incoming.get(2)) {
        struct.supply_aad_prefix = iprot.readBool();
        struct.setSupply_aad_prefixIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

