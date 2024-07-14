/**
 * Autogenerated by Thrift Compiler (0.16.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.parquet.format;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.16.0)", date = "2022-04-12")
public class OffsetIndex implements org.apache.thrift.TBase<OffsetIndex, OffsetIndex._Fields>, java.io.Serializable, Cloneable, Comparable<OffsetIndex> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("OffsetIndex");

  private static final org.apache.thrift.protocol.TField PAGE_LOCATIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("page_locations", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new OffsetIndexStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new OffsetIndexTupleSchemeFactory();

  /**
   * PageLocations, ordered by increasing PageLocation.offset. It is required
   * that page_locations[i].first_row_index < page_locations[i+1].first_row_index.
   */
  public @org.apache.thrift.annotation.Nullable java.util.List<PageLocation> page_locations; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    /**
     * PageLocations, ordered by increasing PageLocation.offset. It is required
     * that page_locations[i].first_row_index < page_locations[i+1].first_row_index.
     */
    PAGE_LOCATIONS((short)1, "page_locations");

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
        case 1: // PAGE_LOCATIONS
          return PAGE_LOCATIONS;
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
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.PAGE_LOCATIONS, new org.apache.thrift.meta_data.FieldMetaData("page_locations", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, PageLocation.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(OffsetIndex.class, metaDataMap);
  }

  public OffsetIndex() {
  }

  public OffsetIndex(
    java.util.List<PageLocation> page_locations)
  {
    this();
    this.page_locations = page_locations;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public OffsetIndex(OffsetIndex other) {
    if (other.isSetPage_locations()) {
      java.util.List<PageLocation> __this__page_locations = new java.util.ArrayList<PageLocation>(other.page_locations.size());
      for (PageLocation other_element : other.page_locations) {
        __this__page_locations.add(new PageLocation(other_element));
      }
      this.page_locations = __this__page_locations;
    }
  }

  public OffsetIndex deepCopy() {
    return new OffsetIndex(this);
  }

  @Override
  public void clear() {
    this.page_locations = null;
  }

  public int getPage_locationsSize() {
    return (this.page_locations == null) ? 0 : this.page_locations.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<PageLocation> getPage_locationsIterator() {
    return (this.page_locations == null) ? null : this.page_locations.iterator();
  }

  public void addToPage_locations(PageLocation elem) {
    if (this.page_locations == null) {
      this.page_locations = new java.util.ArrayList<PageLocation>();
    }
    this.page_locations.add(elem);
  }

  /**
   * PageLocations, ordered by increasing PageLocation.offset. It is required
   * that page_locations[i].first_row_index < page_locations[i+1].first_row_index.
   */
  @org.apache.thrift.annotation.Nullable
  public java.util.List<PageLocation> getPage_locations() {
    return this.page_locations;
  }

  /**
   * PageLocations, ordered by increasing PageLocation.offset. It is required
   * that page_locations[i].first_row_index < page_locations[i+1].first_row_index.
   */
  public OffsetIndex setPage_locations(@org.apache.thrift.annotation.Nullable java.util.List<PageLocation> page_locations) {
    this.page_locations = page_locations;
    return this;
  }

  public void unsetPage_locations() {
    this.page_locations = null;
  }

  /** Returns true if field page_locations is set (has been assigned a value) and false otherwise */
  public boolean isSetPage_locations() {
    return this.page_locations != null;
  }

  public void setPage_locationsIsSet(boolean value) {
    if (!value) {
      this.page_locations = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case PAGE_LOCATIONS:
      if (value == null) {
        unsetPage_locations();
      } else {
        setPage_locations((java.util.List<PageLocation>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case PAGE_LOCATIONS:
      return getPage_locations();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case PAGE_LOCATIONS:
      return isSetPage_locations();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that instanceof OffsetIndex)
      return this.equals((OffsetIndex)that);
    return false;
  }

  public boolean equals(OffsetIndex that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_page_locations = true && this.isSetPage_locations();
    boolean that_present_page_locations = true && that.isSetPage_locations();
    if (this_present_page_locations || that_present_page_locations) {
      if (!(this_present_page_locations && that_present_page_locations))
        return false;
      if (!this.page_locations.equals(that.page_locations))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetPage_locations()) ? 131071 : 524287);
    if (isSetPage_locations())
      hashCode = hashCode * 8191 + page_locations.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(OffsetIndex other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.compare(isSetPage_locations(), other.isSetPage_locations());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPage_locations()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.page_locations, other.page_locations);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("OffsetIndex(");
    boolean first = true;

    sb.append("page_locations:");
    if (this.page_locations == null) {
      sb.append("null");
    } else {
      sb.append(this.page_locations);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (page_locations == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'page_locations' was not present! Struct: " + toString());
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
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class OffsetIndexStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public OffsetIndexStandardScheme getScheme() {
      return new OffsetIndexStandardScheme();
    }
  }

  private static class OffsetIndexStandardScheme extends org.apache.thrift.scheme.StandardScheme<OffsetIndex> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, OffsetIndex struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // PAGE_LOCATIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list56 = iprot.readListBegin();
                struct.page_locations = new java.util.ArrayList<PageLocation>(_list56.size);
                @org.apache.thrift.annotation.Nullable PageLocation _elem57;
                for (int _i58 = 0; _i58 < _list56.size; ++_i58)
                {
                  _elem57 = new PageLocation();
                  _elem57.read(iprot);
                  struct.page_locations.add(_elem57);
                }
                iprot.readListEnd();
              }
              struct.setPage_locationsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, OffsetIndex struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.page_locations != null) {
        oprot.writeFieldBegin(PAGE_LOCATIONS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.page_locations.size()));
          for (PageLocation _iter59 : struct.page_locations)
          {
            _iter59.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class OffsetIndexTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public OffsetIndexTupleScheme getScheme() {
      return new OffsetIndexTupleScheme();
    }
  }

  private static class OffsetIndexTupleScheme extends org.apache.thrift.scheme.TupleScheme<OffsetIndex> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, OffsetIndex struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      {
        oprot.writeI32(struct.page_locations.size());
        for (PageLocation _iter60 : struct.page_locations)
        {
          _iter60.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, OffsetIndex struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list61 = iprot.readListBegin(org.apache.thrift.protocol.TType.STRUCT);
        struct.page_locations = new java.util.ArrayList<PageLocation>(_list61.size);
        @org.apache.thrift.annotation.Nullable PageLocation _elem62;
        for (int _i63 = 0; _i63 < _list61.size; ++_i63)
        {
          _elem62 = new PageLocation();
          _elem62.read(iprot);
          struct.page_locations.add(_elem62);
        }
      }
      struct.setPage_locationsIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}
