package com.gs.rules.engine.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.util.List;

public class TypeUtil {

  public static ExternalTypeInfo<Row> convertExternalTypeInfo(ExternalTypeInfo<Row> externalTypeInfo) {
    FieldsDataType fieldsDataType = (FieldsDataType)externalTypeInfo.getDataType();
    RowType rowType = (RowType)fieldsDataType.getLogicalType();
    List<RowType.RowField> rowTypeFields = rowType.getFields();
    DataTypes.Field[] dataTypeFields = new DataTypes.Field[rowTypeFields.size()+1];
    for (int i=0; i<rowTypeFields.size(); i++) {
      RowType.RowField field = rowTypeFields.get(i);
      dataTypeFields[i] = DataTypes.FIELD(field.getName(), logicalType2DataTypes(field.getType()));
    }
    //增加distribute字段
    dataTypeFields[rowTypeFields.size()] =  DataTypes.FIELD("distribute", DataTypes.BOOLEAN());
    return ExternalTypeInfo.of(DataTypes.ROW(dataTypeFields));
  }

  private static DataType logicalType2DataTypes(LogicalType logicalType) {
    String typeName = logicalType.getClass().getName();
    if (typeName.equals(DataTypes.STRING().getLogicalType().getClass().getName())) {
      return DataTypes.STRING();
    } else if (typeName.equals(DataTypes.BOOLEAN().getLogicalType().getClass().getName())) {
      return DataTypes.BOOLEAN();
    }
    return null;
  }
}
