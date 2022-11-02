package com.gs.rules.engine.util;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
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
      if ("pt".equals(field.getName())) {
        continue;
      }
      dataTypeFields[i] = DataTypes.FIELD(field.getName(), logicalType2DataTypes(field.getType()));
    }
    //增加distribute和分区字段
    dataTypeFields[rowTypeFields.size()-1] =  DataTypes.FIELD("distribute", DataTypes.BOOLEAN());
    dataTypeFields[rowTypeFields.size()] =  DataTypes.FIELD("pt", DataTypes.STRING());
    return ExternalTypeInfo.of(DataTypes.ROW(dataTypeFields));
  }

  public static Schema convert2Schema(ExternalTypeInfo<Row> externalTypeInfo) {
    FieldsDataType fieldsDataType = (FieldsDataType)externalTypeInfo.getDataType();
    RowType rowType = (RowType)fieldsDataType.getLogicalType();
    List<RowType.RowField> rowTypeFields = rowType.getFields();
    Schema.Builder builder = Schema.newBuilder();
    for (int i=0; i<rowTypeFields.size(); i++) {
      RowType.RowField field = rowTypeFields.get(i);
      if ("pt".equals(field.getName())) {
        continue;
      }
      builder.column(field.getName(), logicalType2DataTypes(field.getType()));
    }
    builder.column("distribute", DataTypes.BOOLEAN());
    builder.column("pt", DataTypes.STRING());
    return builder.build();
  }

  private static DataType logicalType2DataTypes(LogicalType logicalType) {
    String typeName = logicalType.getClass().getName();
    if (typeName.equals(DataTypes.STRING().getLogicalType().getClass().getName())) {
      return DataTypes.STRING();
    } else if (typeName.equals(DataTypes.BOOLEAN().getLogicalType().getClass().getName())) {
      return DataTypes.BOOLEAN();
    } else if (typeName.equals(DataTypes.BIGINT().getLogicalType().getClass().getName())) {
      return DataTypes.BIGINT();
    } else if (typeName.equals(DataTypes.INT().getLogicalType().getClass().getName())) {
      return DataTypes.INT();
    } else if (typeName.equals(DataTypes.TIMESTAMP().getLogicalType().getClass().getName())) {
      return DataTypes.TIMESTAMP();
    } else if (typeName.equals(DataTypes.DATE().getLogicalType().getClass().getName())) {
      return DataTypes.DATE();
    } else if (typeName.equals(DataTypes.DOUBLE().getLogicalType().getClass().getName())) {
      return DataTypes.DOUBLE();
    } else if (typeName.equals(DataTypes.FLOAT().getLogicalType().getClass().getName())) {
      return DataTypes.FLOAT();
    }
    throw new RuntimeException(String.format("unsupported type of %s", typeName));
  }
}
