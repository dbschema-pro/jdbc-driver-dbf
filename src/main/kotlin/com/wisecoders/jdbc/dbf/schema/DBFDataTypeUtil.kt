package com.wisecoders.jdbc.dbf.schema

import com.linuxense.javadbf.DBFDataType
import com.linuxense.javadbf.DBFField
import com.wisecoders.jdbc.dbf.io.H2Reader
import java.sql.Types

/**
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/deed.en), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/dbf-jdbc-driver).
 */
object DBFDataTypeUtil {
    fun getJavaType(field: DBFField): Int {
        return when (field.type) {
            DBFDataType.UNKNOWN, DBFDataType.CURRENCY -> Types.OTHER
            DBFDataType.VARBINARY, DBFDataType.BLOB, DBFDataType.GENERAL_OLE -> Types.BLOB
            DBFDataType.NUMERIC, DBFDataType.LONG, DBFDataType.DOUBLE -> Types.DECIMAL
            DBFDataType.AUTOINCREMENT -> Types.INTEGER
            DBFDataType.TIMESTAMP -> Types.TIMESTAMP
            DBFDataType.TIMESTAMP_DBASE7 -> Types.TIMESTAMP_WITH_TIMEZONE
            DBFDataType.NULL_FLAGS -> Types.NULL
            DBFDataType.FLOATING_POINT -> Types.FLOAT
            DBFDataType.CHARACTER -> Types.CHAR
            DBFDataType.LOGICAL -> Types.BOOLEAN
            DBFDataType.DATE -> Types.DATE
            DBFDataType.MEMO -> Types.LONGNVARCHAR
            DBFDataType.PICTURE, DBFDataType.BINARY -> Types.BINARY
            DBFDataType.VARCHAR -> Types.VARCHAR
            else -> Types.VARCHAR
        }
    }

    fun getH2Type(field: DBFField): String {
        return when (field.type) {
            DBFDataType.BINARY, DBFDataType.DOUBLE -> "double"
            DBFDataType.FLOATING_POINT -> "float"
            DBFDataType.CHARACTER -> "char(" + field.length + ")"
            DBFDataType.LOGICAL -> "boolean"
            DBFDataType.DATE -> "date"
            DBFDataType.MEMO -> "longvarchar(1048575)"
            DBFDataType.VARCHAR -> "varchar(" + field.length + ")"
            DBFDataType.PICTURE, DBFDataType.UNKNOWN, DBFDataType.BLOB, DBFDataType.GENERAL_OLE -> "binary"
            DBFDataType.VARBINARY, DBFDataType.NUMERIC -> "decimal(" + (field.length + field.decimalCount) + "," + field.decimalCount + ")"
            DBFDataType.LONG, DBFDataType.CURRENCY, DBFDataType.AUTOINCREMENT -> "bigint"
            DBFDataType.TIMESTAMP -> "timestamp"
            DBFDataType.TIMESTAMP_DBASE7 -> "timestamp with time zone"
            DBFDataType.NULL_FLAGS -> "bit"
            else -> "varchar(" + Int.MAX_VALUE + ")"
        }
    }


    fun isH2SystemTable(tableName: String?): Boolean {
        return H2Reader.COLUMNS_META_TABLE.equals(tableName, ignoreCase = true) || H2Reader.FILES_META_TABLE.equals(
            tableName,
            ignoreCase = true
        )
    }
}
