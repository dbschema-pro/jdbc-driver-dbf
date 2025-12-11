package com.wisecoders.jdbc.dbf.schema

import com.linuxense.javadbf.DBFDataType
import com.linuxense.javadbf.DBFField
import kotlin.math.min

/**
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/deed.en), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/dbf-jdbc-driver).
 */
class Table(val name: String) {
    val fields: MutableList<DBFField> = ArrayList()


    fun createDBFField(
        name: String,
        type: String,
        length: Int,
        decimal: Int
    ) {
        var name = name
        val field = DBFField()
        if (name.length > 10) {
            name = name.substring(0, 10)
        }
        field.name = name.uppercase()

        when (type.lowercase()) {
            "double", "decimal" -> {
                field.type = DBFDataType.NUMERIC
                field.length = length
                field.decimalCount = decimal
            }

            "float" -> field.type = DBFDataType.FLOATING_POINT
            "int" -> field.type = DBFDataType.AUTOINCREMENT
            "bigint" -> field.type = DBFDataType.NUMERIC
            "boolean" -> field.type = DBFDataType.LOGICAL
            "date" -> field.type = DBFDataType.DATE
            "bit" -> field.type = DBFDataType.NULL_FLAGS
            "longvarchar" -> field.type = DBFDataType.MEMO
            "timestamp" -> if (DBFDataType.TIMESTAMP.isWriteSupported) {
                field.type = DBFDataType.TIMESTAMP
            } else {
                field.type = DBFDataType.CHARACTER
                field.length = 256
            }

            "timestampwithtimezone" -> field.type = DBFDataType.TIMESTAMP_DBASE7
            "character" -> {
                field.type = DBFDataType.CHARACTER
                field.length = length
            }

            else -> {
                // I TRIED ALSO DBFDataType.VARCHAR AND THE LIBRARY DOES NOT SUPPORT WRITING IT. MEMO ALSO NOT.
                field.type = DBFDataType.CHARACTER
                field.length = min(length.toDouble(), 254.0).toInt()
            }
        }
        fields.add(field)
    }

    val dBFFields: Array<DBFField>
        get() = fields.toTypedArray()

    override fun toString(): String {
        val sb = StringBuilder(name).append("(\n")
        for (field in fields) {
            sb.append(field.name.uppercase()).append(" ").append(field.type).append(" (")
                .append(field.length).append(", ").append(field.decimalCount).append(" ) ")
                .append(if (field.isNullable) "" else "NOT NULL ").append("\n")
        }
        sb.append(")")
        return sb.toString()
    }
}
