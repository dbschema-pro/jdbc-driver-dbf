package com.wisecoders.jdbc.dbf.io

import com.linuxense.javadbf.DBFWriter
import com.wisecoders.common_lib.common_slf4j.slf4jLogger
import com.wisecoders.jdbc.dbf.io.H2Reader.saveFileIntoFilesMeta
import com.wisecoders.jdbc.dbf.schema.DBFDataTypeUtil
import com.wisecoders.jdbc.dbf.schema.Db
import java.io.File
import java.io.FileOutputStream
import java.nio.charset.Charset
import java.sql.Connection
import java.sql.SQLException


/**
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/deed.en), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/dbf-jdbc-driver).
 */
class H2Writer(
    private val h2Connection: Connection,
    outputFolder: File,
    charset: String?,
) {

    init {
        val db = Db()

        // Read H2 metadata and populate Db tables
        h2Connection.metaData.getColumns(null, null, null, null).use { rsColumns ->
            while (rsColumns.next()) {
                val schemaName = rsColumns.getString(2)
                val tableName = rsColumns.getString(3)
                if (!schemaName.equals("INFORMATION_SCHEMA", ignoreCase = true) &&
                    !DBFDataTypeUtil.isH2SystemTable(tableName)
                ) {
                    val columnName = rsColumns.getString(4)
                    db.getOrCreateTable(tableName).createDBFField(
                        columnName,
                        rsColumns.getString(6),
                        rsColumns.getInt(7),
                        rsColumns.getInt(9)
                    )
                }
            }
        }

        // Export each table
        for (table in db.tables.values) {
            val outputFile = File(outputFolder, "${table.name}.dbf")
            LOGGER.atInfo().setMessage(("Saving $table")).log()

            FileOutputStream(outputFile).use { os ->
                val writer = if (charset != null) DBFWriter(os, Charset.forName(charset)) else DBFWriter(os)
                writer.use {
                    it.setFields(table.dBFFields)

                    h2Connection.createStatement().use { st ->
                        st.executeQuery("SELECT * FROM ${table.name}").use { rs ->
                            var recCount = 0
                            while (rs.next()) {
                                val columnCount = rs.metaData.columnCount
                                val data = Array<Any?>(columnCount) { i -> rs.getObject(i + 1) }

                                try {
                                    it.addRecord(data)
                                } catch (ex: Throwable) {
                                    throw SQLException("Error saving ${outputFile.absolutePath} record: ${formatRecord(data)}", ex)
                                }
                                recCount++
                            }
                            LOGGER.atInfo().setMessage("Saved ${table.name} $recCount records.").log()
                        }
                    }
                    saveFileIntoFilesMeta(table, outputFile, h2Connection)
                }
            }
        }
    }

    companion object {
        private val LOGGER = slf4jLogger()
    }

    private fun formatRecord(data: Array<Any?>): String =
        data.joinToString(prefix = "[", postfix = "]") { value ->
            when (value) {
                null -> "null"
                else -> "'${value}'"
            }
        }

}
