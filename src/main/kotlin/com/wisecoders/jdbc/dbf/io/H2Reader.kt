package com.wisecoders.jdbc.dbf.io

import com.linuxense.javadbf.DBFField
import com.linuxense.javadbf.DBFReader
import com.wisecoders.common_lib.common_slf4j.slf4jLogger
import com.wisecoders.jdbc.dbf.io.DBFUtil.getFieldDescription
import com.wisecoders.jdbc.dbf.schema.DBFDataTypeUtil
import com.wisecoders.jdbc.dbf.schema.Table
import java.io.File
import java.sql.Connection
import java.sql.SQLException
import kotlin.math.max

/**
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/deed.en), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/dbf-jdbc-driver).
 */
object H2Reader {
    private const val QUOTE_CHAR = '"'
    const val COLUMNS_META_TABLE: String = "dbs_meta_columns"
    private const val INSERT_INTO_COLUMNS_META_TABLE =
        "insert into $COLUMNS_META_TABLE( table_name, column_name, column_type, length, decimal ) values ( ?,?,?,?,? )"

    const val FILES_META_TABLE: String = "dbs_meta_files"
    private const val CREATE_META_FILES =
        "CREATE TABLE IF NOT EXISTS $FILES_META_TABLE ( file_path varchar(250) NOT NULL PRIMARY KEY, table_name varchar(2000) NOT NULL, size bigint NOT NULL, last_modified bigint NOT NULL ) "


    @JvmStatic
    @Throws(SQLException::class)
    fun isFileTransferred(
        file: File,
        h2Connection: Connection,
    ): Boolean {
        // Ensure meta table exists
        h2Connection.prepareStatement(CREATE_META_FILES).use { it.execute() }
        h2Connection.commit()

        val query = "SELECT size, last_modified FROM $FILES_META_TABLE WHERE file_path = ?"
        h2Connection.prepareStatement(query).use { st ->
            st.setString(1, file.absolutePath)
            st.executeQuery().use { rs ->
                if (rs.next()) {
                    val (storedSize, storedLastModified) = rs.getLong(1) to rs.getLong(2)
                    if (storedSize == file.length() && storedLastModified == file.lastModified()) {
                        LOGGER.atInfo()
                            .setMessage("File '${file.absolutePath}' is already loaded in H2.")
                            .log()
                        return true
                    }
                }
            }
        }

        return false
    }


    @JvmStatic
    @Throws(SQLException::class)
    fun saveFileIntoFilesMeta(
        table: Table,
        file: File,
        h2Connection: Connection,
    ) {
        val sql = """
        MERGE INTO $FILES_META_TABLE 
        (file_path, table_name, size, last_modified) 
        KEY(file_path) 
        VALUES (?, ?, ?, ?)
    """.trimIndent()

        h2Connection.prepareStatement(sql).use { st ->
            st.setString(1, file.absolutePath)
            st.setString(2, table.name)
            st.setLong(3, file.length())
            st.setLong(4, file.lastModified())
            st.executeUpdate()
        }
        // Optionally commit if autocommit is false
        // h2Connection.commit()
    }

    @JvmStatic
    @Throws(Exception::class)
    fun transfer(
        table: Table,
        reader: DBFReader,
        h2Connection: Connection,
    ) {
        createH2MetaTable(h2Connection)
        deleteFromH2MetaTable(h2Connection, table)

        LOGGER.atInfo().setMessage("Transfer table '${table.name}' definition.").log()

        val dbfFields = List(reader.fieldCount) { reader.getField(it) }

        // Save metadata & log field descriptions
        val fieldDescriptions = dbfFields.joinToString("\n") { field ->
            saveFieldInMetaTable(h2Connection, table, field)
            "\t${getFieldDescription(field)}"
        }
        LOGGER.atInfo().setMessage("Table ${table.name}\n$fieldDescriptions").log()

        // Build SQL statements
        val dropSQL = "DROP TABLE IF EXISTS $QUOTE_CHAR${table.name}$QUOTE_CHAR"
        val createSQL = buildString {
            append("CREATE TABLE $QUOTE_CHAR${table.name}$QUOTE_CHAR (\n")
            append(dbfFields.joinToString(",\n") { field ->
                "\t$QUOTE_CHAR${field.name.lowercase()}$QUOTE_CHAR ${DBFDataTypeUtil.getH2Type(field)}"
            })
            append("\n)")
        }
        val insertSQL = buildString {
            append("INSERT INTO $QUOTE_CHAR${table.name}$QUOTE_CHAR(")
            append(dbfFields.joinToString(",") { "$QUOTE_CHAR${it.name.lowercase()}$QUOTE_CHAR" })
            append(") VALUES (")
            append(dbfFields.joinToString(",") { "?" })
            append(")")
        }

        // Drop & create table
        h2Connection.prepareStatement(dropSQL).use {
            LOGGER.atInfo().setMessage(dropSQL).log()
            it.execute()
        }
        h2Connection.commit()

        h2Connection.prepareStatement(createSQL).use {
            LOGGER.atInfo().setMessage(createSQL).log()
            it.execute()
        }
        h2Connection.commit()

        // Insert rows
        val batchSize = max(50, 500 - (dbfFields.size * 3))
        val startTime = System.currentTimeMillis()
        var count = 0
        var pendingBatch = 0

        h2Connection.prepareStatement(insertSQL).use { stInsert ->
            LOGGER.atInfo().setMessage("Transfer '${table.name}' data...").log()

            while (true) {
                val record = reader.nextRecord() ?: break
                try {
                    dbfFields.forEachIndexed { i, field ->
                        val value = record.getOrNull(i)
                        if (value == null) {
                            stInsert.setNull(i + 1, DBFDataTypeUtil.getJavaType(field))
                        } else {
                            stInsert.setObject(i + 1, value)
                        }
                    }
                    stInsert.addBatch()
                    count++
                    pendingBatch++

                    if (pendingBatch >= batchSize) {
                        stInsert.executeBatch()
                        h2Connection.commit()
                        pendingBatch = 0
                    }
                } catch (ex: Exception) {
                    LOGGER.atError().setMessage(stInsert.toString()).setCause(ex).log()
                    throw ex
                }
            }

            if (pendingBatch > 0) {
                stInsert.executeBatch()
            }
            h2Connection.commit()
        }

        LOGGER.atInfo().setMessage(
            "------- Transferred '${table.name}' $count records in ${System.currentTimeMillis() - startTime} ms"
        ).log()
    }


    private const val CREATE_COLUMNS_META_TABLE = "create table if not exists " + COLUMNS_META_TABLE + "( " +
            "table_name varchar(2000) not null, " +
            "column_name varchar(2000) not null, " +
            "column_type varchar(120), " +
            "length int not null, " +
            "decimal int not null, " +
            "primary key (table_name, column_name))"


    @Throws(SQLException::class)
    private fun createH2MetaTable(h2Connection: Connection) {
        val st = h2Connection.createStatement()
        LOGGER.atInfo().setMessage(CREATE_COLUMNS_META_TABLE).log()
        st.execute(CREATE_COLUMNS_META_TABLE)
        st.close()
        h2Connection.commit()
    }

    private const val DELETE_FROM_META_TABLE = "delete from $COLUMNS_META_TABLE WHERE table_name=?"

    @Throws(SQLException::class)
    private fun deleteFromH2MetaTable(
        h2Connection: Connection,
        table: Table,
    ) {
        LOGGER.atInfo().setMessage("Execute: $DELETE_FROM_META_TABLE").log()
        val st = h2Connection.prepareStatement(DELETE_FROM_META_TABLE)
        st.setString(1, table.name)
        st.executeUpdate()
        st.close()
        h2Connection.commit()
    }


    @Throws(SQLException::class)
    private fun saveFieldInMetaTable(
        h2Connection: Connection,
        table: Table,
        field: DBFField,
    ) {
        h2Connection.prepareStatement(INSERT_INTO_COLUMNS_META_TABLE).use { st ->
            st.setString(1, table.name)
            st.setString(2, field.name.lowercase())
            st.setString(3, field.type.name)
            st.setInt(4, field.length)
            st.setInt(5, field.decimalCount)
            st.execute()
        }
        h2Connection.commit() // keep if autocommit is false
    }

    private val LOGGER = slf4jLogger()

}
