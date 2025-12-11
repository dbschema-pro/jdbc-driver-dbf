package com.wisecoders.jdbc.dbf

import com.linuxense.javadbf.DBFReader
import com.wisecoders.common_lib.common_slf4j.slf4jLogger
import com.wisecoders.jdbc.dbf.io.H2Reader.FILES_META_TABLE
import com.wisecoders.jdbc.dbf.io.H2Reader.isFileTransferred
import com.wisecoders.jdbc.dbf.io.H2Reader.saveFileIntoFilesMeta
import com.wisecoders.jdbc.dbf.io.H2Reader.transfer
import com.wisecoders.jdbc.dbf.io.H2Writer
import com.wisecoders.jdbc.dbf.schema.Table
import java.io.File
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.nio.charset.Charset
import java.nio.file.Files
import java.sql.Blob
import java.sql.CallableStatement
import java.sql.Clob
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.NClob
import java.sql.PreparedStatement
import java.sql.SQLClientInfoException
import java.sql.SQLException
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Savepoint
import java.sql.Statement
import java.sql.Struct
import java.util.Properties
import java.util.concurrent.Executor
import java.util.regex.Matcher
import java.util.regex.Pattern
import org.h2.jdbc.JdbcConnection

/**
 * When you open a connection, we store transfer all DBF data to a local H2 database in user.home/.DbSchema/ .
 * We also create a proxy on Statement and intercept 'save dbf to folder_path' statements.
 * The dbf save code can be improved, we are happy for contributions.
 *
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/deed.en), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/dbf-jdbc-driver).
 */
class H2Connection internal constructor(
    private val h2Connection: JdbcConnection,
    private val defaultCharset: String?,
) : Connection {
    @Throws(SQLException::class)
    override fun createStatement(): Statement {
        val statement = h2Connection.createStatement()
        return StatementProxy(statement).proxyStatement
    }

    @Throws(SQLException::class)
    override fun createStatement(
        resultSetType: Int,
        resultSetConcurrency: Int,
    ): Statement {
        val statement = h2Connection.createStatement(resultSetType, resultSetConcurrency)
        return StatementProxy(statement).proxyStatement
    }

    @Throws(SQLException::class)
    override fun createStatement(
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int,
    ): Statement {
        val statement = h2Connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability)
        return StatementProxy(statement).proxyStatement
    }


    private inner class StatementProxy(private val target: Any) : InvocationHandler {
        val proxyStatement: Statement = Proxy.newProxyInstance(
            Statement::class.java.classLoader,
            arrayOf<Class<*>>(Statement::class.java),
            this
        ) as Statement

        @Throws(Throwable::class)
        override fun invoke(
            proxy: Any,
            method: Method,
            args: Array<Any>?,
        ): Any {
            var matcher: Matcher
            if (!args.isNullOrEmpty()) {
                if ((SAVE_COMMAND_PATTERN.matcher(args[0].toString()).also { matcher = it }).matches()) {
                    LOGGER.atInfo().setMessage(("Saving dbf...")).log()
                    val start = System.currentTimeMillis()
                    try {
                        saveDbf(matcher.group(5))
                    } catch (ex: SQLException) {
                        LOGGER.atError().setMessage("Error saving dbf").setCause(ex).log()
                        throw ex
                    } catch (ex: Exception) {
                        LOGGER.atError().setMessage("Error saving dbf").setCause(ex).log()
                        throw SQLException(ex.localizedMessage, ex)
                    }
                    val elapsed = System.currentTimeMillis() - start
                    LOGGER.atInfo().setMessage(("Executing " + method.name + " finished in " + elapsed + " ms")).log()
                    return true
                } else if ((RELOAD_PATTERN.matcher(args[0].toString()).also { matcher = it }).matches()) {
                    LOGGER.atInfo().setMessage(("Reload " + matcher.group(3))).log()
                    reload(matcher.group(3))
                    return true
                }
            }

            return if (args == null) {
                method.invoke(target)
            } else {
                method.invoke(target, *args)
            }
        }
    }

    @Throws(SQLException::class)
    fun transferFolder(
        dbfFolder: File,
        dbfSubFolder: File,
        h2Connection: Connection,
        defaultCharset: Charset?,
    ) {
        val files = dbfSubFolder.listFiles()
        if (files != null) {
            for (dbfFile in files) {
                if (dbfFile.isFile) {
                    if (dbfFile.name.lowercase().endsWith(".dbf")) {
                        try {
                            DBFReader(Files.newInputStream(dbfFile.toPath()), defaultCharset).use { reader ->
                                var memoFile =
                                    File(dbfFolder, dbfFile.name.substring(0, dbfFile.name.length - 4) + ".fpt")
                                if (memoFile.exists()) {
                                    reader.setMemoFile(memoFile)
                                } else {
                                    memoFile =
                                        File(dbfFolder, dbfFile.name.substring(0, dbfFile.name.length - 4) + ".dbt")
                                    if (memoFile.exists()) {
                                        reader.setMemoFile(memoFile)
                                    }
                                }
                                val table = Table(extractTableNameFrom(dbfFolder, dbfFile))
                                if (!isFileTransferred(dbfFile, h2Connection)) {
                                    transfer(table, reader, h2Connection)
                                    saveFileIntoFilesMeta(table, dbfFile, h2Connection)
                                }
                            }
                        } catch (ex: Exception) {
                            LOGGER.atError().setMessage("Error transferring $dbfFile").setCause(ex).log()
                            throw SQLException(ex.localizedMessage, ex)
                        }
                    }
                } else if (dbfFile.isDirectory) {
                    transferFolder(dbfFolder, dbfFile, h2Connection, defaultCharset)
                }
            }
        }
    }

    @Throws(Exception::class)
    private fun saveDbf(path: String) {
        var path = path
        if (path == null || path.trim { it <= ' ' }.isEmpty()) {
            throw SQLException("Save dbf path is empty. Please specify a directory path")
        }
        path = path.trim { it <= ' ' }
        if ((path.startsWith("'") || path.endsWith("'")) || (path.startsWith("\"") || path.endsWith("\""))) {
            path = path.substring(1, path.length - 1)
        }
        val outputFolder = File(path)
        outputFolder.mkdirs()
        H2Writer(h2Connection, outputFolder, defaultCharset)
    }

    @Throws(Exception::class)
    private fun reload(filePath: String) {
        h2Connection.prepareStatement("DELETE FROM $FILES_META_TABLE WHERE file_path=?").use { st ->
            st.setString(1, filePath)
            st.executeUpdate()
            val folder = (File(filePath)).parentFile

            if (!folder.exists()) {
                throw SQLException("Folder does not exists: '$folder'")
            }
            if (!folder.isDirectory) {
                throw SQLException("Expected path is not folder: '$folder'")
            }
            transferFolder(folder, folder, this.h2Connection, null)
        }
    }


    @Throws(SQLException::class)
    override fun prepareStatement(sql: String): PreparedStatement {
        return h2Connection.prepareStatement(sql)
    }

    @Throws(SQLException::class)
    override fun prepareCall(sql: String): CallableStatement {
        return h2Connection.prepareCall(sql)
    }

    @Throws(SQLException::class)
    override fun nativeSQL(sql: String): String {
        return h2Connection.nativeSQL(sql)
    }

    @Throws(SQLException::class)
    override fun setAutoCommit(autoCommit: Boolean) {
        h2Connection.autoCommit = autoCommit
    }

    @Throws(SQLException::class)
    override fun getAutoCommit(): Boolean {
        return h2Connection.autoCommit
    }

    @Throws(SQLException::class)
    override fun commit() {
        h2Connection.commit()
    }

    @Throws(SQLException::class)
    override fun rollback() {
        h2Connection.rollback()
    }

    @Throws(SQLException::class)
    override fun close() {
        h2Connection.close()
    }

    @Throws(SQLException::class)
    override fun isClosed(): Boolean {
        return h2Connection.isClosed
    }

    @Throws(SQLException::class)
    override fun getMetaData(): DatabaseMetaData {
        return h2Connection.metaData
    }

    @Throws(SQLException::class)
    override fun setReadOnly(readOnly: Boolean) {
        h2Connection.isReadOnly = readOnly
    }

    @Throws(SQLException::class)
    override fun isReadOnly(): Boolean {
        return h2Connection.isReadOnly
    }

    @Throws(SQLException::class)
    override fun setCatalog(catalog: String) {
        h2Connection.catalog = catalog
    }

    @Throws(SQLException::class)
    override fun getCatalog(): String {
        return h2Connection.catalog
    }

    @Throws(SQLException::class)
    override fun setTransactionIsolation(level: Int) {
        h2Connection.transactionIsolation = level
    }

    @Throws(SQLException::class)
    override fun getTransactionIsolation(): Int {
        return h2Connection.transactionIsolation
    }

    @Throws(SQLException::class)
    override fun getWarnings(): SQLWarning {
        return h2Connection.warnings
    }

    @Throws(SQLException::class)
    override fun clearWarnings() {
        h2Connection.clearWarnings()
    }

    @Throws(SQLException::class)
    override fun prepareStatement(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
    ): PreparedStatement {
        return h2Connection.prepareStatement(sql, resultSetType, resultSetConcurrency)
    }

    @Throws(SQLException::class)
    override fun prepareCall(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
    ): CallableStatement {
        return h2Connection.prepareCall(sql, resultSetType, resultSetConcurrency)
    }

    @Throws(SQLException::class)
    override fun getTypeMap(): Map<String, Class<*>> {
        return h2Connection.typeMap
    }

    @Throws(SQLException::class)
    override fun setTypeMap(map: Map<String?, Class<*>?>?) {
        h2Connection.typeMap = map
    }

    @Throws(SQLException::class)
    override fun setHoldability(holdability: Int) {
        h2Connection.holdability = holdability
    }

    @Throws(SQLException::class)
    override fun getHoldability(): Int {
        return h2Connection.holdability
    }

    @Throws(SQLException::class)
    override fun setSavepoint(): Savepoint {
        return h2Connection.setSavepoint()
    }

    @Throws(SQLException::class)
    override fun setSavepoint(name: String): Savepoint {
        return h2Connection.setSavepoint(name)
    }

    @Throws(SQLException::class)
    override fun rollback(savepoint: Savepoint) {
        h2Connection.rollback(savepoint)
    }

    @Throws(SQLException::class)
    override fun releaseSavepoint(savepoint: Savepoint) {
        h2Connection.releaseSavepoint(savepoint)
    }

    @Throws(SQLException::class)
    override fun prepareStatement(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int,
    ): PreparedStatement {
        return h2Connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
    }

    @Throws(SQLException::class)
    override fun prepareCall(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int,
    ): CallableStatement {
        return h2Connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability)
    }

    @Throws(SQLException::class)
    override fun prepareStatement(
        sql: String,
        autoGeneratedKeys: Int,
    ): PreparedStatement {
        return h2Connection.prepareStatement(sql, autoGeneratedKeys)
    }

    @Throws(SQLException::class)
    override fun prepareStatement(
        sql: String,
        columnIndexes: IntArray,
    ): PreparedStatement {
        return h2Connection.prepareStatement(sql, columnIndexes)
    }

    @Throws(SQLException::class)
    override fun prepareStatement(
        sql: String,
        columnNames: Array<String>,
    ): PreparedStatement {
        return h2Connection.prepareStatement(sql, columnNames)
    }

    @Throws(SQLException::class)
    override fun createClob(): Clob {
        return h2Connection.createClob()
    }

    @Throws(SQLException::class)
    override fun createBlob(): Blob {
        return h2Connection.createBlob()
    }

    @Throws(SQLException::class)
    override fun createNClob(): NClob {
        return h2Connection.createNClob()
    }

    @Throws(SQLException::class)
    override fun createSQLXML(): SQLXML {
        return h2Connection.createSQLXML()
    }

    override fun isValid(timeout: Int): Boolean {
        return h2Connection.isValid(timeout)
    }

    @Throws(SQLClientInfoException::class)
    override fun setClientInfo(
        name: String,
        value: String,
    ) {
        h2Connection.setClientInfo(name, value)
    }

    @Throws(SQLClientInfoException::class)
    override fun setClientInfo(properties: Properties) {
        h2Connection.clientInfo = properties
    }

    @Throws(SQLException::class)
    override fun getClientInfo(name: String): String {
        return h2Connection.getClientInfo(name)
    }

    @Throws(SQLException::class)
    override fun getClientInfo(): Properties {
        return h2Connection.clientInfo
    }

    @Throws(SQLException::class)
    override fun createArrayOf(
        typeName: String,
        elements: Array<Any>,
    ): java.sql.Array {
        return h2Connection.createArrayOf(typeName, elements)
    }

    @Throws(SQLException::class)
    override fun createStruct(
        typeName: String,
        attributes: Array<Any>,
    ): Struct {
        return h2Connection.createStruct(typeName, attributes)
    }

    @Throws(SQLException::class)
    override fun setSchema(schema: String) {
        h2Connection.schema = schema
    }

    @Throws(SQLException::class)
    override fun getSchema(): String {
        return h2Connection.schema
    }

    @Throws(SQLException::class)
    override fun abort(executor: Executor) {
        h2Connection.abort(executor)
    }

    @Throws(SQLException::class)
    override fun setNetworkTimeout(
        executor: Executor,
        milliseconds: Int,
    ) {
        h2Connection.setNetworkTimeout(executor, milliseconds)
    }

    @Throws(SQLException::class)
    override fun getNetworkTimeout(): Int {
        return h2Connection.networkTimeout
    }

    @Throws(SQLException::class)
    override fun <T> unwrap(iface: Class<T>): T {
        return h2Connection.unwrap(iface)
    }

    @Throws(SQLException::class)
    override fun isWrapperFor(iface: Class<*>?): Boolean {
        return h2Connection.isWrapperFor(iface)
    }

    companion object {
        private val LOGGER = slf4jLogger()

        private val SAVE_COMMAND_PATTERN: Pattern =
            Pattern.compile("(\\s*)save(\\s+)dbf(\\s+)to(\\s+)(.*)", Pattern.CASE_INSENSITIVE)
        private val RELOAD_PATTERN: Pattern = Pattern.compile("(\\s*)reload(\\s+)(.*)", Pattern.CASE_INSENSITIVE)

        private fun extractTableNameFrom(
            dbfFolder: File,
            dbfFile: File,
        ): String {
            var relativePath = dbfFolder.toURI().relativize(dbfFile.toURI()).path
            if (relativePath.lowercase().endsWith(".dbf")) {
                relativePath = relativePath.substring(0, relativePath.length - ".dbf".length)
            }
            return relativePath
        }
    }
}
