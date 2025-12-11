package com.wisecoders.jdbc.dbf

import com.wisecoders.common_lib.common_slf4j.slf4jLogger
import java.io.File
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.NoSuchAlgorithmException
import java.sql.Connection
import java.sql.Driver
import java.sql.DriverManager
import java.sql.DriverPropertyInfo
import java.sql.SQLException
import java.util.Properties
import java.util.logging.Level
import java.util.logging.Logger
import org.h2.jdbc.JdbcConnection


/**
 * When you open a connection for the first time in a JVM, we store transfer all DBF data to a local H2 database in user.home/.DbSchema/ .
 * We also create a proxy on Statement and intercept 'save dbf to folder_path' statements.
 * The dbf save code can be improved, we are happy for contributions.
 *
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/deed.en), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/dbf-jdbc-driver).
 */
class JdbcDriver : Driver {
    @Throws(SQLException::class)
    override fun connect(
        url: String,
        info: Properties,
    ): Connection {
        if (!acceptsURL(PREFIX)) {
            throw SQLException("Incorrect URL. Expected jdbc:dbschema:dbf:<folderPath>")
        }

        var defaultCharset: String? = null
        var path = url.substring(PREFIX.length)
        val idxQuestionMark = path.indexOf('?')
        if (idxQuestionMark > 0) {
            val params = path.substring(idxQuestionMark + 1)
            path = path.substring(0, idxQuestionMark)
            for (paramSet in params.split("&".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) {
                val pair = paramSet.split("=".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                if (pair.size == 2) {
                    if ("log".equals(pair[0], ignoreCase = true) || "logs".equals(pair[0], ignoreCase = true)) {
                    } else if ("charset".equals(pair[0], ignoreCase = true)) {
                        defaultCharset = pair[1]
                    }
                }
            }
        }
        return getConnection(path, defaultCharset)
    }

    private val h2Databases: MutableList<String?> = ArrayList()


    @Throws(SQLException::class)
    private fun getConnection(
        databasePath: String,
        defaultCharsetName: String?,
    ): Connection {
        val folder = File(databasePath)
        if (!folder.exists()) {
            throw SQLException("Folder does not exists: '$folder'")
        }
        if (!folder.isDirectory) {
            throw SQLException("Expected path is not folder: '$folder'")
        }
        val h2DbName = md5Java(databasePath)
        val h2DatabasePath = getH2DatabasePath(h2DbName)
        val h2JdbcUrl =
            "jdbc:h2:file:$h2DatabasePath;database_to_lower=true;case_insensitive_identifiers=true"
        //final String h2JdbcUrl = "jdbc:h2:mem:dbfdriver;database_to_lower=true";
        LOGGER.atInfo().setMessage(
            "Create H2 database '$h2JdbcUrl'"
        ).log()

        val h2NativeConnection = (org.h2.Driver().connect(h2JdbcUrl, Properties())) as JdbcConnection
        val h2Connection = H2Connection(h2NativeConnection, defaultCharsetName)
        if (!h2Databases.contains(h2DbName)) {
            h2Connection.transferFolder(
                folder,
                folder,
                h2NativeConnection,
                if (defaultCharsetName != null) Charset.forName(defaultCharsetName) else null
            )
            h2Databases.add(h2DbName)
        }
        return h2Connection
    }


    private fun getH2DatabasePath(h2DbName: String?): String {
        return H2_LOCATION + h2DbName
    }

    override fun acceptsURL(url: String): Boolean {
        return url.startsWith(PREFIX)
    }

    internal class ExtendedDriverPropertyInfo(
        name: String?,
        value: String?,
        choices: Array<String>?,
        description: String?,
    ) :
        DriverPropertyInfo(name, value) {
        init {
            this.description = description
            this.choices = choices
        }
    }

    override fun getPropertyInfo(
        url: String,
        info: Properties,
    ): Array<DriverPropertyInfo> {
        val result = arrayOf<DriverPropertyInfo>(ExtendedDriverPropertyInfo("log", "true", arrayOf("true", "false"), "Activate driver INFO logging"))
        return result
    }

    override fun getMajorVersion(): Int {
        return 1
    }


    override fun getMinorVersion(): Int {
        return 0
    }

    override fun jdbcCompliant(): Boolean {
        return true
    }

    override fun getParentLogger(): Logger? {
        return null
    }

    companion object {
        private const val PREFIX = "jdbc:dbschema:dbf:"
        private val H2_LOCATION = "${System.getProperty("user.home")}/.DbSchema/data/jdbc-dbf-cache/"

        private val LOGGER: org.slf4j.Logger = slf4jLogger()

        init {
            DriverManager.registerDriver(JdbcDriver())
        }

        private fun md5Java(message: String): String? {
            var digest: String? = null
            try {
                val md = MessageDigest.getInstance("MD5")
                val hash = md.digest(message.toByteArray(StandardCharsets.UTF_8))

                //converting byte array to Hexadecimal String
                val sb = StringBuilder(2 * hash.size)
                for (b in hash) {
                    sb.append(String.format("%02x", b.toInt() and 0xff))
                }

                digest = sb.toString()
            } catch (ex: NoSuchAlgorithmException) {
                Logger.getLogger(JdbcDriver::class.java.name).log(Level.SEVERE, null, ex)
            }
            return digest
        }
    }
}
