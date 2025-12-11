package com.wisecoders.jdbc.dbf.schema

/**
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/deed.en), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/dbf-jdbc-driver).
 */
class Db {
    val tables: MutableMap<String, Table> = HashMap()

    fun getOrCreateTable(tableName: String): Table {
        return tables.getOrPut(tableName) { Table( tableName) }
    }

    fun getTables(): Collection<Table> {
        return tables.values
    }
}
