package com.wisecoders.jdbc.dbf.io

import com.linuxense.javadbf.DBFField

/**
 * Licensed under [CC BY-ND 4.0 DEED](https://creativecommons.org/licenses/by-nd/4.0/deed.en), copyright [Wise Coders GmbH](https://wisecoders.com), used by [DbSchema Database Designer](https://dbschema.com).
 * Code modifications allowed only as pull requests to the [public GIT repository](https://github.com/wise-coders/dbf-jdbc-driver).
 */
object DBFUtil {
    @JvmStatic
    fun getFieldDescription(field: DBFField): String {
        return field.name.lowercase() + " " +
                field.type.name + "(" +
                field.length + "," +
                field.decimalCount + ")"
    }
}
