plugins {
    alias(libs.plugins.wisecoders.commonGradle.jdbcDriver)
}

group = "com.wisecoders.jdbc-drivers"

jdbcDriver {
    dbId = "DBF"
}

dependencies {
    implementation(libs.wisecoders.commonLib.commonSlf4j)
    implementation(libs.slf4j.api)
    implementation(libs.javadbf)
    implementation(libs.h2)

     runtimeOnly(libs.logback.classic)
}
