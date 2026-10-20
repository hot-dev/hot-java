plugins {
    `java-library`
    id("com.vanniktech.maven.publish") version "0.34.0"
}

group = property("GROUP") as String
version = property("VERSION_NAME") as String

repositories {
    mavenCentral()
}

// CI overrides this (-PjavaToolchainVersion=17) to test on the Java baseline.
val javaToolchainVersion = (findProperty("javaToolchainVersion") as String?)?.toInt() ?: 21

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(javaToolchainVersion)
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release = 17
    options.encoding = "UTF-8"
}

tasks.jar {
    manifest {
        attributes["Implementation-Title"] = "hot-sdk"
        attributes["Implementation-Version"] = version
    }
}

tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}

dependencies {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.3")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.wiremock:wiremock:3.10.0")
}

tasks.test {
    useJUnitPlatform()
    // A new disposable server must always execute the live tests, even if
    // Gradle's compiled test inputs are unchanged. Do not fingerprint tokens.
    outputs.upToDateWhen {
        !providers.environmentVariable("HOT_TEST_API_KEY").isPresent &&
            !providers.environmentVariable("HOT_TEST_SUBORG_FIXTURE").isPresent
    }
    // Forward the integration-test variables from the *client* environment.
    // The forked test JVM otherwise inherits the daemon's stale environment.
    for (key in listOf("HOT_TEST_API_KEY", "HOT_TEST_BASE_URL", "HOT_TEST_SUBORG_FIXTURE")) {
        val value = providers.environmentVariable(key)
        if (value.isPresent) {
            environment(key, value.get())
        }
    }
}

mavenPublishing {
    publishToMavenCentral()
    signAllPublications()

    coordinates(
        property("GROUP") as String,
        property("ARTIFACT_ID") as String,
        property("VERSION_NAME") as String,
    )

    pom {
        name = "Hot Dev SDK"
        description = "Official Java SDK for the Hot Dev API"
        url = "https://github.com/hot-dev/hot-java"
        licenses {
            license {
                name = "Apache-2.0"
                url = "https://www.apache.org/licenses/LICENSE-2.0"
            }
        }
        developers {
            developer {
                id = "hot-dev"
                name = "Hot Dev"
                url = "https://hot.dev"
            }
        }
        scm {
            url = "https://github.com/hot-dev/hot-java"
            connection = "scm:git:git://github.com/hot-dev/hot-java.git"
            developerConnection = "scm:git:ssh://git@github.com/hot-dev/hot-java.git"
        }
    }
}
