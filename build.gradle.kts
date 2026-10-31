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
    // The BOM aligns jackson-core and jackson-annotations with databind. The published POM
    // carries the resolved 2.18.12 versions (see versionMapping below).
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.18.12"))
    implementation("com.fasterxml.jackson.core:jackson-databind")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.wiremock:wiremock-jetty12:3.13.2")

    // Test-only floors for WireMock's transitive dependencies with published advisories (OSV,
    // 2026-10-30). Tests also run against the Jackson version the SDK ships. Handlebars stays at
    // WireMock's 4.3.1: 4.5 (which fixes GHSA-r4gv-qr8j-p3pg) removed helpers WireMock 3.13 loads,
    // and these tests use no response templating.
    testImplementation(enforcedPlatform("com.fasterxml.jackson:jackson-bom:2.18.12"))
    testImplementation(platform("org.eclipse.jetty:jetty-bom:12.0.40"))
    testImplementation(platform("org.eclipse.jetty.ee10:jetty-ee10-bom:12.0.40"))
    constraints {
        testImplementation("com.google.guava:guava:33.7.2-jre")
        testImplementation("org.apache.httpcomponents.client5:httpclient5:5.6.5")
        testImplementation("org.apache.httpcomponents.core5:httpcore5:5.4.4")
        testImplementation("org.apache.httpcomponents.core5:httpcore5-h2:5.4.4")
    }
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

// Publish resolved versions, so the POM pins jackson-databind 2.18.12 explicitly rather than
// leaving it to the imported BOM.
publishing {
    publications.withType<MavenPublication>().configureEach {
        versionMapping {
            allVariants {
                fromResolutionResult()
            }
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
