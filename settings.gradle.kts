plugins {
    // Auto-provisions the JDK toolchain when the local one doesn't match.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "hot-sdk"
