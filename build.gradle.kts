import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask
import com.github.benmanes.gradle.versions.updates.resolutionstrategy.ComponentSelectionWithCurrent
import org.gradle.kotlin.dsl.withType
import com.vanniktech.maven.publish.KotlinJvm
import com.vanniktech.maven.publish.JavadocJar
import org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jmailen.gradle.kotlinter.tasks.LintTask
import kotlin.jvm.java

val vertxVersion = "5.0.5"
val awsSdkVersion = "2.39.2"
val junit5Version = "6.0.1"
val logbackVersion = "1.5.21"
val localstackVersion = "0.2.23"

val branchName = gitBranch()
val groupValue: String = "no.solibo.oss"
val versionValue: String = calculateVersion(properties["version"] as String, branchName)

repositories {
  mavenCentral()
}

plugins {
    `java-library`
    id("com.github.ben-manes.versions") version "0.53.0"
    id("org.jmailen.kotlinter") version "5.3.0"
    id("com.adarshr.test-logger") version "4.0.0"
    id("com.vanniktech.maven.publish") version "0.35.0"
    id("org.jetbrains.dokka") version "2.1.0"

  kotlin("jvm") version "2.2.21"
}

group = groupValue
version = versionValue

fun isNonStable(version: String): Boolean {
  val stableKeyword = listOf("RELEASE", "FINAL", "GA").any { version.uppercase().contains(it) }
  val regex = "^[0-9,.v-]+(-r)?$".toRegex()
  val isStable = stableKeyword || regex.matches(version)
  return isStable.not()
}

dependencies {
    api("io.vertx:vertx-core:$vertxVersion")
    api("software.amazon.awssdk:aws-core:$awsSdkVersion")

    testImplementation("io.vertx:vertx-junit5:$vertxVersion")
    testImplementation("io.vertx:vertx-rx-java2:$vertxVersion")
    testImplementation("cloud.localstack:localstack-utils:$localstackVersion")
    testImplementation("ch.qos.logback:logback-classic:$logbackVersion")
    testImplementation("ch.qos.logback:logback-core:$logbackVersion")
    testImplementation("software.amazon.awssdk:aws-sdk-java:$awsSdkVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:$junit5Version")
}

tasks {
  withType(LintTask::class.java) {
    dependsOn("formatKotlin")
  }

  "build" {
    dependsOn("lintKotlin")
  }

  withType<KotlinCompile> {
    compilerOptions {
      jvmTarget = JVM_21
      allWarningsAsErrors.set(false)
      suppressWarnings.set(true)
      freeCompilerArgs.addAll(listOf(
        "-opt-in=kotlin.RequiresOptIn",
        "-Xcontext-parameters",
        "-Xwhen-guards",
      ))
    }
  }

  test {
    useJUnitPlatform()
  }

  withType<DependencyUpdatesTask> {
    resolutionStrategy {
      componentSelection {
        all(
          Action<ComponentSelectionWithCurrent> {
            val currentVersion = currentVersion
            val candidateVersion = candidate.version

            if (isNonStable(candidateVersion) && !isNonStable(currentVersion)) {
              reject("Release candidate")
            }
          }
        )
      }
    }
  }
}

mavenPublishing {
  coordinates(groupValue, project.name, versionValue)

  pom {
    name.set(project.name)
    description.set("Vertx Async Client For Aws SDK")
    inceptionYear.set("2025")
    url.set("https://github.com/Solibo-AS/vertxt-client-aws-sdk")
    licenses {
      license {
        name.set("The MIT License (MIT)")
        url.set("https://mit-license.org/")
        distribution.set("https://mit-license.org/")
      }
    }
    developers {
      developer {
        id.set("mikand13")
        name.set("Anders Enger Mikkelsen")
        url.set("https://github.com/mikand13")
      }
    }
    scm {
      url.set("https://github.com/Solibo-AS/vertxt-client-aws-sdk")
      connection.set("org-88184710@github.com:Solibo-AS/vertxt-client-aws-sdk.git")
      developerConnection.set("org-88184710@github.com:Solibo-AS/vertxt-client-aws-sdk.git")
    }
  }

  configure(KotlinJvm(
    javadocJar = JavadocJar.Dokka("dokkaHtml"),
    sourcesJar = true,
  ))

  publishToMavenCentral(automaticRelease = true)
  signAllPublications()
}

fun calculateVersion(version: String, branch: String): String = when {
  branch == "develop" -> "$version-SNAPSHOT"
  branch == "master" -> version
  branch.startsWith("release", ignoreCase = true) -> version
  else -> "$version-$branch"
}

fun gitBranch(): String = System.getenv("BRANCH_NAME") ?:
Runtime.getRuntime().exec(arrayOf("git", "rev-parse", "--abbrev-ref", "HEAD"))
  .inputStream
  .bufferedReader()
  .readText()
  .trim()
