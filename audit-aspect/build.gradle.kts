plugins {
    `java-library`
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    withSourcesJar()
}

dependencies {
    api("org.springframework:spring-aop:6.2.0")
    implementation("org.aspectj:aspectjweaver:1.9.22.1")
    implementation("io.projectreactor:reactor-core:3.7.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("org.springframework:spring-context:6.2.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.11.4")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(17)
}
