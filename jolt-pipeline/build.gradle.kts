plugins {
    `java-library`
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    withSourcesJar()
}

dependencies {
    api("com.bazaarvoice.jolt:jolt-core:0.1.8")
    implementation("com.bazaarvoice.jolt:json-utils:0.1.8")
    implementation("org.springframework:spring-context:6.2.0")
    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("io.projectreactor:reactor-core:3.7.0")
    implementation("io.projectreactor:reactor-tools:3.7.0")
    implementation("org.apache.commons:commons-text:1.10.0")
    implementation("org.apache.commons:commons-lang3:3.17.0")
    implementation("org.apache.groovy:groovy-jsr223:4.0.15")
    compileOnly("org.projectlombok:lombok:1.18.36")
    annotationProcessor("org.projectlombok:lombok:1.18.36")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testImplementation("io.projectreactor:reactor-test:3.7.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.11.4")
    testImplementation("org.projectlombok:lombok:1.18.36")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.36")
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(17)
}
