<?xml version="1.0" encoding="UTF-8"?>
<project xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd" xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <modelVersion>4.0.0</modelVersion>

    <groupId>{groupId}</groupId>
    <artifactId>{artifactId}</artifactId>
    <version>{version}</version>

    <name>{groupId}:{artifactId}</name>
    <description>Odradek: a server that owns the Kafka clients for an application, and its client libraries.</description>
    <url>https://github.com/bpalermo/odradek</url>

    <licenses>
        <license>
            <name>Apache License, Version 2.0</name>
            <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
        </license>
    </licenses>

    <developers>
        <developer>
            <name>The Odradek authors</name>
            <url>https://github.com/bpalermo/odradek</url>
        </developer>
    </developers>

    <scm>
        <connection>scm:git:https://github.com/bpalermo/odradek.git</connection>
        <developerConnection>scm:git:ssh://git@github.com/bpalermo/odradek.git</developerConnection>
        <url>https://github.com/bpalermo/odradek</url>
    </scm>

    <dependencies>
<!-- extra dependencies -->
{dependencies}
    </dependencies>
</project>
