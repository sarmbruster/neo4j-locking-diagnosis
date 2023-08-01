package de.armbruster.neo4j;

import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class LockingTest {

    @Container
    private static Neo4jContainer<?>neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:4.4.19-enterprise"))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withoutAuthentication();

    @Test
    void testLocking() {
        try (Driver driver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.none())) {
            System.out.println(driver.executableQuery("return 1").execute().toString());
        }
    }
}
