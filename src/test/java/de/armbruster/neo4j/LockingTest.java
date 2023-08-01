package de.armbruster.neo4j;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.EagerResult;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SimpleQueryRunner;
import org.neo4j.driver.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.Neo4jLabsPlugin;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class LockingTest {

    final Logger logger = LoggerFactory.getLogger(LockingTest.class);

    @Container
    private static Neo4jContainer<?>neo4jContainer = new Neo4jContainer<>(DockerImageName.parse("neo4j:4.4.19-enterprise"))
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
            .withLabsPlugins(Neo4jLabsPlugin.APOC)
            .withoutAuthentication();

    private static Driver driver;

    @BeforeAll
    static void setupNeo4jDriver() {
        driver = GraphDatabase.driver(neo4jContainer.getBoltUrl(), AuthTokens.none());
    }

    @AfterAll
    static void closeNeo4jDriver() {
        if (driver!=null) {
            driver.close();
        }
    }

    @Test
    void testSimple() throws InterruptedException {
        EagerResult result = driver.executableQuery("""
                CREATE (:Person{id:0})
                CREATE (:Person{id:1})
                """).execute();
        logger.info(result.toString());
        Thread t1 = new Thread(() -> runQuery(driver, 0, 1, 0, 0));
        Thread t2 = new Thread(() -> runQuery(driver, 1, 0, 1 ,1));
        logger.info("before run");
        t1.start();
        t2.start();
        logger.info("after run");
        t1.join();
        t2.join();
        logger.info("after join");
        int count = driver.executableQuery("""
                MATCH ()-->()
                RETURN count(*)
                """).execute().records().get(0).get(0).asInt();
        assertEquals(2, count);
    }

    private void runQuery(Driver driver, int id1, int id2, int v1, int v2) {
        logger.info("thread start: {} -> {}", id1, id2);
        try (Session session = driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                logger.info("start: {} -> {}", id1, id2);
                tx.run("""
                        MATCH (p1:Person{id:$id1})
                        set p1.value=$v1
                        WITH p1
                        MATCH (p2:Person{id:$id2})
                        set p2.value=$v2
                        WITH p1, p2
                        CREATE (p1)-[:KNOWS]->(p2)
                        WITH p1,p2
                        CALL apoc.util.sleep(1000)
                        """, Map.of("id1", id1,
                        "id2", id2,
                        "v1", v1,
                        "v2", v2));
                logger.info("done: {} -> {}", id1, id2);
                tx.commit();
            }
        } catch (Exception e) {
            logger.error("OOPS", e);
        }
    }

    @Test
    void testLocking() throws InterruptedException {
        driver.executableQuery("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE");

        ExecutorService executorService = Executors.newFixedThreadPool(100);
        Random random = new Random();

        List<Callable<String>> tasks = IntStream.range(0, 10000).mapToObj(value -> (Callable<String>) () -> {
            try (Session session = driver.session()) {

                /*// option 1: use managed transaction with auto-retry in case of transient exceptions
                session.executeWriteWithoutResult(transactionContext -> runPayload(transactionContext, random, value));
                return null;*/

                // option 2: use self managed transactions. retry is your responsibility here, therefore you will see some exceptions
                try (Transaction tx = session.beginTransaction()) {
                    runPayload(tx, random, value);
                    tx.commit();
                    return null;
                }
            } catch (Exception e) {
                logger.error("OOPS", e);
                return e.getMessage();
            }
        }).collect(Collectors.toList());
        List<Future<String>> futures = executorService.invokeAll(tasks);

        List<String> failures = futures.stream().map(LockingTest::futureGetAndCatch).filter(Objects::nonNull).toList();
        logger.warn("we have {} locking issues", failures.size());
        for (String s : failures ) {
            logger.info("failure : {}", s);
        }

        executorService.shutdown();
        assertTrue(executorService.awaitTermination(10, TimeUnit.MINUTES));

        List<Record> records = driver.executableQuery("""
                MATCH (n)-->()
                RETURN n.id, count(*) as outdegree ORDER BY n.id
                """).execute().records();
        for (Record r : records) {
            logger.info("{}", r);
        }

        int count = driver.executableQuery("""
                MATCH ()-[r]->()
                RETURN count(r)
                """).execute().records().get(0).get(0).asInt();
        logger.info("we have in total {} relationships", count);

        List<Record> nonUniqueRelationships = driver.executableQuery("""
                match (a)-->(b)
                WITH a,b, count(*) as rels
                WHERE rels>1
                RETURN a.id, b.id, rels
                """).execute().records();
        nonUniqueRelationships.forEach(record -> logger.info("has more than one rels {}", record));
        assertEquals(0, nonUniqueRelationships.size());

        assertEquals(0, failures.size());
    }

    private void runPayload(SimpleQueryRunner simpleQueryRunner, Random random, int run) {
        long id1 = Math.round(random.nextGaussian() * 3);
        long id2 = Math.round(random.nextGaussian() * 3);
        simpleQueryRunner.run("""
                        MERGE (p1:Person{id:$id1})
                        MERGE (p2:Person{id:$id2})
                        WITH p1,p2
                        // if the line below is not present, MERGE might cause duplicates
                        //CALL apoc.lock.nodes(case when $id1 > $id2 then [p1,p2] else [p2,p1] end)
                        MERGE (p1)-[r:KNOWS]->(p2)
                        SET r.timestamp=localdatetime()
                        """,
                Map.of("id1", id1,"id2", id2));
        logger.debug("run {}: {} -> {}", run, id1, id2);
    }

    private static String futureGetAndCatch(Future<String> stringFuture) {
        try {
            return stringFuture.get();
        } catch (InterruptedException|ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
