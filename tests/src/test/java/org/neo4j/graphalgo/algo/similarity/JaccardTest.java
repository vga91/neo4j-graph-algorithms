/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.algo.similarity;

import org.junit.*;
import org.neo4j.graphalgo.similarity.JaccardProc;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.*;
import static org.neo4j.graphalgo.core.utils.TransactionUtil.testResult;
import static org.neo4j.graphalgo.core.utils.TransactionUtil.withTx;

public class JaccardTest {

    @Rule
    public DatabaseRule db = new ImpermanentDatabaseRule();

    public static final String STATEMENT_STREAM = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
            "WITH {item:id(p), categories: collect(distinct id(i))} as userData\n" +
            "WITH collect(userData) as data\n" +
            "call algo.similarity.jaccard.stream(data,$config) " +
            "yield item1, item2, count1, count2, intersection, similarity " +
            "RETURN * ORDER BY item1,item2";

    public static final String STATEMENT = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
            "WITH {item:id(p), categories: collect(distinct id(i))} as userData\n" +
            "WITH collect(userData) as data\n" +
            "CALL algo.similarity.jaccard(data, $config) " +
            "yield p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs, computations " +
            "RETURN *";

    public static final String STORE_EMBEDDING_STATEMENT = "MATCH (p:Person)-[:LIKES]->(i:Item) \n" +
            "WITH p, collect(distinct id(i)) as userData\n" +
            "SET p.embedding = userData";

    public static final String EMBEDDING_STATEMENT = "MATCH (p:Person) \n" +
            "WITH {item:id(p), categories: p.embedding} as userData\n" +
            "WITH collect(userData) as data\n" +
            "CALL algo.similarity.jaccard(data, $config) " +
            "yield p25, p50, p75, p90, p95, p99, p999, p100, nodes, similarityPairs " +
            "RETURN *";

    @Before
    public void before() throws KernelException {
        db.getDependencyResolver().resolveDependency(GlobalProcedures.class).registerProcedure(JaccardProc.class);
        db.executeTransactionally(buildDatabaseQuery());
    }

    private void buildRandomDB(int size) {
        db.executeTransactionally("MATCH (n) DETACH DELETE n");
        db.executeTransactionally("UNWIND range(1,$size/10) as _ CREATE (:Person) CREATE (:Item) ",singletonMap("size",size));
        String statement =
                "MATCH (p:Person) WITH collect(p) as people " +
                "MATCH (i:Item) WITH people, collect(i) as items " +
                "UNWIND range(1,$size) as _ " +
                "WITH people[toInteger(rand()*size(people))] as p, items[toInteger(rand()*size(items))] as i " +
                "MERGE (p)-[:LIKES]->(i) RETURN count(*) ";
        db.executeTransactionally(statement,singletonMap("size",size));
    }
    private static String buildDatabaseQuery() {
        return  "CREATE (a:Person {name:'Alice'})\n" +
                "CREATE (b:Person {name:'Bob'})\n" +
                "CREATE (c:Person {name:'Charlie'})\n" +
                "CREATE (d:Person {name:'Dana'})\n" +
                "CREATE (i1:Item {name:'p1'})\n" +
                "CREATE (i2:Item {name:'p2'})\n" +
                "CREATE (i3:Item {name:'p3'})\n" +

                "CREATE" +
                " (a)-[:LIKES]->(i1),\n" +
                " (a)-[:LIKES]->(i2),\n" +
                " (a)-[:LIKES]->(i3),\n" +
                " (b)-[:LIKES]->(i1),\n" +
                " (b)-[:LIKES]->(i2),\n" +
                " (c)-[:LIKES]->(i3)\n";
        // a: 3
        // b: 2
        // c: 1
        // a / b = 2 : 2/3
        // a / c = 1 : 1/3
        // b / c = 0 : 0/3 = 0
    }

    @Test
    public void jaccardSingleMultiThreadComparision() {
        int size = 333;
        buildRandomDB(size);
        System.out.println("to delete - " + 
                db.executeTransactionally(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"concurrency", 1)), r -> r.resultAsString()));


        try (final Transaction tx = db.beginTx()) {
            final Result result1 = tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff", -0.1, "concurrency", 1)));
            Result result2 = tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"concurrency", 2)));
            Result result4 = tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"concurrency", 4)));
            Result result8 = tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"concurrency", 8)));

            int count=0;
            while (result1.hasNext()) {
                Map<String, Object> row1 = result1.next();
                assertEquals(row1.toString(), row1,result2.next());
                assertEquals(row1.toString(), row1,result4.next());
                assertEquals(row1.toString(), row1,result8.next());
                count++;
            }
            int people = size/10;
            assertEquals((people * people - people)/2,count);
            tx.commit();
        }

//        Result result1 = withTx(db, tx -> {
//            final Result execute = tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff", -0.1, "concurrency", 1)));
//            execute.close();
//            return execute;
//        });
//        Result result2 = withTx(db, tx -> tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"concurrency", 2))));
//        Result result4 = withTx(db, tx -> tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"concurrency", 4))));
//        Result result8 = withTx(db, tx -> tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"concurrency", 8))));
//        int count=0;
//        while (result1.hasNext()) {
//            Map<String, Object> row1 = result1.next();
//            assertEquals(row1.toString(), row1,result2.next());
//            assertEquals(row1.toString(), row1,result4.next());
//            assertEquals(row1.toString(), row1,result8.next());
//            count++;
//        }
//        int people = size/10;
//        assertEquals((people * people - people)/2,count);
    }

    @Test
    public void jaccardSingleMultiThreadComparisionTopK() {
        int size = 333;
        buildRandomDB(size);

        try (final Transaction tx = db.beginTx()) {
            Result result1 = tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"topK",1,"concurrency", 1)));
            Result result2 = tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"topK",1,"concurrency", 2)));
            Result result4 = tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"topK",1,"concurrency", 4)));
            Result result8 = tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"topK",1,"concurrency", 8)));
            int count=0;
            while (result1.hasNext()) {
                Map<String, Object> row1 = result1.next();
                assertEquals(row1.toString(), row1,result2.next());
                assertEquals(row1.toString(), row1,result4.next());
                assertEquals(row1.toString(), row1,result8.next());
                count++;
            }
            int people = size/10;
            assertEquals(people,count);
            tx.commit();
        }
        
//        Result result1 = withTx(db, tx -> tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"topK",1,"concurrency", 1))));
//        Result result2 = withTx(db, tx -> tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"topK",1,"concurrency", 2))));
//        Result result4 = withTx(db, tx -> tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"topK",1,"concurrency", 4))));
//        Result result8 = withTx(db, tx -> tx.execute(STATEMENT_STREAM, Map.of("config", Map.of("similarityCutoff",-0.1,"topK",1,"concurrency", 8))));
    }

    @Test
    public void topNjaccardStreamTest() {
        testResult(db, STATEMENT_STREAM, Map.of("config", Map.of("top",2)), results -> {
            assert01(results.next());
            assert02(results.next());
            assertFalse(results.hasNext());
        });
    }

    @Test
    public void jaccardStreamTest() {
        testResult(db, STATEMENT_STREAM, Map.of("config", Map.of("concurrency",1)), results -> {
            assertTrue(results.hasNext());
            assert01(results.next());
            assert02(results.next());
            assert12(results.next());
            assertFalse(results.hasNext());
        });
    }

    @Test
    public void jaccardStreamSourceTargetIdsTest() {
        testResult(db, STATEMENT_STREAM, Map.of("config",Map.of(
                "concurrency",1,
                "targetIds", Collections.singletonList(1L),
                "sourceIds", Collections.singletonList(0L))), results -> {

            assertTrue(results.hasNext());
            assert01(results.next());
            assertFalse(results.hasNext());
        });
    }

    @Test
    public void jaccardStreamSourceTargetIdsTopKTest() {
        testResult(db, STATEMENT_STREAM, Map.of("config",Map.of(
                "concurrency",1,
                "topK", 1,
                "sourceIds", Collections.singletonList(0L))), results -> {

            assertTrue(results.hasNext());
            assert01(results.next());
            assertFalse(results.hasNext());
        });
    }

    @Test
    public void topKJaccardStreamTest() {
        Map<String, Object> params = Map.of("config", Map.of( "concurrency", 1,"topK", 1));
        System.out.println(db.executeTransactionally(STATEMENT_STREAM, params, Result::resultAsString));

        testResult(db, STATEMENT_STREAM, params, results -> {
            assertTrue(results.hasNext());
            assert01(results.next());
            assert01(flip(results.next()));
            assert02(flip(results.next()));
            assertFalse(results.hasNext());
        });
    }

    private Map<String, Object> flip(Map<String, Object> row) {
        return Map.of("similarity", row.get("similarity"),"intersection", row.get("intersection"),
                "item1",row.get("item2"),"count1",row.get("count2"),
                "item2",row.get("item1"),"count2",row.get("count1"));
    }

    private void assertSameSource(Result results, int count, long source) {
        Map<String, Object> row;
        long target = 0;
        for (int i = 0; i<count; i++) {
            if (target == source) target++;
            assertTrue(results.hasNext());
            row = results.next();
            assertEquals(source, row.get("item1"));
            assertEquals(target, row.get("item2"));
            target++;
        }
    }


    @Test
    public void topK4jaccardStreamTest() {
        Map<String, Object> params = Map.of("config", Map.of("topK", 4, "concurrency", 4, "similarityCutoff", -0.1));
        System.out.println(db.executeTransactionally(STATEMENT_STREAM,params, Result::resultAsString));

        testResult(db, STATEMENT_STREAM,params, results -> {
            assertSameSource(results, 2, 0L);
            assertSameSource(results, 2, 1L);
            assertSameSource(results, 2, 2L);
            assertFalse(results.hasNext());
        });
    }

    @Test
    public void topK3jaccardStreamTest() {
        Map<String, Object> params = Map.of("config", Map.of("concurrency", 3, "topK", 3));

        System.out.println(db.executeTransactionally(STATEMENT_STREAM, params, Result::resultAsString));

        testResult(db, STATEMENT_STREAM, params, results -> {
            assertSameSource(results, 2, 0L);
            assertSameSource(results, 2, 1L);
            assertSameSource(results, 2, 2L);
            assertFalse(results.hasNext());
        });
    }

    @Test
    public void simpleJaccardTest() {
        Map<String, Object> params = Map.of("config", Map.of("similarityCutoff", 0.0));

        Map<String, Object> row = db.executeTransactionally(STATEMENT,params, Result::next);
        assertEquals((double) row.get("p25"), 0.33, 0.01);
        assertEquals((double) row.get("p50"), 0.33, 0.01);
        assertEquals((double) row.get("p75"), 0.66, 0.01);
        assertEquals((double) row.get("p95"), 0.66, 0.01);
        assertEquals((double) row.get("p99"), 0.66, 0.01);
        assertEquals((double) row.get("p100"), 0.66, 0.01);
    }

    @Test
    public void simpleJaccardFromEmbeddingTest() {
        db.executeTransactionally(STORE_EMBEDDING_STATEMENT);

        Map<String, Object> params = Map.of("config", Map.of("similarityCutoff", 0.0));

        Map<String, Object> row = db.executeTransactionally(EMBEDDING_STATEMENT,params, Result::next);
        assertEquals((double) row.get("p25"), 0.33, 0.01);
        assertEquals((double) row.get("p50"), 0.33, 0.01);
        assertEquals((double) row.get("p75"), 0.66, 0.01);
        assertEquals((double) row.get("p95"), 0.66, 0.01);
        assertEquals((double) row.get("p99"), 0.66, 0.01);
        assertEquals((double) row.get("p100"), 0.66, 0.01);
    }


    @Test
    public void simpleJaccardWriteTest() {
        Map<String, Object> params = Map.of("config", Map.of( "write",true, "similarityCutoff", 0.1));

        db.executeTransactionally(STATEMENT,params);

        String checkSimilaritiesQuery = "MATCH (a)-[similar:SIMILAR]-(b)" +
                "RETURN a.name AS node1, b.name as node2, similar.score AS score " +
                "ORDER BY id(a), id(b)";

        System.out.println(db.executeTransactionally(checkSimilaritiesQuery, Map.of(), Result::resultAsString));
        testResult(db, checkSimilaritiesQuery, Map.of(), result -> {

            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals(row.get("node1"), "Alice");
            assertEquals(row.get("node2"), "Bob");
            assertEquals((double) row.get("score"), 0.66, 0.01);

            assertTrue(result.hasNext());
            row = result.next();
            assertEquals(row.get("node1"), "Alice");
            assertEquals(row.get("node2"), "Charlie");
            assertEquals((double) row.get("score"), 0.33, 0.01);

            assertTrue(result.hasNext());
            row = result.next();
            assertEquals(row.get("node1"), "Bob");
            assertEquals(row.get("node2"), "Alice");
            assertEquals((double) row.get("score"), 0.66, 0.01);

            assertTrue(result.hasNext());
            row = result.next();
            assertEquals(row.get("node1"), "Charlie");
            assertEquals(row.get("node2"), "Alice");
            assertEquals((double) row.get("score"), 0.33, 0.01);

            assertFalse(result.hasNext());
        });
    }

    @Test
    public void dontComputeComputationsByDefault() {
        Map<String, Object> params = Map.of("config", Map.of(
                "write", true,
                "similarityCutoff", 0.1));

        testResult(db, STATEMENT, params, writeResult -> {
            Map<String, Object> writeRow = writeResult.next();
            assertEquals(-1L, (long) writeRow.get("computations"));
        });
    }

    @Test
    public void numberOfComputations() {
        Map<String, Object> params = Map.of("config", Map.of(
                "write", true,
                "showComputations", true,
                "similarityCutoff", 0.1));

        testResult(db , STATEMENT, params, writeResult -> {
            Map<String, Object> writeRow = writeResult.next();
            assertEquals(3L, (long) writeRow.get("computations"));
        });
    }

    private void assert12(Map<String, Object> row) {
        assertEquals(1L, row.get("item1"));
        assertEquals(2L, row.get("item2"));
        assertEquals(2L, row.get("count1"));
        assertEquals(1L, row.get("count2"));
        // assertEquals(0L, row.get("intersection"));
        assertEquals(0d, row.get("similarity"));
    }

    // a / b = 2 : 2/3
    // a / c = 1 : 1/3
    // b / c = 0 : 0/3 = 0

    private void assert02(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(2L, row.get("item2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(1L, row.get("count2"));
        // assertEquals(1L, row.get("intersection"));
        assertEquals(1d/3d, row.get("similarity"));
    }

    private void assert01(Map<String, Object> row) {
        assertEquals(0L, row.get("item1"));
        assertEquals(1L, row.get("item2"));
        assertEquals(3L, row.get("count1"));
        assertEquals(2L, row.get("count2"));
        // assertEquals(2L, row.get("intersection"));
        assertEquals(2d/3d, row.get("similarity"));
    }
}
