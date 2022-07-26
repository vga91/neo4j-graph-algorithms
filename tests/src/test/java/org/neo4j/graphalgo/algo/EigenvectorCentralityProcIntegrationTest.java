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
package org.neo4j.graphalgo.algo;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphalgo.EigenvectorCentralityProc;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.neo4j.graphalgo.core.utils.TransactionUtil.testResult;

@RunWith(Parameterized.class)
public class EigenvectorCentralityProcIntegrationTest {

    private static GraphDatabaseAPI db;
    private static Map<Long, Double> expected = new HashMap<>();
    private static Map<Long, Double> weightedExpected = new HashMap<>();

//    @AfterClass
//    public static void tearDown() throws Exception {
//        if (db != null) db.shutdown();
//    }

    @BeforeClass
    public static void setup() throws KernelException {
        ClassLoader classLoader = EigenvectorCentralityProcIntegrationTest.class.getClassLoader();
        File file = new File(classLoader.getResource("got/got-s1-nodes.csv").getFile());

        db = (GraphDatabaseAPI) new TestDatabaseManagementServiceBuilder(new File(UUID.randomUUID().toString()).toPath())
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root, file.getParentFile().toPath())
                .build();
        

        try (Transaction tx = db.beginTx()) {
            db.executeTransactionally("CREATE CONSTRAINT ON (c:Character)\n" +
                    "ASSERT c.id IS UNIQUE;");
        }

        try (Transaction tx = db.beginTx()) {
            db.executeTransactionally("LOAD CSV WITH HEADERS FROM 'file:///got-s1-nodes.csv' AS row\n" +
                    "MERGE (c:Character {id: row.Id})\n" +
                    "SET c.name = row.Label;");

            db.executeTransactionally("LOAD CSV WITH HEADERS FROM 'file:///got-s1-edges.csv' AS row\n" +
                    "MATCH (source:Character {id: row.Source})\n" +
                    "MATCH (target:Character {id: row.Target})\n" +
                    "MERGE (source)-[rel:INTERACTS_SEASON1]->(target)\n" +
                    "SET rel.weight = toInteger(row.Weight);");

            tx.commit();
        }

        GlobalProcedures procedures = db.getDependencyResolver().resolveDependency(GlobalProcedures.class);
        procedures.registerProcedure(EigenvectorCentralityProc.class);
        procedures.registerProcedure(PageRankProc.class);


        try (Transaction tx = db.beginTx()) {
            final Label label = Label.label("Character");
            expected.put(tx.findNode(label, "name", "Ned").getId(), 111.68570401574802);
            expected.put(tx.findNode(label, "name", "Robert").getId(), 88.09448401574804);
            expected.put(tx.findNode(label, "name", "Cersei").getId(), 		84.59226401574804);
            expected.put(tx.findNode(label, "name", "Catelyn").getId(), 	84.51566401574803);
            expected.put(tx.findNode(label, "name", "Tyrion").getId(), 82.00291401574802);
            expected.put(tx.findNode(label, "name", "Joffrey").getId(), 77.67397401574803);
            expected.put(tx.findNode(label, "name", "Robb").getId(), 73.56551401574802);
            expected.put(tx.findNode(label, "name", "Arya").getId(), 73.32532401574804	);
            expected.put(tx.findNode(label, "name", "Petyr").getId(), 72.26733401574802);
            expected.put(tx.findNode(label, "name", "Sansa").getId(), 71.56470401574803);
            tx.commit();
        }
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"Heavy"},
                new Object[]{"Light"},
                new Object[]{"Kernel"},
                new Object[]{"Huge"}
        );
    }

    @Parameterized.Parameter
    public String graphImpl;

    @Test
    public void testStream() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.eigenvector.stream('Character', 'INTERACTS_SEASON1', {graph:'"+graphImpl+"', direction: 'BOTH'}) " +
                        "YIELD nodeId, score " +
                        "RETURN nodeId, score " +
                        "ORDER BY score DESC " +
                        "LIMIT 10",
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        assertMapEquals(expected, actual);
    }

//    @Test
//    public void testWeightedPageRankStream() throws Exception {
//        final Map<Long, Double> actual = new HashMap<>();
//        runQuery(
//                "CALL algo.pageRank.stream('Label1', 'TYPE1', {graph:'"+graphImpl+"', weightProperty: 'foo'}) YIELD nodeId, score",
//                row -> actual.put(
//                        (Long)row.get("nodeId"),
//                        (Double) row.get("score")));
//
//        assertMapEquals(weightedExpected, actual);
//    }

//    @Test
//    public void testWeightedPageRankWithCachedWeightsStream() throws Exception {
//        final Map<Long, Double> actual = new HashMap<>();
//        runQuery(
//                "CALL algo.pageRank.stream('Label1', 'TYPE1', {graph:'"+graphImpl+"', weightProperty: 'foo', cacheWeights: true}) YIELD nodeId, score",
//                row -> actual.put(
//                        (Long)row.get("nodeId"),
//                        (Double) row.get("score")));
//
//        assertMapEquals(weightedExpected, actual);
//    }

//    @Test
//    public void testWeightedPageRankWithAllRelationshipsEqualStream() throws Exception {
//        final Map<Long, Double> actual = new HashMap<>();
//        runQuery(
//                "CALL algo.pageRank.stream('Label1', 'TYPE1', {graph:'"+graphImpl+"', weightProperty: 'madeUp', defaultValue: 1.0}) YIELD nodeId, score",
//                row -> actual.put(
//                        (Long)row.get("nodeId"),
//                        (Double) row.get("score")));
//
//        assertMapEquals(expected, actual);
//    }

    @Test
    public void testWriteBack() throws Exception {
        runQuery(
                "CALL algo.eigenvector('Character', 'INTERACTS_SEASON1', {graph:'"+graphImpl+"', direction: 'BOTH'}) " +
                        "YIELD writeMillis, write, writeProperty ",
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("eigenvector", row.getString("writeProperty"));
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        assertResult("eigenvector", expected);
    }

//    @Test
//    public void testWeightedPageRankWriteBack() throws Exception {
//        runQuery(
//                "CALL algo.pageRank('Label1', 'TYPE1', {graph:'"+graphImpl+"', weightProperty: 'foo'}) YIELD writeMillis, write, writeProperty",
//                row -> {
//                    assertTrue(row.getBoolean("write"));
//                    assertEquals("pagerank", row.getString("writeProperty"));
//                    assertTrue(
//                            "write time not set",
//                            row.getNumber("writeMillis").intValue() >= 0);
//                });
//
//        assertResult("pagerank", weightedExpected);
//    }

    @Test
    public void testWriteBackUnderDifferentProperty() throws Exception {
        runQuery(
                "CALL algo.eigenvector('Character', 'INTERACTS_SEASON1', {writeProperty:'foobar', graph:'"+graphImpl+"', direction: 'BOTH'}) YIELD writeMillis, write, writeProperty",
                row -> {
                    assertTrue(row.getBoolean("write"));
                    assertEquals("foobar", row.getString("writeProperty"));
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        assertResult("foobar", expected);
    }

    @Test
    public void testParallelWriteBack() throws Exception {
        runQuery(
                "CALL algo.eigenvector('Character', 'INTERACTS_SEASON1', {batchSize:3, concurrency:2, write:true, graph:'"+graphImpl+"', direction: 'BOTH'}) YIELD writeMillis, write, writeProperty, iterations",
                row -> {
                    assertTrue(
                            "write time not set",
                            row.getNumber("writeMillis").intValue() >= 0);
                });

        assertResult("eigenvector", expected);
    }

    @Test
    public void testParallelExecution() throws Exception {
        final Map<Long, Double> actual = new HashMap<>();
        runQuery(
                "CALL algo.eigenvector.stream('Character', 'INTERACTS_SEASON1', {batchSize:2, graph:'"+graphImpl+"', direction: 'BOTH'}) " +
                        "YIELD nodeId, score " +
                        "RETURN nodeId, score " +
                        "ORDER BY score DESC " +
                        "LIMIT 10",
                row -> {
                    final long nodeId = row.getNumber("nodeId").longValue();
                    actual.put(nodeId, (Double) row.get("score"));
                });
        assertMapEquals(expected, actual);
    }

    private static void runQuery(
            String query,
            Consumer<Result.ResultRow> check) {
        runQuery(query, new HashMap<>(), check);
    }

    private static void runQuery(
            String query,
            Map<String, Object> params,
            Consumer<Result.ResultRow> check) {
        testResult(db, query, params, result -> {
            result.accept(row -> {
                check.accept(row);
                return true;
            });
        });
    }

    private void assertResult(final String scoreProperty, Map<Long, Double> expected) {
        try (Transaction tx = db.beginTx()) {
            for (Map.Entry<Long, Double> entry : expected.entrySet()) {
                double score = ((Number) tx
                        .getNodeById(entry.getKey())
                        .getProperty(scoreProperty)).doubleValue();
                assertEquals(
                        "score for " + entry.getKey(),
                        entry.getValue(),
                        score,
                        0.1);
            }
            tx.commit();
        }
    }

    private static void assertMapEquals(
            Map<Long, Double> expected,
            Map<Long, Double> actual) {
        assertEquals("number of elements", expected.size(), actual.size());
        HashSet<Long> expectedKeys = new HashSet<>(expected.keySet());
        for (Map.Entry<Long, Double> entry : actual.entrySet()) {
            assertTrue(
                    "unknown key " + entry.getKey(),
                    expectedKeys.remove(entry.getKey()));
            assertEquals(
                    "value for " + entry.getKey(),
                    expected.get(entry.getKey()),
                    entry.getValue(),
                    0.1);
        }
        for (Long expectedKey : expectedKeys) {
            fail("missing key " + expectedKey);
        }
    }
}
