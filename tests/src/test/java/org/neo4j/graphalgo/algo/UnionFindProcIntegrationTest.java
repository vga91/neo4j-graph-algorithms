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

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import com.carrotsearch.hppc.cursors.IntIntCursor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.UnionFindProc;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphalgo.core.utils.StatementApi.executeAndAccept;

/**
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class UnionFindProcIntegrationTest {

    @ClassRule
    public static DatabaseRule db = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setup() throws KernelException {
        String createGraph =
                "CREATE (nA:Label)\n" +
                "CREATE (nB:Label)\n" +
                "CREATE (nC:Label)\n" +
                "CREATE (nD:Label)\n" +
                "CREATE (nE)\n" +
                "CREATE (nF)\n" +
                "CREATE (nG)\n" +
                "CREATE (nH)\n" +
                "CREATE (nI)\n" +
                "CREATE (nJ)\n" + // {J}
                "CREATE\n" +

                // {A, B, C, D}
                "  (nA)-[:TYPE]->(nB),\n" +
                "  (nB)-[:TYPE]->(nC),\n" +
                "  (nC)-[:TYPE]->(nD),\n" +

                "  (nD)-[:TYPE {cost:4.2}]->(nE),\n" + // threshold UF should split here

                // {E, F, G}
                "  (nE)-[:TYPE]->(nF),\n" +
                "  (nF)-[:TYPE]->(nG),\n" +

                // {H, I}
                "  (nH)-[:TYPE]->(nI)";

        try (Transaction tx = db.beginTx()) {
            db.executeTransactionally(createGraph);
            tx.commit();
        }

        db.getDependencyResolver()
                .resolveDependency(GlobalProcedures.class)
                .registerProcedure(UnionFindProc.class);
    }
//
//    @AfterClass
//    public static void tearDown() throws Exception {
//        if (db != null) db.shutdown();
//    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"Heavy"},
                new Object[]{"Light"},
                new Object[]{"Huge"},
                new Object[]{"Kernel"}
        );
    }

    @Parameterized.Parameter
    public String graphImpl;

    @Test
    public void testUnionFind() throws Exception {
        executeAndAccept(db, "CALL algo.unionFind('', '',{graph:'"+graphImpl+"'}) YIELD setCount, communityCount", row -> {
            assertEquals(3L, row.getNumber("communityCount"));
            assertEquals(3L, row.getNumber("setCount"));
            return true;
        });
    }

    @Test
    public void testUnionFindWithLabel() throws Exception {
        executeAndAccept(db, "CALL algo.unionFind('Label', '',{graph:'"+graphImpl+"'}) YIELD setCount, communityCount", row -> {
            assertEquals(1L, row.getNumber("communityCount"));
            assertEquals(1L, row.getNumber("setCount"));
            return true;
        });
    }

    @Test
    public void testUnionFindWriteBack() throws Exception {
        executeAndAccept(db, "CALL algo.unionFind('', 'TYPE', {write:true,graph:'"+graphImpl+"'}) YIELD setCount, communityCount, writeMillis, nodes, partitionProperty, writeProperty", row -> {
            assertNotEquals(-1L, row.getNumber("writeMillis"));
            assertNotEquals(-1L, row.getNumber("nodes"));
            assertEquals(3L, row.getNumber("communityCount"));
            assertEquals(3L, row.getNumber("setCount"));
            assertEquals("partition", row.getString("partitionProperty"));
            assertEquals("partition", row.getString("writeProperty"));
            return false;
        });
    }

    @Test
    public void testUnionFindWriteBackExplicitWriteProperty() throws Exception {
        executeAndAccept(db, "CALL algo.unionFind('', 'TYPE', {write:true,graph:'"+graphImpl+"', writeProperty:'unionFind'}) YIELD setCount, communityCount, writeMillis, nodes, partitionProperty, writeProperty", row -> {
            assertNotEquals(-1L, row.getNumber("writeMillis"));
            assertNotEquals(-1L, row.getNumber("nodes"));
            assertEquals(3L, row.getNumber("communityCount"));
            assertEquals(3L, row.getNumber("setCount"));
            assertEquals("unionFind", row.getString("partitionProperty"));
            assertEquals("unionFind", row.getString("writeProperty"));
            return false;
        });
    }


    @Test
    public void testUnionFindStream() throws Exception {
        final IntIntScatterMap map = new IntIntScatterMap(11);
        executeAndAccept(db, "CALL algo.unionFind.stream('', 'TYPE', {graph:'"+graphImpl+"'}) YIELD setId", row -> {
            map.addTo(row.getNumber("setId").intValue(), 1);
            return true;
        });
        assertMapContains(map, 1, 2, 7);
    }

    @Test
    public void testThresholdUnionFindStream() throws Exception {
        final IntIntScatterMap map = new IntIntScatterMap(11);
        executeAndAccept(db, "CALL algo.unionFind.stream('', 'TYPE', {weightProperty:'cost', defaultValue:10.0, threshold:5.0, concurrency:1, graph:'"+graphImpl+"'}) YIELD setId", row -> {
            map.addTo(row.getNumber("setId").intValue(), 1);
            return true;
        });
        assertMapContains(map, 4, 3, 2, 1);
    }

    @Test
    public void testThresholdUnionFindLowThreshold() throws Exception {
        final IntIntScatterMap map = new IntIntScatterMap(11);
        executeAndAccept(db, "CALL algo.unionFind.stream('', 'TYPE', {weightProperty:'cost', defaultValue:10.0, concurrency:1, threshold:3.14, graph:'"+graphImpl+"'}) YIELD setId", row -> {
            map.addTo(row.getNumber("setId").intValue(), 1);
            return true;
        });
        assertMapContains(map, 1, 2, 7);
    }

    private static void assertMapContains(IntIntMap map, int... values) {
        assertEquals("set count does not match", values.length, map.size());
        for (int count : values) {
            assertTrue("set size " + count + " does not match", mapContainsValue(map, count));
        }
    }

    private static boolean mapContainsValue(IntIntMap map, int value) {
        for (IntIntCursor cursor : map) {
            if (cursor.value == value) {
                return true;
            }
        }
        return false;
    }
}
