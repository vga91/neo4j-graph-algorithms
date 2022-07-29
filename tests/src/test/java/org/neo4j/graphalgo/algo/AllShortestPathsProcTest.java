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
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.AllShortestPathsProc;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.*;
import static org.neo4j.graphalgo.core.utils.StatementApi.executeAndAccept;


/**         5     5      5
 *      (1)---(2)---(3)----.
 *    5/ 2    2     2     2 \     5
 *  (0)---(7)---(8)---(9)---(10)-//->(0)
 *    3\    3     3     3   /
 *      (4)---(5)---(6)----°
 *
 * S->X: {S,G,H,I,X}:8, {S,D,E,F,X}:12, {S,A,B,C,X}:20
 */
@RunWith(Parameterized.class)
public final class AllShortestPathsProcTest {

    @ClassRule
    public static DatabaseRule api = new ImpermanentDatabaseRule();
    
    private static long startNodeId;
    private static long targetNodeId;

    @BeforeClass
    public static void setup() throws KernelException {
        final String cypher =
                "CREATE (s:Node {name:'s'})\n" +
                        "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +
                        "CREATE (g:Node {name:'g'})\n" +
                        "CREATE (h:Node {name:'h'})\n" +
                        "CREATE (i:Node {name:'i'})\n" +
                        "CREATE (x:Node {name:'x'})\n" +
                        "CREATE" +

                        " (x)-[:TYPE {cost:5}]->(s),\n" + // creates cycle

                        " (s)-[:TYPE {cost:5}]->(a),\n" + // line 1
                        " (a)-[:TYPE {cost:5}]->(b),\n" +
                        " (b)-[:TYPE {cost:5}]->(c),\n" +
                        " (c)-[:TYPE {cost:5}]->(x),\n" +

                        " (s)-[:TYPE {cost:3}]->(d),\n" + // line 2
                        " (d)-[:TYPE {cost:3}]->(e),\n" +
                        " (e)-[:TYPE {cost:3}]->(f),\n" +
                        " (f)-[:TYPE {cost:3}]->(x),\n" +

                        " (s)-[:TYPE {cost:2}]->(g),\n" + // line 3
                        " (g)-[:TYPE {cost:2}]->(h),\n" +
                        " (h)-[:TYPE {cost:2}]->(i),\n" +
                        " (i)-[:TYPE {cost:2}]->(x)";

        api.getDependencyResolver()
                .resolveDependency(GlobalProcedures.class)
                .registerProcedure(AllShortestPathsProc.class);

        try (Transaction tx = api.beginTx()) {
            tx.execute(cypher);

            startNodeId = tx.findNode(Label.label("Node"), "name", "s").getId();
            targetNodeId = tx.findNode(Label.label("Node"), "name", "x").getId();
            tx.commit();
        }
    }

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
    public void testMSBFSASP() throws Exception {

        final Consumer consumer = mock(Consumer.class);

        final String cypher = "CALL algo.allShortestPaths.stream('', {graph:'"+graphImpl+"', direction: 'OUTGOING'}) " +
                "YIELD sourceNodeId, targetNodeId, distance RETURN sourceNodeId, targetNodeId, distance";

        executeAndAccept(api, cypher, row -> {
            final long source = row.getNumber("sourceNodeId").longValue();
            final long target = row.getNumber("targetNodeId").longValue();
            final double distance = row.getNumber("distance").doubleValue();
            assertNotEquals(Double.POSITIVE_INFINITY, distance);
            if (source == target) {
                assertEquals(0.0, distance, 0.1);
            }
            consumer.test(source, target, distance);
            return true;
        });

        // 4 steps from start to end max
        verify(consumer, times(1)).test(eq(startNodeId), eq(targetNodeId), eq(4.0));

    }




    @Test
    public void testMSBFSASPIncoming() throws Exception {

        final Consumer consumer = mock(Consumer.class);

        final String cypher = "CALL algo.allShortestPaths.stream('', {graph:'"+graphImpl+"', direction: 'INCOMING'}) " +
                "YIELD sourceNodeId, targetNodeId, distance RETURN sourceNodeId, targetNodeId, distance";

        executeAndAccept(api, cypher, row -> {
            final long source = row.getNumber("sourceNodeId").longValue();
            final long target = row.getNumber("targetNodeId").longValue();
            final double distance = row.getNumber("distance").doubleValue();
            assertNotEquals(Double.POSITIVE_INFINITY, distance);
            if (source == target) {
                assertEquals(0.0, distance, 0.1);
            }
            consumer.test(source, target, distance);
            return true;
        });

        // 4 steps from start to end max
        verify(consumer, times(1)).test(eq(targetNodeId), eq(startNodeId), eq(4.0));
    }


    @Test
    @Ignore
    public void testWeightedASP() throws Exception {

        final Consumer consumer = mock(Consumer.class);

        final String cypher = "CALL algo.allShortestPaths.stream('cost', {graph:'"+graphImpl+"', direction: 'OUTGOING'}) " +
                "YIELD sourceNodeId, targetNodeId, distance RETURN sourceNodeId, targetNodeId, distance";
        
        executeAndAccept(api, cypher, row -> {
            final long source = row.getNumber("sourceNodeId").longValue();
            final long target = row.getNumber("targetNodeId").longValue();
            final double distance = row.getNumber("distance").doubleValue();
            assertNotEquals(Double.POSITIVE_INFINITY, distance);
            if (source == target) {
                assertEquals(0.0, distance, 0.1);
            }
            assertNotEquals(Double.POSITIVE_INFINITY, distance);
            consumer.test(source, target, distance);
            return true;
        });

        verify(consumer, times(1)).test(eq(startNodeId), eq(targetNodeId), eq(8.0));

    }

    private interface Consumer {
        void test(long source, long target, double distance);
    }
}
