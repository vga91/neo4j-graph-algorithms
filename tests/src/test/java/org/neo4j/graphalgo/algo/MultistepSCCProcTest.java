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

import com.carrotsearch.hppc.LongLongScatterMap;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphalgo.StronglyConnectedComponentsProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
import org.neo4j.graphdb.Transaction;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphalgo.core.utils.StatementApi.executeAndAccept;

/**
 * @author mknblch
 */
@Ignore
public class MultistepSCCProcTest {

    @ClassRule
    public static DatabaseRule api = new ImpermanentDatabaseRule();

    private static Graph graph;

    @BeforeClass
    public static void setup() throws KernelException {
        final String cypher =
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
                        " (a)-[:TYPE {cost:5}]->(b),\n" +
                        " (b)-[:TYPE {cost:5}]->(c),\n" +
                        " (c)-[:TYPE {cost:5}]->(a),\n" +

                        " (d)-[:TYPE {cost:2}]->(e),\n" +
                        " (e)-[:TYPE {cost:2}]->(f),\n" +
                        " (f)-[:TYPE {cost:2}]->(d),\n" +

                        " (a)-[:TYPE {cost:2}]->(d),\n" +

                        " (g)-[:TYPE {cost:3}]->(h),\n" +
                        " (h)-[:TYPE {cost:3}]->(i),\n" +
                        " (i)-[:TYPE {cost:3}]->(g)";

        api.executeTransactionally(cypher);


        api.getDependencyResolver()
                .resolveDependency(GlobalProcedures.class)
                .registerProcedure(StronglyConnectedComponentsProc.class);

        graph = new GraphLoader(api)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .load(HeavyGraphFactory.class);
    }

//    @AfterClass
//    public static void shutdownGraph() throws Exception {
//        api.shutdown();
//    }

    @Test
    public void testWrite() throws Exception {
        String cypher = "CALL algo.scc.multistep('Node', 'TYPE', {concurrency:4, cutoff:0}) " +
                "YIELD loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize " +
                "RETURN loadMillis, computeMillis, writeMillis, setCount, maxSetSize, minSetSize";

        executeAndAccept(api, cypher, row -> {

            assertTrue(row.getNumber("loadMillis").longValue() > 0L);
            assertTrue(row.getNumber("computeMillis").longValue() > 0L);
            assertTrue(row.getNumber("writeMillis").longValue() > 0L);

            assertEquals(3, row.getNumber("setCount").intValue());
            assertEquals(3, row.getNumber("minSetSize").intValue());
            assertEquals(3, row.getNumber("maxSetSize").intValue());

            return true;
        });
    }

    @Test
    public void testStream() throws Exception {
        String cypher = "CALL algo.scc.multistep.stream('Node', 'TYPE', {write:true, concurrency:4, cutoff:0}) YIELD nodeId, cluster RETURN nodeId, cluster";
        final LongLongScatterMap testMap = new LongLongScatterMap();
        executeAndAccept(api, cypher, row -> {
            testMap.addTo(row.getNumber("cluster").longValue(), 1);
            return true;
        });
        // we expect 3 clusters
        assertEquals(3, testMap.size());
        // with 3 elements each
        testMap.forEach((Consumer<? super LongLongCursor>) cursor -> {
            assertEquals(3, cursor.value);
        });
    }
}
