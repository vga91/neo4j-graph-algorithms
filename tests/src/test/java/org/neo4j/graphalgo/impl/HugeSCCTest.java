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
package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.TransactionWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.impl.scc.HugeSCCIterativeTarjan;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.track_cursor_close;
import static org.neo4j.graphalgo.core.utils.StatementApi.executeAndAccept;

/**        _______
 *        /       \
 *      (0)--(1) (3)--(4)
 *        \  /     \ /
 *        (2)  (6) (5)
 *             / \
 *           (7)-(8)
 *
 * @author mknblch
 */
public class HugeSCCTest {

    @ClassRule
    public static DatabaseRule api = new ImpermanentDatabaseRule()
            .setConfig(track_cursor_close, false);

    private static HugeGraph graph;

    @BeforeClass
    public static void setup() {
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

        graph = (HugeGraph) new TransactionWrapper(api).apply(ktx -> new GraphLoader(api, ktx)
                .withLabel("Node")
                .withRelationshipType("TYPE")
                .withRelationshipWeightsFromProperty("cost", Double.MAX_VALUE)
                .load(HugeGraphFactory.class));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        graph = null;
    }

    public static int getMappedNodeId(String name) {
        final Node[] node = new Node[1];
        executeAndAccept(api, "MATCH (n:Node) WHERE n.name = '" + name + "' RETURN n", row -> {
            node[0] = row.getNode("n");
            return false;
        });
        return graph.toMappedNodeId(node[0].getId());
    }

    @Test
    public void testHugeIterativeScc() throws Exception {
        assertCC(new HugeSCCIterativeTarjan(graph, AllocationTracker.EMPTY)
                .compute()
                .getConnectedComponents());
    }

    private void assertCC(HugeLongArray connectedComponents) {
        assertBelongSameSet(connectedComponents,
                getMappedNodeId("a"),
                getMappedNodeId("b"),
                getMappedNodeId("c"));
        assertBelongSameSet(connectedComponents,
                getMappedNodeId("d"),
                getMappedNodeId("e"),
                getMappedNodeId("f"));
        assertBelongSameSet(connectedComponents,
                getMappedNodeId("g"),
                getMappedNodeId("h"),
                getMappedNodeId("i"));
    }

    private static void assertBelongSameSet(HugeLongArray data, Integer... expected) {
        // check if all belong to same set
        final long needle = data.get(expected[0]);
        for (int i : expected) {
            assertEquals(needle, data.get(i));
        }

        final List<Integer> exp = Arrays.asList(expected);
        // check no other element belongs to this set
        for (int i = 0; i < data.size(); i++) {
            if (exp.contains(i)) {
                continue;
            }
            assertNotEquals(needle, data.get(i));
        }

    }
}
