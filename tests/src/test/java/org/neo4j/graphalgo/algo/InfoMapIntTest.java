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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.InfoMapProc;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Node;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;

import java.util.BitSet;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.graphalgo.util.TestUtil.executeAndAccept;

/**
 *
 * TODO REPAIR
 *
 * Graph:
 *
 *        (b)        (e)
 *       /  \       /  \     (x)
 *     (a)--(c)---(d)--(f)
 *
 * @author mknblch
 */
public class InfoMapIntTest {


    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "CREATE (a:Node {name:'a'} )\n" +
                "CREATE (b:Node {name:'b'} )\n" +
                "CREATE (c:Node {name:'c'} )\n" +
                "CREATE (d:Node {name:'d'} )\n" +
                "CREATE (e:Node {name:'e'} )\n" +
                "CREATE (f:Node {name:'f'} )\n" +
                "CREATE (x:Node {name:'x'} )\n" +
                        "CREATE" +
                        " (b)-[:TYPE {v:1.0}]->(a),\n" +
                        " (a)-[:TYPE {v:1.0}]->(c),\n" +
                        " (c)-[:TYPE {v:1.0}]->(a),\n" +
                        " (d)-[:TYPE {v:2.0}]->(c),\n" +
                        " (d)-[:TYPE {v:1.0}]->(e),\n" +
                        " (d)-[:TYPE {v:1.0}]->(f),\n" +
                        " (e)-[:TYPE {v:1.0}]->(f)";

        db.executeTransactionally(cypher);
        db.resolveDependency(GlobalProcedures.class).registerProcedure(InfoMapProc.class);
        db.resolveDependency(GlobalProcedures.class).registerProcedure(PageRankProc.class);
    }

    @Test
    public void testUnweighted() throws Exception {

        final BitSet bitSet = new BitSet(8);

        db.executeTransactionally("CALL algo.infoMap('Node', 'TYPE', {iterations:15, writeProperty:'c'})");

        executeAndAccept(db, "MATCH (n) RETURN n", row -> {
            final Node node = row.getNode("n");
            bitSet.set((Integer) node.getProperty("c"));
            return true;
        });

        assertEquals(3, bitSet.cardinality());
    }


    @Test
    public void testUnweightedStream() throws Exception {

        final BitSet bitSet = new BitSet(8);

        executeAndAccept(db, "CALL algo.infoMap.stream('Node', 'TYPE', {iterations:15}) YIELD nodeId, community",
                row -> {
                    bitSet.set((Integer) row.getNumber("community").intValue());
                    return true;
                });

        assertEquals(3, bitSet.cardinality());

    }


    @Test
    public void testWeighted() throws Exception {

        final BitSet bitSet = new BitSet(8);

        db.executeTransactionally("CALL algo.infoMap('Node', 'TYPE', {weightProperty:'v', writeProperty:'c'})");

        executeAndAccept(db, "MATCH (n) RETURN n", row -> {
            final Node node = row.getNode("n");
            bitSet.set((Integer) node.getProperty("c"));
            return true;
        });

        assertEquals(2, bitSet.cardinality());

    }


    @Test
    public void testWeightedStream() throws Exception {

        final BitSet bitSet = new BitSet(8);

        executeAndAccept(db, "CALL algo.infoMap.stream('Node', 'TYPE', {iterations:15, weightProperty:'v'}) YIELD nodeId, community",
        row -> {
                    bitSet.set((Integer) row.getNumber("community").intValue());
                    return true;
                });

        assertEquals(2, bitSet.cardinality());

    }

    @Test
    public void testPredefinedPageRankStream() throws Exception {


        db.executeTransactionally("CALL algo.pageRank('Node', 'TYPE', {writeProperty:'p', iterations:1}) YIELD nodes");

        final BitSet bitSet = new BitSet(8);

        executeAndAccept(db, "CALL algo.infoMap.stream('Node', 'TYPE', {pageRankProperty:'p'}) YIELD nodeId, community",
                row -> {
                    bitSet.set((Integer) row.getNumber("community").intValue());
                    return true;
                });

        assertEquals(3, bitSet.cardinality());

    }

}
