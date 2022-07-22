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
import org.neo4j.graphalgo.*;
import org.neo4j.graphdb.Result;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphalgo.core.utils.StatementApi.executeAndAccept;

/**
 * @author mknblch
 */
public class EmptyGraphIntegrationTest {

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setup() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase();

        GlobalProcedures procedures = db.getDependencyResolver().resolveDependency(GlobalProcedures.class);
        procedures.registerProcedure(UnionFindProc.class);
        procedures.registerProcedure(MSColoringProc.class);
        procedures.registerProcedure(StronglyConnectedComponentsProc.class);
        procedures.registerProcedure(AllShortestPathsProc.class);
        procedures.registerProcedure(BetweennessCentralityProc.class);
        procedures.registerProcedure(ClosenessCentralityProc.class);
        procedures.registerProcedure(TriangleProc.class);
        procedures.registerProcedure(DangalchevCentralityProc.class);
        procedures.registerProcedure(HarmonicCentralityProc.class);
        procedures.registerProcedure(KSpanningTreeProc.class);
        procedures.registerProcedure(LabelPropagationProc.class);
        procedures.registerProcedure(LouvainProc.class);
        procedures.registerProcedure(PageRankProc.class);
        procedures.registerProcedure(PrimProc.class);
        procedures.registerProcedure(ShortestPathProc.class);
        procedures.registerProcedure(ShortestPathsProc.class);
        procedures.registerProcedure(KShortestPathsProc.class);
        procedures.registerProcedure(ShortestPathDeltaSteppingProc.class);
    }

//    @AfterClass
//    public static void tearDown() throws Exception {
//        if (db != null) db.shutdown();
//    }

    public String graphImpl = "heavy";

    @Test
    public void testUnionFindStream() {
        Result result = db.executeTransactionally("CALL algo.unionFind.stream('', '',{graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testUnionFind() throws Exception {
        executeAndAccept(db, "CALL algo.unionFind('', '',{graph:'" + graphImpl + "'}) YIELD nodes", row -> {
            assertEquals(0L, row.getNumber("nodes"));
            return true;
        });
    }

    @Test
    public void testUnionFindMSColoringStream() {
        Result result = db.executeTransactionally("CALL algo.unionFind.mscoloring.stream('', '',{graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testUnionFindMSColoring() throws Exception {
        executeAndAccept(db, "CALL algo.unionFind.mscoloring('', '',{graph:'" + graphImpl + "'}) YIELD nodes", row -> {
            assertEquals(0L, row.getNumber("nodes"));
            return true;
        });
    }

    @Test
    public void testStronglyConnectedComponentsStream() {
        Result result = db.executeTransactionally("CALL algo.scc.stream('', '',{graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testStronglyConnectedComponents() throws Exception {
        executeAndAccept(db, "CALL algo.scc('', '',{graph:'" + graphImpl + "'})",
                row -> {
                    assertEquals(0L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    public void testStronglyConnectedComponentsMultiStepStream() {
        Result result = db.executeTransactionally("CALL algo.scc.stream('', '',{graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testStronglyConnectedComponentsMultiStep() throws Exception {
        executeAndAccept(db, "CALL algo.scc('', '',{graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("setCount"));
            return true;
        });
    }

    @Test
    public void testStronglyConnectedComponentsTunedTarjan() throws Exception {
        executeAndAccept(db, "CALL algo.scc.recursive.tunedTarjan('', '',{graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("setCount"));
            return true;
        });
    }

    @Test
    public void testStronglyConnectedComponentsTunedTarjanStream() {
        Result result = db.executeTransactionally("CALL algo.scc.recursive.tunedTarjan.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testForwardBackwardStronglyConnectedComponentsStream() {
        Result result = db.executeTransactionally("CALL algo.scc.forwardBackward.stream(0, '', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testAllShortestPathsStream() {
        Result result = db.executeTransactionally("CALL algo.allShortestPaths.stream('',{graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testBetweennessCentralityStream() {
        Result result = db.executeTransactionally("CALL algo.betweenness.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testBetweennessCentrality() throws Exception {
        executeAndAccept(db, "CALL algo.betweenness('', '',{graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("nodes"));
            return true;
        });
    }

    @Test
    public void testSampledBetweennessCentralityStream() {
        Result result = db.executeTransactionally("CALL algo.betweenness.sampled.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testSampledBetweennessCentrality() throws Exception {
        executeAndAccept(db, "CALL algo.betweenness.sampled('', '',{graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("nodes"));
            return true;
        });
    }

    @Test
    public void testClosenessCentralityStream() {
        Result result = db.executeTransactionally("CALL algo.closeness.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testClosenessCentrality() throws Exception {
        executeAndAccept(db, "CALL algo.closeness('', '',{graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("nodes"));
            return true;
        });
    }

    @Test
    public void testTriangleCountStream() {
        Result result = db.executeTransactionally("CALL algo.triangleCount.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testTriangleCount() throws Exception {
        executeAndAccept(db, "CALL algo.triangleCount('', '',{graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("nodeCount"));
            return true;
        });
    }

    @Test
    public void testTriangleStream() {
        Result result = db.executeTransactionally("CALL algo.triangle.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testDangelchevCentralityStream() {
        Result result = db.executeTransactionally("CALL algo.closeness.dangalchev.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testDangelchevCentrality() throws Exception {
        executeAndAccept(db, "CALL algo.closeness.dangalchev('', '',{graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("nodes"));
            return true;
        });
    }

    @Test
    public void testHarmonicCentralityStream() {
        Result result = db.executeTransactionally("CALL algo.closeness.harmonic.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testHarmonicCentrality() throws Exception {
        executeAndAccept(db, "CALL algo.closeness.harmonic('', '',{graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("nodes"));
            return true;
        });
    }

    @Test
    public void testKSpanningTreeKMax() throws Exception {
        executeAndAccept(db, "CALL algo.spanningTree.kmax('', '', '', 0, 3, {graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("effectiveNodeCount"));
            return true;
        });
    }

    @Test
    public void testKSpanningTreeKMin() throws Exception {
        executeAndAccept(db, "CALL algo.spanningTree.kmin('', '', '', 0, 3, {graph:'" + graphImpl + "'})", row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    public void testLabelPropagationStream() {
        Result result = db.executeTransactionally("CALL algo.labelPropagation.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testLabelPropagationCentrality() throws Exception {
        executeAndAccept(db, "CALL algo.labelPropagation('', '', '', {graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("nodes"));
            return true;
        });
    }

    @Test
    public void testLouvainStream() {
        Result result = db.executeTransactionally("CALL algo.louvain.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testLouvain() throws Exception {
        executeAndAccept(db, "CALL algo.louvain('', '', {graph:'" + graphImpl + "'})", row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testPageRankStream() {
        Result result = db.executeTransactionally("CALL algo.pageRank.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testPageRank() throws Exception {
        executeAndAccept(db, "CALL algo.pageRank('', '', {graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("nodes"));
            return true;
        });
    }

    @Test
    public void testMST() throws Exception {
        executeAndAccept(db, "CALL algo.mst('', '', '', 0, {graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("effectiveNodeCount"));
            return true;
        });
    }

    @Test
    public void testSpanningTree() throws Exception {
        executeAndAccept(db, "CALL algo.spanningTree('', '', '', 0, {graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("effectiveNodeCount"));
            return true;
        });
    }

    @Test
    public void testSpanningTreeMinimum() throws Exception {
        executeAndAccept(db, "CALL algo.spanningTree.minimum('', '', '', 0, {graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("effectiveNodeCount"));
            return true;
        });
    }

    @Test
    public void testSpanningTreeMaximum() throws Exception {
        executeAndAccept(db, "CALL algo.spanningTree.maximum('', '', '', 0, {graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("effectiveNodeCount"));
            return true;
        });
    }

    @Test
    public void testShortestPathAStarStream() throws Exception {
        Result result = db.executeTransactionally("CALL algo.shortestPath.astar.stream(null, null, '', '', '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testShortestPathStream() throws Exception {
        Result result = db.executeTransactionally("CALL algo.shortestPath.stream(null, null, '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testShortestPath() throws Exception {
        executeAndAccept(db, "CALL algo.shortestPath(null, null, '', {graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("nodeCount"));
            return true;
        });
    }

    @Test
    public void testShortestPathsStream() throws Exception {
        Result result = db.executeTransactionally("CALL algo.shortestPaths.stream(null, '', {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testShortestPaths() throws Exception {
        executeAndAccept(db, "CALL algo.shortestPaths(null, '', {graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("nodeCount"));
            return true;
        });
    }

    @Test
    public void testKShortestPaths() throws Exception {
        executeAndAccept(db, "CALL algo.kShortestPaths(null, null, 3, '', {graph:'" + graphImpl + "'})", row -> {
                assertEquals(0L, row.getNumber("resultCount"));
                return true;
            });
    }

    @Test
    public void testShortestPathsDeltaSteppingStream() throws Exception {
        Result result = db.executeTransactionally("CALL algo.shortestPath.deltaStepping.stream(null, '', 0, {graph:'" + graphImpl + "'})", Map.of(), r -> r);
        assertFalse(result.hasNext());
    }

    @Test
    public void testShortestPathsDeltaStepping() throws Exception {
        executeAndAccept(db, "CALL algo.shortestPath.deltaStepping(null, '', 0, {graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("nodeCount"));
            return true;
        });
    }
}
