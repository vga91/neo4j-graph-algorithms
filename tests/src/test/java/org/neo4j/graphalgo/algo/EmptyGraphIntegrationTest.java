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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author mknblch
 */
public class EmptyGraphIntegrationTest {

    private static GraphDatabaseAPI db;

    @BeforeClass
    public static void setup() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase();

        Procedures procedures = db.getDependencyResolver().resolveDependency(GlobalProcedures.class);
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

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
    }

    public String graphImpl = "heavy";

    @Test
    public void testUnionFindStream() {
        Result result = dB.executeTransactionally("CALL algo.unionFind.stream('', '',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testUnionFind() throws Exception {
        dB.executeTransactionally("CALL algo.unionFind('', '',{graph:'" + graphImpl + "'}) YIELD nodes")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testUnionFindMSColoringStream() {
        Result result = dB.executeTransactionally("CALL algo.unionFind.mscoloring.stream('', '',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testUnionFindMSColoring() throws Exception {
        dB.executeTransactionally("CALL algo.unionFind.mscoloring('', '',{graph:'" + graphImpl + "'}) YIELD nodes")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testStronglyConnectedComponentsStream() {
        Result result = dB.executeTransactionally("CALL algo.scc.stream('', '',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testStronglyConnectedComponents() throws Exception {
        dB.executeTransactionally("CALL algo.scc('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    public void testStronglyConnectedComponentsMultiStepStream() {
        Result result = dB.executeTransactionally("CALL algo.scc.stream('', '',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testStronglyConnectedComponentsMultiStep() throws Exception {
        dB.executeTransactionally("CALL algo.scc('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    public void testStronglyConnectedComponentsTunedTarjan() throws Exception {
        dB.executeTransactionally("CALL algo.scc.recursive.tunedTarjan('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    public void testStronglyConnectedComponentsTunedTarjanStream() {
        Result result = dB.executeTransactionally("CALL algo.scc.recursive.tunedTarjan.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testForwardBackwardStronglyConnectedComponentsStream() {
        Result result = dB.executeTransactionally("CALL algo.scc.forwardBackward.stream(0, '', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testAllShortestPathsStream() {
        Result result = dB.executeTransactionally("CALL algo.allShortestPaths.stream('',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testBetweennessCentralityStream() {
        Result result = dB.executeTransactionally("CALL algo.betweenness.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testBetweennessCentrality() throws Exception {
        dB.executeTransactionally("CALL algo.betweenness('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testSampledBetweennessCentralityStream() {
        Result result = dB.executeTransactionally("CALL algo.betweenness.sampled.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testSampledBetweennessCentrality() throws Exception {
        dB.executeTransactionally("CALL algo.betweenness.sampled('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testClosenessCentralityStream() {
        Result result = dB.executeTransactionally("CALL algo.closeness.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testClosenessCentrality() throws Exception {
        dB.executeTransactionally("CALL algo.closeness('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testTriangleCountStream() {
        Result result = dB.executeTransactionally("CALL algo.triangleCount.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testTriangleCount() throws Exception {
        dB.executeTransactionally("CALL algo.triangleCount('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodeCount"));
                    return true;
                });
    }

    @Test
    public void testTriangleStream() {
        Result result = dB.executeTransactionally("CALL algo.triangle.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testDangelchevCentralityStream() {
        Result result = dB.executeTransactionally("CALL algo.closeness.dangalchev.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testDangelchevCentrality() throws Exception {
        dB.executeTransactionally("CALL algo.closeness.dangalchev('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testHarmonicCentralityStream() {
        Result result = dB.executeTransactionally("CALL algo.closeness.harmonic.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testHarmonicCentrality() throws Exception {
        dB.executeTransactionally("CALL algo.closeness.harmonic('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testKSpanningTreeKMax() throws Exception {
        dB.executeTransactionally("CALL algo.spanningTree.kmax('', '', '', 0, 3, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    public void testKSpanningTreeKMin() throws Exception {
        dB.executeTransactionally("CALL algo.spanningTree.kmin('', '', '', 0, 3, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    public void testLabelPropagationStream() {
        Result result = dB.executeTransactionally("CALL algo.labelPropagation.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testLabelPropagationCentrality() throws Exception {
        dB.executeTransactionally("CALL algo.labelPropagation('', '', '', {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testLouvainStream() {
        Result result = dB.executeTransactionally("CALL algo.louvain.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testLouvain() throws Exception {
        dB.executeTransactionally("CALL algo.louvain('', '', {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testPageRankStream() {
        Result result = dB.executeTransactionally("CALL algo.pageRank.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testPageRank() throws Exception {
        dB.executeTransactionally("CALL algo.pageRank('', '', {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    public void testMST() throws Exception {
        dB.executeTransactionally("CALL algo.mst('', '', '', 0, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    public void testSpanningTree() throws Exception {
        dB.executeTransactionally("CALL algo.spanningTree('', '', '', 0, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    public void testSpanningTreeMinimum() throws Exception {
        dB.executeTransactionally("CALL algo.spanningTree.minimum('', '', '', 0, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    public void testSpanningTreeMaximum() throws Exception {
        dB.executeTransactionally("CALL algo.spanningTree.maximum('', '', '', 0, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    public void testShortestPathAStarStream() throws Exception {
        Result result = dB.executeTransactionally("CALL algo.shortestPath.astar.stream(null, null, '', '', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testShortestPathStream() throws Exception {
        Result result = dB.executeTransactionally("CALL algo.shortestPath.stream(null, null, '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testShortestPath() throws Exception {
        dB.executeTransactionally("CALL algo.shortestPath(null, null, '', {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodeCount"));
                    return true;
                });
    }

    @Test
    public void testShortestPathsStream() throws Exception {
        Result result = dB.executeTransactionally("CALL algo.shortestPaths.stream(null, '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testShortestPaths() throws Exception {
        dB.executeTransactionally("CALL algo.shortestPaths(null, '', {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodeCount"));
                    return true;
                });
    }

    @Test
    public void testKShortestPaths() throws Exception {
        dB.executeTransactionally("CALL algo.kShortestPaths(null, null, 3, '', {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("resultCount"));
                    return true;
                });
    }

    @Test
    public void testShortestPathsDeltaSteppingStream() throws Exception {
        Result result = dB.executeTransactionally("CALL algo.shortestPath.deltaStepping.stream(null, '', 0, {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    public void testShortestPathsDeltaStepping() throws Exception {
        dB.executeTransactionally("CALL algo.shortestPath.deltaStepping(null, '', 0, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodeCount"));
                    return true;
                });
    }
}
