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
import org.junit.Test;
import org.neo4j.graphalgo.*;
import org.neo4j.graphalgo.test.rule.DatabaseRule;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;
import org.neo4j.graphdb.Result;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphalgo.core.utils.StatementApi.executeAndAccept;
import static org.neo4j.graphalgo.core.utils.TransactionUtil.testResult;

/**
 * @author mknblch
 */
public class EmptyGraphIntegrationTest {

    @ClassRule
    public static DatabaseRule db = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setup() throws KernelException {

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
        testResult(db, "CALL algo.unionFind.stream('', '',{graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.unionFind.mscoloring.stream('', '',{graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.scc.stream('', '',{graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.scc.stream('', '',{graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.scc.recursive.tunedTarjan.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testForwardBackwardStronglyConnectedComponentsStream() {
        testResult(db, "CALL algo.scc.forwardBackward.stream(0, '', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testAllShortestPathsStream() {
        testResult(db, "CALL algo.allShortestPaths.stream('',{graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testBetweennessCentralityStream() {
        testResult(db, "CALL algo.betweenness.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.betweenness.sampled.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.closeness.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.triangleCount.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.triangle.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testDangelchevCentralityStream() {
        testResult(db, "CALL algo.closeness.dangalchev.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.closeness.harmonic.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.labelPropagation.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.louvain.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.pageRank.stream('', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.shortestPath.astar.stream(null, null, '', '', '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testShortestPathStream() throws Exception {
        testResult(db, "CALL algo.shortestPath.stream(null, null, '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.shortestPaths.stream(null, '', {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
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
        testResult(db, "CALL algo.shortestPath.deltaStepping.stream(null, '', 0, {graph:'" + graphImpl + "'})", Map.of(), result -> {
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testShortestPathsDeltaStepping() throws Exception {
        executeAndAccept(db, "CALL algo.shortestPath.deltaStepping(null, '', 0, {graph:'" + graphImpl + "'})", row -> {
            assertEquals(0L, row.getNumber("nodeCount"));
            return true;
        });
    }
}
