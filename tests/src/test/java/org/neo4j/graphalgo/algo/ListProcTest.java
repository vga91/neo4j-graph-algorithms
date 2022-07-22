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
import org.neo4j.graphalgo.ListProc;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphalgo.linkprediction.LinkPrediction;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 20.10.17
 */
public class ListProcTest {
    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();
    public static final List<String> PROCEDURES = asList("algo.pageRank", "algo.pageRank.stream");
    public static final List<String> FUNCTIONS = Arrays.asList("algo.linkprediction.adamicAdar", "algo.linkprediction.commonNeighbors", "algo.linkprediction.preferentialAttachment", "algo.linkprediction.resourceAllocation", "algo.linkprediction.sameCommunity",
            "algo.linkprediction.totalNeighbors");
    public static final List<String> ALL = Stream.of(PROCEDURES, FUNCTIONS).flatMap(Collection::stream).collect(Collectors.toList());

    @BeforeClass
    public static void setUp() throws Exception {
        GlobalProcedures procedures = DB.getDependencyResolver().resolveDependency(GlobalProcedures.class);
        procedures.registerProcedure(ListProc.class);
        procedures.registerProcedure(PageRankProc.class);
        procedures.registerFunction(LinkPrediction.class);
    }

//    @AfterClass
//    public static void tearDown() {
//        DB.shutdown();
//    }

    @Test
    public void listProcedures() throws Exception {
        assertEquals(ALL, listProcs(null));
        assertEquals(PROCEDURES, listProcs("page"));
        assertEquals(singletonList("algo.pageRank.stream"), listProcs("stream"));
        assertEquals(emptyList(), listProcs("foo"));
    }

    @Test
    public void listFunctions() throws Exception {
        assertEquals(FUNCTIONS, listProcs("linkprediction"));
    }

    @Test
    public void listEmpty() throws Exception {
        assertEquals(ALL,
                DB.executeTransactionally("CALL algo.list()", Map.of(), r -> r.<String>columnAs("name").stream().collect(Collectors.toList())));
    }

    private List<String> listProcs(Object name) {
        return DB.executeTransactionally("CALL algo.list($name)", singletonMap("name", name), r -> r.<String>columnAs("name").stream().collect(Collectors.toList()));
    }
}
