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
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphalgo.GetNodeFunc;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.test.rule.ImpermanentDatabaseRule;

import java.util.*;

import static org.junit.Assert.*;

public class GetNodeFuncTest {
    public static GraphDatabaseService DB;

    @BeforeClass
    public static void setUp() throws Exception {
        DB = new ImpermanentDatabaseRule()
                .setConfig(GraphDatabaseSettings.procedure_unrestricted, List.of("algo.*"));

        GlobalProcedures proceduresService = ((GraphDatabaseAPI) DB).getDependencyResolver().resolveDependency(GlobalProcedures.class);

        proceduresService.registerProcedure(GlobalProcedures.class, true);
        proceduresService.registerFunction(GetNodeFunc.class, true);
    }

    @Test
    public void lookupNode() throws Exception {
        String createNodeQuery = "CREATE (p:Person {name: 'Mark'}) RETURN p AS node";
        Node savedNode = (Node) DB.executeTransactionally(createNodeQuery, Map.of(), Result::next).get("node");

        Map<String, Object> params = Map.of("nodeId", savedNode.getId());
        Map<String, Object> row = DB.executeTransactionally("RETURN algo.asNode($nodeId) AS node", params, Result::next);

        Node node = (Node) row.get("node");
        assertEquals(savedNode, node);
    }

    @Test
    public void lookupNonExistentNode() throws Exception {
        Map<String, Object> row = DB.executeTransactionally(
                "RETURN algo.asNode(3) AS node", Map.of(), Result::next);

        assertNull(row.get("node"));
    }

    @Test
    public void lookupNodes() throws Exception {
        String createNodeQuery = "CREATE (p1:Person {name: 'Mark'}) CREATE (p2:Person {name: 'Arya'}) RETURN p1, p2";
        Map<String, Object> savedRow = DB.executeTransactionally(createNodeQuery, Map.of(), Result::next);
        Node savedNode1 = (Node) savedRow.get("p1");
        Node savedNode2 = (Node) savedRow.get("p2");

        Map<String, Object> params = Map.of("nodeIds", Arrays.asList(savedNode1.getId(), savedNode2.getId()));
        Map<String, Object> row = DB.executeTransactionally("RETURN algo.asNodes($nodeIds) AS nodes", params, Result::next);

        List<Node> nodes = (List<Node>) row.get("nodes");
        assertEquals(Arrays.asList(savedNode1, savedNode2), nodes);
    }

    @Test
    public void lookupNonExistentNodes() throws Exception {
        Map<String, Object> row = DB.executeTransactionally(
                "RETURN algo.getNodesById([3,4,5]) AS nodes", Map.of(), Result::next);

        List<Node> nodes = (List<Node>) row.get("nodes");
        assertEquals(0, nodes.size());
    }

}
