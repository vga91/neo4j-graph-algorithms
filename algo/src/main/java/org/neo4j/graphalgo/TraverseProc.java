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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.*;
import org.neo4j.graphalgo.impl.Traverse;
import org.neo4j.graphalgo.impl.walking.WalkPath;
import org.neo4j.graphalgo.impl.walking.WalkResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author mknblch
 */
public class TraverseProc {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction kernelTransaction;

    @Context
    public Transaction transaction;

    @Procedure(value = "algo.bfs.stream")
    @Description("CALL algo.bfs.stream(label:String, relationshipType:String, startNodeId:long, direction:Direction, " +
            "{writeProperty:String, target:long, maxDepth:long, weightProperty:String, maxCost:double}) YIELD nodeId")
    public Stream<WalkResult> bfs(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "direction") String direction,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideDirection(direction)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);

        final Graph graph = new GraphLoader(api, Pools.DEFAULT, kernelTransaction)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeWeights()
                .withOptionalRelationshipWeightsFromProperty(configuration.getWeightProperty(), 1.)
                .withDirection(configuration.getDirection(Direction.OUTGOING))
                .withLog(log)
                .load(configuration.getGraphImpl(HeavyGraph.TYPE));
        final Traverse traverse = new Traverse(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "BFS"))
                .withTerminationFlag(TerminationFlag.wrap(kernelTransaction));

        final List<Long> targetNodes = configuration.get("targetNodes", Collections.emptyList());
        final long maxDepth = configuration.getNumber("maxDepth", -1L).longValue();
        final Traverse.ExitPredicate exitFunction;
        final Traverse.Aggregator aggregatorFunction;

        // target node given; terminate if target is reached
        if (!targetNodes.isEmpty()) {
            final List<Integer> mappedTargets = targetNodes.stream()
                    .map(graph::toMappedNodeId)
                    .collect(Collectors.toList());
            exitFunction = (s, t, w) -> mappedTargets.contains(t) ? Traverse.ExitPredicate.Result.BREAK : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;
        // maxDepth given; continue to aggregate nodes with lower depth until no more nodes left
        } else if (maxDepth != -1) {
            exitFunction = (s, t, w) -> w >= maxDepth ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + 1.;

        // maxCost & weightProperty given; aggregate nodes with lower cost then maxCost
        } else if (configuration.hasWeightProperty() && configuration.containsKeys("maxCost")) {
            final double maxCost = configuration.getNumber("maxCost", 1.).doubleValue();
            exitFunction = (s, t, w) -> w >= maxCost ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + graph.weightOf(s, t);

        // do complete BFS until all nodes have been visited
        } else {
            exitFunction = (s, t, w) -> Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;
        }

        final long[] nodes = traverse.computeBfs(
                startNode,
                configuration.getDirection(Direction.OUTGOING),
                exitFunction,
                aggregatorFunction);

        return Stream.of(new WalkResult(nodes, WalkPath.toPath(transaction, nodes)));
    }

    @Procedure(value = "algo.dfs.stream")
    @Description("CALL algo.dfs.stream(label:String, relationshipType:String, startNodeId:long, direction:Direction, " +
            "{writeProperty:String, target:long, maxDepth:long, weightProperty:String, maxCost:double}) YIELD nodeId")
    public Stream<WalkResult> dfs(
            @Name(value = "label") String label,
            @Name(value = "relationshipType") String relationship,
            @Name(value = "direction") String direction,
            @Name(value = "startNodeId") long startNode,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = ProcedureConfiguration.create(config)
                .overrideDirection(direction)
                .overrideNodeLabelOrQuery(label)
                .overrideRelationshipTypeOrQuery(relationship);
        final Graph graph = new GraphLoader(api, Pools.DEFAULT, kernelTransaction)
                .withOptionalLabel(label)
                .withOptionalRelationshipType(relationship)
                .withoutNodeWeights()
                .withOptionalRelationshipWeightsFromProperty(configuration.getWeightProperty(), 1.)
                .withDirection(configuration.getDirection(Direction.OUTGOING))
                .withLog(log)
                .load(configuration.getGraphImpl(HeavyGraph.TYPE));
        final Traverse traverse = new Traverse(graph)
                .withProgressLogger(ProgressLogger.wrap(log, "DFS"))
                .withTerminationFlag(TerminationFlag.wrap(kernelTransaction));

        final List<Long> targetNodes = configuration.get("targetNodes", Collections.emptyList());
        final long maxDepth = configuration.getNumber("maxDepth", -1L).longValue();
        final Traverse.ExitPredicate exitFunction;
        final Traverse.Aggregator aggregatorFunction;

        // target node given; terminate if target is reached
        if (!targetNodes.isEmpty()) {
            final List<Integer> mappedTargets = targetNodes.stream()
                    .map(graph::toMappedNodeId)
                    .collect(Collectors.toList());
            exitFunction = (s, t, w) -> mappedTargets.contains(t) ? Traverse.ExitPredicate.Result.BREAK : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;

        // maxDepth given; continue to aggregate nodes with lower depth until no more nodes left
        } else if (maxDepth != -1) {
            exitFunction = (s, t, w) -> w > maxDepth ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + 1.;

        // maxCost & weightProperty given; aggregate nodes with lower cost then maxCost
        } else if (configuration.hasWeightProperty() && configuration.containsKeys("maxCost")) {
            final double maxCost = configuration.getNumber("maxCost", 1.).doubleValue();
            exitFunction = (s, t, w) -> w >= maxCost ? Traverse.ExitPredicate.Result.CONTINUE : Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> w + graph.weightOf(s, t);

        // do complete BFS until all nodes have been visited
        } else {
            exitFunction = (s, t, w) -> Traverse.ExitPredicate.Result.FOLLOW;
            aggregatorFunction = (s, t, w) -> .0;
        }

        final long[] nodes = traverse.computeDfs(
                startNode,
                configuration.getDirection(Direction.OUTGOING),
                exitFunction,
                aggregatorFunction);

        return Stream.of(new WalkResult(nodes, WalkPath.toPath(transaction, nodes)));
    }

}
