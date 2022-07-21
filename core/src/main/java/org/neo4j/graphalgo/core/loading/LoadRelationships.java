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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.Nodes;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelections;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

public interface LoadRelationships {

    int degreeOut(NodeCursor cursor);

    int degreeIn(NodeCursor cursor);

    int degreeBoth(NodeCursor cursor);

    /**
     * See {@link NodesHelper} for the reason why we have this in addition to {@link #degreeBoth(NodeCursor)}.
     */
    int degreeUndirected(NodeCursor cursor);

    RelationshipTraversalCursor relationshipsOut(NodeCursor cursor);

    RelationshipTraversalCursor relationshipsIn(NodeCursor cursor);

    RelationshipTraversalCursor relationshipsBoth(NodeCursor cursor);

    default RelationshipTraversalCursor relationshipsOf(Direction direction, NodeCursor cursor) {
        switch (direction) {
            case OUTGOING:
                return relationshipsOut(cursor);
            case INCOMING:
                return relationshipsIn(cursor);
            case BOTH:
                return relationshipsBoth(cursor);
            default:
                throw new IllegalArgumentException("direction " + direction);
        }
    }

    static void consumeRelationships(RelationshipTraversalCursor cursor, Consumer<RelationshipTraversalCursor> action) {
        try (RelationshipTraversalCursor rels = cursor) {
            while (rels.next()) {
                action.accept(rels);
            }
        }
    }

    static LoadRelationships of(KernelTransaction transaction, int[] relationshipType) {
//    static LoadRelationships of(CursorFactory cursors, int[] relationshipType) {
        if (relationshipType == null || relationshipType.length == 0) {
            return new LoadAllRelationships(transaction);
        }
        return new LoadRelationshipsOfSingleType(transaction, relationshipType);
    }
}


final class LoadAllRelationships implements LoadRelationships {
    private final CursorFactory cursors;
    private final CursorContext cursorContext;
//    private final RelationshipTraversalCursor traversalCursor;

    LoadAllRelationships(final KernelTransaction transaction) {
        this.cursors = transaction.cursors();
        this.cursorContext = transaction.cursorContext();
//        this.traversalCursor = transaction.();
    }

    @Override
    public int degreeOut(final NodeCursor cursor) {
        return Nodes.countOutgoing(cursor);
    }

    @Override
    public int degreeIn(final NodeCursor cursor) {
        return Nodes.countIncoming(cursor);
    }

    @Override
    public int degreeBoth(final NodeCursor cursor) {
//        return Nodes.countAll(cursor, cursors);
        return countAll(cursor, cursors, cursorContext);
    }

    private static int countAll(NodeCursor nodeCursor, CursorFactory cursors, CursorContext cursorContext) {
        Set<Pair<Long, Long>> sourceTargetPairs = new HashSet<>();

        try (RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor(cursorContext)) {
            nodeCursor.relationships(traversal, ALL_RELATIONSHIPS);
            while (traversal.next()) {
                long low = Math.min(traversal.sourceNodeReference(), traversal.targetNodeReference());
                long high = Math.max(traversal.sourceNodeReference(), traversal.targetNodeReference());
                sourceTargetPairs.add(Pair.of(low, high));
            }
            return sourceTargetPairs.size();
        }
    }


    @Override
    public int degreeUndirected(final NodeCursor cursor) {
        return NodesHelper.countUndirected(cursor, cursors);
    }

    @Override
    public RelationshipTraversalCursor relationshipsOut(final NodeCursor cursor) {
        return RelationshipSelections.outgoingCursor(cursors, cursor, null, cursorContext);
    }

    @Override
    public RelationshipTraversalCursor relationshipsIn(final NodeCursor cursor) {
        return RelationshipSelections.incomingCursor(cursors, cursor, null, cursorContext);
    }

    @Override
    public RelationshipTraversalCursor relationshipsBoth(final NodeCursor cursor) {
        return RelationshipSelections.allCursor(cursors, cursor, null, cursorContext);
    }
}

final class LoadRelationshipsOfSingleType implements LoadRelationships {
    private final CursorFactory cursors;
    private final CursorContext cursorContext;
    private final int type;
    private final int[] types;

    LoadRelationshipsOfSingleType(final KernelTransaction transaction, final int[] types) {
        this.cursors = transaction.cursors();
        this.cursorContext = transaction.cursorContext();
        this.type = types[0];
        this.types = types;
    }

    @Override
    public int degreeOut(final NodeCursor cursor) {
        return Nodes.countOutgoing(cursor, type);
    }

    @Override
    public int degreeIn(final NodeCursor cursor) {
        return Nodes.countIncoming(cursor, type);
    }

    @Override
    public int degreeBoth(final NodeCursor cursor) {
        return countAll(cursor, cursors, type, cursorContext);
    }

    public static int countAll( NodeCursor nodeCursor, CursorFactory cursors, int type, CursorContext cursorContext)
    {
        Set<Pair<Long, Long>> sourceTargetPairs = new HashSet<>();

        try (RelationshipTraversalCursor traversal = cursors.allocateRelationshipTraversalCursor(cursorContext)) {
            nodeCursor.relationships(traversal, ALL_RELATIONSHIPS);
            while (traversal.next()) {
                if (traversal.type() == type) {
                    long low = Math.min(traversal.sourceNodeReference(), traversal.targetNodeReference());
                    long high = Math.max(traversal.sourceNodeReference(), traversal.targetNodeReference());
                    sourceTargetPairs.add(Pair.of(low, high));
                }
            }
            return sourceTargetPairs.size();
        }
    }

    @Override
    public int degreeUndirected(final NodeCursor cursor) {
        return NodesHelper.countUndirected(cursor, cursors, type);
    }

    @Override
    public RelationshipTraversalCursor relationshipsOut(final NodeCursor cursor) {
        return RelationshipSelections.outgoingCursor(cursors, cursor, types, cursorContext);
    }

    @Override
    public RelationshipTraversalCursor relationshipsIn(final NodeCursor cursor) {
        return RelationshipSelections.incomingCursor(cursors, cursor, types, cursorContext);
    }

    @Override
    public RelationshipTraversalCursor relationshipsBoth(final NodeCursor cursor) {
        return RelationshipSelections.allCursor(cursors, cursor, types, cursorContext);
    }
}
