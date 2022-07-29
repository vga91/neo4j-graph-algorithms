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

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.storageengine.api.StubStorageCursors;
import org.neo4j.values.storable.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StubNodeCursor /*implements NodeCursor */{
//    private int offset;
//    private boolean dense;
//    private List<NodeData> nodes;
//
//    public StubNodeCursor() {
//        this(true);
//    }
//
//    public StubNodeCursor(boolean dense) {
//        this.offset = -1;
//        this.nodes = new ArrayList();
//        this.dense = dense;
//    }
//
//    void single(long reference) {
//        this.offset = 2147483647;
//
//        for(int i = 0; i < this.nodes.size(); ++i) {
//            if (reference == ((NodeData)this.nodes.get(i)).id) {
//                this.offset = i - 1;
//            }
//        }
//
//    }
//
//    void scan() {
//        this.offset = -1;
//    }
//
//    public StubNodeCursor withNode(long id) {
//        this.nodes.add(new NodeData(id, new long[0], Collections.emptyMap()));
//        return this;
//    }
//
//    public StubNodeCursor withNode(long id, long... labels) {
//        this.nodes.add(new NodeData(id, labels, Collections.emptyMap()));
//        return this;
//    }
//
//    public StubNodeCursor withNode(long id, long[] labels, Map<Integer, Value> properties) {
//        this.nodes.add(new NodeData(id, labels, properties));
//        return this;
//    }
//
//    public long nodeReference() {
//        return this.offset >= 0 && this.offset < this.nodes.size() ? ((NodeData)this.nodes.get(this.offset)).id : -1L;
//    }
//
//    public LabelSet labels() {
//        return this.offset >= 0 && this.offset < this.nodes.size() ? ((NodeData)this.nodes.get(this.offset)).labelSet() : LabelSet.NONE;
//    }
//
//    public boolean hasLabel(int label) {
//        return this.labels().contains(label);
//    }
//
//    public void relationships(RelationshipGroupCursor cursor) {
//        ((StubGroupCursor)cursor).rewind();
//    }
//
//    public void allRelationships(RelationshipTraversalCursor relationships) {
//        ((StubRelationshipCursor)relationships).rewind();
//    }
//
//    public void properties(PropertyCursor cursor) {
//        ((StubPropertyCursor)cursor).init(((StubStorageCursors.NodeData)this.nodes.get(this.offset)).properties);
//    }
//
//    public long relationshipGroupReference() {
//        throw new UnsupportedOperationException("not implemented");
//    }
//
//    public long allRelationshipsReference() {
//        throw new UnsupportedOperationException("not implemented");
//    }
//
//    public long propertiesReference() {
//        if (this.offset >= 0 && this.offset < this.nodes.size()) {
//            NodeData node = (NodeData)this.nodes.get(this.offset);
//            if (!node.properties.isEmpty()) {
//                return node.id;
//            }
//        }
//
//        return -1L;
//    }
//
//    public boolean isDense() {
//        return this.dense;
//    }
//
//    public boolean next() {
//        if (this.offset == 2147483647) {
//            return false;
//        } else {
//            return ++this.offset < this.nodes.size();
//        }
//    }
//
//    public void close() {
//    }
//
//    public boolean isClosed() {
//        return false;
//    }
}
