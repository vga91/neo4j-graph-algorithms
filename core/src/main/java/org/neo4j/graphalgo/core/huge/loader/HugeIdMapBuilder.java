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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;

import java.util.Arrays;

final class HugeIdMapBuilder {

    static HugeIdMap build(
            HugeLongArrayBuilder idMapBuilder,
            long highestNodeId,
            AllocationTracker tracker) {
        System.out.println("idMapBuilder = " + idMapBuilder + ", highestNodeId = " + highestNodeId + ", tracker = " + tracker);
        HugeLongArray graphIds = idMapBuilder.build();
        System.out.println("graphIds = " + graphIds);
        SparseLongArray nodeToGraphIds = SparseLongArray.newArray(highestNodeId, tracker);
//        SparseLongArray nodeToGraphIds = SparseLongArray.newArray(highestNodeId + 10, tracker);
        System.out.println("nodeToGraphIds.toString() = " + nodeToGraphIds.toString());

        try (HugeLongArray.Cursor cursor = graphIds.cursor(graphIds.newCursor())) {
            while (cursor.next()) {
                // todo -???
                long[] array = cursor.array;
                System.out.println("array = " + Arrays.toString(array));
                int offset = cursor.offset;
                System.out.println("offset = " + offset);
                int limit = cursor.limit;
                System.out.println("limit = " + limit);
                long internalId = cursor.base + offset;
                System.out.println("internalId = " + internalId);
                for (int i = offset; i < limit; ++i, ++internalId) {
                    nodeToGraphIds.set(array[i], internalId);
                }
            }
        }

        return new HugeIdMap(graphIds, nodeToGraphIds, idMapBuilder.size());
    }

//    private Long getaLong(long element, HugeCursor<long[]> cursor) {
//        long[] internalArray = cursor.array;
//        int offset = cursor.offset;
//        int localLimit = cursor.limit - 4;
//        for (; offset <= localLimit; offset += 4) {
//            if (internalArray[offset] == element) return offset + cursor.base;
//            if (internalArray[offset + 1] == element) return offset + 1 + cursor.base;
//            if (internalArray[offset + 2] == element) return offset + 2 + cursor.base;
//            if (internalArray[offset + 3] == element) return offset + 3 + cursor.base;
//        }
//        for (; offset < cursor.limit; ++offset) {
//            if (internalArray[offset] == element) return offset + cursor.base;
//        }
//        return null;
//    }


    private HugeIdMapBuilder() {
    }
}
