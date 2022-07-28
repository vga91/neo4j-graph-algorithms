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

import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.ImportProgress;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;


final class ScanningNodesImporter extends ScanningRecordsImporter<NodeRecord, IdsAndProperties> {

    private final ImportProgress progress;
    private final AllocationTracker tracker;
    private final PropertyMapping[] propertyMappings;

    private Map<String, HugeNodePropertiesBuilder> builders;
    private HugeLongArrayBuilder idMapBuilder;
//    private final KernelTransaction tx;

    ScanningNodesImporter(
            GraphDatabaseAPI api,
            GraphDimensions dimensions,
            ImportProgress progress,
            AllocationTracker tracker,
            ExecutorService threadPool,
            int concurrency,
            PropertyMapping[] propertyMappings,
            KernelTransaction tx) {
        super(NodeStoreScanner.NODE_ACCESS, "Node", api, dimensions, threadPool, concurrency, tx);
        this.progress = progress;
        this.tracker = tracker;
//        this.tx = tx;
        this.propertyMappings = propertyMappings;
    }

    @Override
    ImportingThreadPool.CreateScanner creator(
            long nodeCount,
            ImportSizing sizing,
            AbstractStorePageCacheScanner<NodeRecord> scanner) {
        idMapBuilder = HugeLongArrayBuilder.of(nodeCount, tracker);
        builders = propertyBuilders(nodeCount);
        return NodesScanner.of(
                api,
                scanner,
                dimensions.labelId(),
                progress,
                idMapBuilder,
                builders.values());
    }

    @Override
    IdsAndProperties build() {
        HugeIdMap hugeIdMap = HugeIdMapBuilder.build(idMapBuilder, dimensions.allNodesCount(), tracker);
        Map<String, HugeWeightMapping> nodeProperties = new HashMap<>();
        for (PropertyMapping propertyMapping : propertyMappings) {
            HugeNodePropertiesBuilder builder = builders.get(propertyMapping.propertyName);
            HugeWeightMapping props = builder != null ? builder.build() : new HugeNullWeightMap(propertyMapping.defaultValue);
            nodeProperties.put(propertyMapping.propertyName, props);
        }
        return new IdsAndProperties(hugeIdMap, Collections.unmodifiableMap(nodeProperties));
    }

    private Map<String, HugeNodePropertiesBuilder> propertyBuilders(long nodeCount) {
        if (propertyMappings == null || propertyMappings.length == 0) {
            return Collections.emptyMap();
        }
        Map<String, HugeNodePropertiesBuilder> builders = new HashMap<>();
        for (int i = 0; i < propertyMappings.length; i++) {
            PropertyMapping propertyMapping = propertyMappings[i];
            int propertyId = dimensions.nodePropertyKeyId(i);
            if (propertyId != StatementConstants.NO_SUCH_PROPERTY_KEY) {
                HugeNodePropertiesBuilder builder = HugeNodePropertiesBuilder.of(
                        nodeCount,
                        tracker,
                        propertyMapping.defaultValue,
                        propertyId);
                builders.put(propertyMapping.propertyName, builder);
            }
        }
        return builders;
    }
}
