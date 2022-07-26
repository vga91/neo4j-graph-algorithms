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

import org.neo4j.io.layout.recordstorage.RecordDatabaseFile;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public final class RelationshipStoreScanner extends AbstractStorePageCacheScanner<RelationshipRecord> {

    public static final AbstractStorePageCacheScanner.Access<RelationshipRecord> RELATIONSHIP_ACCESS = new Access<RelationshipRecord>() {
        @Override
        public RecordStore<RelationshipRecord> store(final NeoStores neoStores) {
            return neoStores.getRelationshipStore();
        }

        @Override
        public RecordFormat<RelationshipRecord> recordFormat(final RecordFormats formats) {
            return formats.relationship();
        }

        @Override
        public String storeFileName() {
            return RecordDatabaseFile.RELATIONSHIP_STORE.getName();
        }

        @Override
        public AbstractStorePageCacheScanner<RelationshipRecord> newScanner(
                final GraphDatabaseAPI api,
                final int prefetchSize) {
            return new RelationshipStoreScanner(prefetchSize, api);
        }
    };

    public static RelationshipStoreScanner of(GraphDatabaseAPI api) {
        return of(api, AbstractStorePageCacheScanner.DEFAULT_PREFETCH_SIZE);
    }

    public static RelationshipStoreScanner of(GraphDatabaseAPI api, int prefetchSize) {
        return new RelationshipStoreScanner(prefetchSize, api);
    }

    private RelationshipStoreScanner(final int prefetchSize, final GraphDatabaseAPI api) {
        // todo - null mocked, investigate
        super(prefetchSize, api, RELATIONSHIP_ACCESS/*, null*/);
    }

    @Override
    RelationshipStore store() {
        return (RelationshipStore) super.store();
    }
}
