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
package org.neo4j.graphalgo.core;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProcedureConfigurationTest {

    @Test
    public void useDefault() {
        Map<String, Object> map = Collections.emptyMap();
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        String value = procedureConfiguration.get("partitionProperty", "defaultValue");
        assertEquals("defaultValue", value);
    }

    @Test
    public void returnValueIfPresent() {
        Map<String, Object> map = Map.of("partitionProperty", "partition");
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        String value = procedureConfiguration.get("partitionProperty", "defaultValue");
        assertEquals("partition", value);
    }

    @Test
    public void newKeyIfPresent() {
        Map<String, Object> map = Map.of("partitionProperty", "old", "writeProperty", "new");
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        String value = procedureConfiguration.get("writeProperty", "partitionProperty", "defaultValue");
        assertEquals("new", value);
    }

    @Test
    public void oldKeyIfNewKeyNotPresent() {
        Map<String, Object> map = Map.of("partitionProperty", "old");
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        String value = procedureConfiguration.get("writeProperty", "partitionProperty", "defaultValue");
        assertEquals("old", value);
    }

    @Test
    public void defaultIfNoKeysPresent() {
        Map<String, Object> map = Collections.emptyMap();
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        String value = procedureConfiguration.get("writeProperty", "partitionProperty", "defaultValue");
        assertEquals("defaultValue", value);
    }

    @Test
    public void defaultIfKeyMissing() {
        Map<String, Object> map = Collections.emptyMap();
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        assertEquals("defaultValue", procedureConfiguration.getString("writeProperty", "defaultValue"));
    }

    @Test
    public void defaultIfKeyPresentButNoValue() {
        Map<String, Object> map = Map.of("writeProperty", "");
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        assertEquals("defaultValue", procedureConfiguration.getString("writeProperty", "defaultValue"));
    }

    @Test
    public void valueIfKeyPresent() {
        Map<String, Object> map = Map.of("writeProperty", "scc");
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        assertEquals("scc", procedureConfiguration.getString("writeProperty", "defaultValue"));
    }

    @Test
    public void convertNonDoubleDefaultValues() {
        Map<String, Object> map = Map.of("defaultValue", 1L);
        ProcedureConfiguration procedureConfiguration = ProcedureConfiguration.create(map);
        assertEquals(1.0, procedureConfiguration.getWeightPropertyDefaultValue(0.0), 0.001);
    }
}
