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
package org.neo4j.graphalgo.bench;

import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.similarity.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Threads(1)
@Fork(value = 1, jvmArgs = {"-Xms8g", "-Xmx8g", "-XX:+UseG1GC"})
@Warmup(iterations = 1000)
@Measurement(iterations = 1000, time = 2)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SimilarityStreamGeneratorBenchmark {

    @Param({"1", "2", "8"})
    public int concurrency;

    public static final int SIZE = 100;

    private SimilarityStreamGenerator<CategoricalInput> streamGenerator;
    private CategoricalInput[] inputs = generateInputs(SIZE);

    private CategoricalInput[] generateInputs(int numberOfInputs) {
        CategoricalInput[] inputs = new CategoricalInput[numberOfInputs];

        for (int i = 0; i < numberOfInputs; i++) {
            inputs[i] = new CategoricalInput(i, new long[]{});
        }

        return inputs;
    }

    @Benchmark
    public void allPairs(Blackhole bh) {
        streamGenerator.stream(inputs, -1.0, 0).forEach(bh::consume);
    }

    @Benchmark
    public void allPairsBlankSourceTarget(Blackhole bh) {
        streamGenerator.stream(inputs, new int[]{}, new int[]{}, -1.0, 0).forEach(bh::consume);
    }

    public static final SimilarityComputer<CategoricalInput> ALL_PAIRS_COMPUTER = (decoder, source, target, cutoff) ->
            similarityResult(source.getId(), target.getId(), true, false);

    private static SimilarityResult similarityResult(long sourceId, long targetId, boolean bidirectional, boolean reversed) {
        return new SimilarityResult(sourceId, targetId, -1, -1, -1, 0.7, bidirectional, reversed);
    }

    @Setup
    public void setup() {
        ProcedureConfiguration configuration = ProcedureConfiguration.create(Map.of("concurrency", concurrency));
        TerminationFlag terminationFlag = terminationFlag();
        streamGenerator = new SimilarityStreamGenerator<>(terminationFlag, configuration, () -> null, ALL_PAIRS_COMPUTER);
    }

    private TerminationFlag terminationFlag() {
        TerminationFlag terminationFlag = mock(TerminationFlag.class);
        when(terminationFlag.running()).thenReturn(true);
        return terminationFlag;
    }

    @TearDown
    public void shutdown() {
    }
}
