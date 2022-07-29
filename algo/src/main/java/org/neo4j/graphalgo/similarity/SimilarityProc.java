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
package org.neo4j.graphalgo.similarity;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.HdrHistogram.DoubleHistogram;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.similarity.recorder.NonRecordingSimilarityRecorder;
import org.neo4j.graphalgo.similarity.recorder.RecordingSimilarityRecorder;
import org.neo4j.graphalgo.similarity.recorder.SimilarityRecorder;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.utils.StatementApi.executeAndAccept;
import static org.neo4j.graphalgo.similarity.TopKConsumer.topK;
import static org.neo4j.graphalgo.similarity.Weights.REPEAT_CUTOFF;

public class SimilarityProc {
    @Context
    public GraphDatabaseAPI api;
    @Context
    public Log log;
    @Context
    public KernelTransaction kernelTransaction;

    static TopKConsumer<SimilarityResult>[] initializeTopKConsumers(int length, int topK) {
        Comparator<SimilarityResult> comparator = topK > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        topK = Math.abs(topK);

        TopKConsumer<SimilarityResult>[] results = new TopKConsumer[length];
        for (int i = 0; i < results.length; i++) results[i] = new TopKConsumer<>(topK, comparator);
        return results;
    }

    static Stream<SimilarityResult> topN(Stream<SimilarityResult> stream, int topN) {
        if (topN == 0) {
            return stream;
        }
        Comparator<SimilarityResult> comparator = topN > 0 ? SimilarityResult.DESCENDING : SimilarityResult.ASCENDING;
        topN = Math.abs(topN);

        if (topN > 10000) {
            return stream.sorted(comparator).limit(topN);
        }
        return topK(stream, topN, comparator);
    }

    static SimilarityRecorder<WeightedInput> similarityRecorder(SimilarityComputer<WeightedInput> computer, ProcedureConfiguration configuration) {
        boolean showComputations = configuration.get("showComputations", false);
        return showComputations ? new RecordingSimilarityRecorder<>(computer) : new NonRecordingSimilarityRecorder<>(computer);
    }

    static SimilarityRecorder<CategoricalInput> categoricalSimilarityRecorder(SimilarityComputer<CategoricalInput> computer, ProcedureConfiguration configuration) {
        boolean showComputations = configuration.get("showComputations", false);
        return showComputations ? new RecordingSimilarityRecorder<>(computer) : new NonRecordingSimilarityRecorder<>(computer);
    }

    Long getDegreeCutoff(ProcedureConfiguration configuration) {
        return configuration.getNumber("degreeCutoff", 0L).longValue();
    }

    Long getWriteBatchSize(ProcedureConfiguration configuration) {
        return configuration.get("writeBatchSize", 10000L);
    }
    Stream<SimilaritySummaryResult> writeAndAggregateResults(Stream<SimilarityResult> stream, int length, int sourceIdsLength, int targetIdsLength, ProcedureConfiguration configuration, boolean write, String writeRelationshipType, String writeProperty, Computations computations) {
        long writeBatchSize = getWriteBatchSize(configuration);
        AtomicLong similarityPairs = new AtomicLong();
        DoubleHistogram histogram = new DoubleHistogram(5);
        Consumer<SimilarityResult> recorder = result -> {
            result.record(histogram);
            similarityPairs.getAndIncrement();
        };

        if (write) {
            SimilarityExporter similarityExporter = new SimilarityExporter(api, writeRelationshipType, writeProperty);
            similarityExporter.export(stream.peek(recorder), writeBatchSize);
        } else {
            stream.forEach(recorder);
        }

        return Stream.of(SimilaritySummaryResult.from(length, sourceIdsLength, targetIdsLength, similarityPairs, computations.count(), writeRelationshipType, writeProperty, write, histogram));
    }

    Stream<SimilaritySummaryResult> emptyStream(String writeRelationshipType, String writeProperty) {
        return Stream.of(SimilaritySummaryResult.from(0, 0,0, new AtomicLong(0), -1, writeRelationshipType,
                writeProperty, false, new DoubleHistogram(5)));
    }

    Double getSimilarityCutoff(ProcedureConfiguration configuration) {
        return configuration.getNumber("similarityCutoff", -1D).doubleValue();
    }

    <T> Stream<SimilarityResult> similarityStream(T[] inputs, int[] sourceIndexIds, int[] targetIndexIds, SimilarityComputer<T> computer, ProcedureConfiguration configuration, Supplier<RleDecoder> decoderFactory, double cutoff, int topK) {
        TerminationFlag terminationFlag = TerminationFlag.wrap(kernelTransaction);

        SimilarityStreamGenerator<T> generator = new SimilarityStreamGenerator<>(terminationFlag, configuration, decoderFactory, computer);
        if (sourceIndexIds.length == 0 && targetIndexIds.length == 0) {
            return generator.stream(inputs, cutoff, topK);
        } else {
            return generator.stream(inputs, sourceIndexIds, targetIndexIds, cutoff, topK);
        }
    }

    CategoricalInput[] prepareCategories(List<Map<String, Object>> data, long degreeCutoff) {
        CategoricalInput[] ids = new CategoricalInput[data.size()];
        int idx = 0;
        for (Map<String, Object> row : data) {
            List<Number> targetIds = SimilarityInput.extractValues(row.get("categories"));
            int size = targetIds.size();
            if (size > degreeCutoff) {
                long[] targets = new long[size];
                int i = 0;
                for (Number id : targetIds) {
                    targets[i++] = id.longValue();
                }
                Arrays.sort(targets);
                ids[idx++] = new CategoricalInput((Long) row.get("item"), targets);
            }
        }
        if (idx != ids.length) ids = Arrays.copyOf(ids, idx);
        Arrays.sort(ids);
        return ids;
    }

    WeightedInput[] prepareWeights(Object rawData, ProcedureConfiguration configuration, Double skipValue) throws Exception {
        if (ProcedureConstants.CYPHER_QUERY.equals(configuration.getGraphName("dense"))) {
            return prepareSparseWeights(api, (String) rawData,  skipValue, configuration);
        } else {
            List<Map<String, Object>> data = (List<Map<String, Object>>) rawData;
            return WeightedInput.prepareDenseWeights(data, getDegreeCutoff(configuration), skipValue);
        }
    }

    Double readSkipValue(ProcedureConfiguration configuration) {
        return configuration.get("skipValue", Double.NaN);
    }

    private WeightedInput[] prepareSparseWeights(GraphDatabaseAPI api, String query, Double skipValue, ProcedureConfiguration configuration) throws Exception {
        Map<String, Object> params = configuration.getParams();
        Long degreeCutoff = getDegreeCutoff(configuration);
        int repeatCutoff = configuration.get("sparseVectorRepeatCutoff", REPEAT_CUTOFF).intValue();

        Map<Long, LongDoubleMap> map = new HashMap<>();
        LongSet ids = new LongHashSet();
        
        executeAndAccept(api, query, params, resultRow -> {
            long item = resultRow.getNumber("item").longValue();
            long id = resultRow.getNumber("category").longValue();
            ids.add(id);
            double weight = resultRow.getNumber("weight").doubleValue();
            map.compute(item, (key, agg) -> {
                if (agg == null) agg = new LongDoubleHashMap();
                agg.put(id, weight);
                return agg;
            });
            return true;
        });

        WeightedInput[] inputs = new WeightedInput[map.size()];
        int idx = 0;

        long[] idsArray = ids.toArray();
        for (Map.Entry<Long, LongDoubleMap> entry : map.entrySet()) {
            Long item = entry.getKey();
            LongDoubleMap sparseWeights = entry.getValue();

            if (sparseWeights.size() > degreeCutoff) {
                List<Number> weightsList = new ArrayList<>(ids.size());
                for (long id : idsArray) {
                    weightsList.add(sparseWeights.getOrDefault(id, skipValue));
                }
                int size = weightsList.size();
                int nonSkipSize = sparseWeights.size();
                double[] weights = Weights.buildRleWeights(weightsList, repeatCutoff);

                inputs[idx++] = WeightedInput.sparse(item, weights, size, nonSkipSize);
            }
        }

        if (idx != inputs.length) inputs = Arrays.copyOf(inputs, idx);
        Arrays.sort(inputs);
        return inputs;
    }

    int getTopK(ProcedureConfiguration configuration) {
        return configuration.getInt("topK", 0);
    }

    int getTopN(ProcedureConfiguration configuration) {
        return configuration.getInt("top", 0);
    }

    private Supplier<RleDecoder> createDecoderFactory(String graphType, int size) {
        if(ProcedureConstants.CYPHER_QUERY.equals(graphType)) {
            return () -> new RleDecoder(size);
        }

        return () -> null;
    }


    Supplier<RleDecoder> createDecoderFactory(ProcedureConfiguration configuration, WeightedInput input) {
        int size = input.initialSize;
        return createDecoderFactory(configuration.getGraphName("dense"), size);
    }


}
