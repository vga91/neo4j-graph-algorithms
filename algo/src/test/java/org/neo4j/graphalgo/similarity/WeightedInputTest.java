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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

public class WeightedInputTest {
    @Test
    public void degreeCutoffBasedOnSkipValue() {
        List<Map<String, Object>> data = new ArrayList<>();
        data.add(Map.of("item", 1L,"weights", Arrays.asList(2.0, 3.0, 4.0)));
        data.add(Map.of("item", 2L,"weights", Arrays.asList(2.0, 3.0, Double.NaN)));

        WeightedInput[] weightedInputs = WeightedInput.prepareDenseWeights(data, 2L, Double.NaN);

        assertEquals(1, weightedInputs.length);
    }

    @Test
    public void degreeCutoffWithoutSkipValue() {
        List<Map<String, Object>> data = new ArrayList<>();
        data.add(Map.of("item", 1L,"weights", Arrays.asList(2.0, 3.0, 4.0)));
        data.add(Map.of("item", 2L,"weights", Arrays.asList(2.0, 3.0, Double.NaN)));

        WeightedInput[] weightedInputs = WeightedInput.prepareDenseWeights(data, 2L, null);

        assertEquals(2, weightedInputs.length);
    }

    @Test
    public void degreeCutoffWithNumericSkipValue() {
        List<Map<String, Object>> data = new ArrayList<>();
        data.add(Map.of("item", 1L,"weights", Arrays.asList(2.0, 3.0, 4.0)));
        data.add(Map.of("item", 2L,"weights", Arrays.asList(2.0, 3.0, 5.0)));

        WeightedInput[] weightedInputs = WeightedInput.prepareDenseWeights(data, 2L, 5.0);

        assertEquals(1, weightedInputs.length);
    }

    @Test
    public void pearsonNoCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.pearson(null, -1.0, input2, true);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void pearsonCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, Weights.buildRleWeights(weights1, 3));
        WeightedInput input2 = new WeightedInput(2, Weights.buildRleWeights(weights2, 3));

        RleDecoder decoder = new RleDecoder(weights1.length);

        SimilarityResult similarityResult = input1.pearson(decoder, -1.0, input2, true);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void pearsonSkipNoCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.pearsonSkip(null, -1.0, input2, 0.0, true);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void pearsonSkipCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6, 0, 0, 0, 0};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6, 0, 0, 0, 0};

        WeightedInput input1 = new WeightedInput(1, Weights.buildRleWeights(weights1, 3));
        WeightedInput input2 = new WeightedInput(2, Weights.buildRleWeights(weights2, 3));

        RleDecoder decoder = new RleDecoder(weights1.length);

        SimilarityResult similarityResult = input1.pearsonSkip(decoder, -1.0, input2, 0.0, true);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void pearsonNaNReturns0() {
        double[] weights1 = new double[]{};
        double[] weights2 = new double[]{};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        assertEquals(0.0, input1.pearsonSkip(null, -1.0, input2, 0.0, true).similarity, 0.01);
        assertEquals(0.0, input1.pearson(null, -1.0, input2, true).similarity, 0.01);
    }

    @Test
    public void pearsonNaNRespectsSimilarityCutOff() {
        double[] weights1 = new double[]{};
        double[] weights2 = new double[]{};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        assertNull(input1.pearsonSkip(null, 0.1, input2, 0.0, true));
        assertNull(input1.pearson(null, 0.1, input2, true));
    }

    @Test
    public void pearsonWithNonOverlappingValues() {
        double[] weights1 = new double[]{1,          2,3, Double.NaN, 4};   // ave = 10/4 = 2.5
        double[] weights2 = new double[]{Double.NaN, 2,3, 1,          4};  // ave = 10/4 = 2.5

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.pearsonSkip(null, -1.0, input2, Double.NaN, true);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void cosineNoCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.cosineSquares(null, -1.0, input2, true);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void cosineCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, Weights.buildRleWeights(weights1, 3));
        WeightedInput input2 = new WeightedInput(2, Weights.buildRleWeights(weights2, 3));

        RleDecoder decoder = new RleDecoder(weights1.length);

        SimilarityResult similarityResult = input1.cosineSquares(decoder, -1.0, input2, true);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void cosineSkipNoCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.cosineSquaresSkip(null, -1.0, input2, 0.0, true);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void cosineSkipCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6, 0, 0, 0, 0};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6, 0, 0, 0, 0};

        WeightedInput input1 = new WeightedInput(1, Weights.buildRleWeights(weights1, 3));
        WeightedInput input2 = new WeightedInput(2, Weights.buildRleWeights(weights2, 3));

        RleDecoder decoder = new RleDecoder(weights1.length);

        SimilarityResult similarityResult = input1.cosineSquaresSkip(decoder, -1.0, input2, 0.0, true);

        assertEquals(1.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void euclideanNoCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.sumSquareDelta(null, -1.0, input2, true);

        assertEquals(0.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void euclideanCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, Weights.buildRleWeights(weights1, 3));
        WeightedInput input2 = new WeightedInput(2, Weights.buildRleWeights(weights2, 3));

        RleDecoder decoder = new RleDecoder(weights1.length);

        SimilarityResult similarityResult = input1.sumSquareDelta(decoder, -1.0, input2, true);

        assertEquals(0.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void euclideanSkipNoCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6};

        WeightedInput input1 = new WeightedInput(1, weights1);
        WeightedInput input2 = new WeightedInput(2, weights2);

        SimilarityResult similarityResult = input1.sumSquareDeltaSkip(null, -1.0, input2, 0.0, true);

        assertEquals(0.0, similarityResult.similarity, 0.01);
    }

    @Test
    public void euclideanSkipCompression() {
        double[] weights1 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6, 0, 0, 0, 0};
        double[] weights2 = new double[]{1, 2, 3, 4, 4, 4, 4, 5, 6, 0, 0, 0, 0};

        WeightedInput input1 = new WeightedInput(1, Weights.buildRleWeights(weights1, 3));
        WeightedInput input2 = new WeightedInput(2, Weights.buildRleWeights(weights2, 3));

        RleDecoder decoder = new RleDecoder(weights1.length);

        SimilarityResult similarityResult = input1.sumSquareDeltaSkip(decoder, -1.0, input2, 0.0, true);

        assertEquals(0.0, similarityResult.similarity, 0.01);
    }


}