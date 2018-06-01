package brickhouse.experimental.vector;

/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/

import brickhouse.experimental.vector.util.VectorAggOp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 *
 * Simple udf to collapse vector to the scalar.
 */
@Description(
        name = "vector_agg_op",
        value =
                "double _FUNC_(map<string, double> vec, string op) - simple udf to support vector to scalar collapse \n" +
                        "    operations operations. Operations supported('+', '*', 'min', 'max') \n" +
                        "double _FUNC_(map<string, double> vec1, map<string, double> vec2, string op) - simple udf to perform\n" +
                        "    vector op vector agg operations 'cos' (cosine similarity), and '*' (dot product)  \n" +
                        "    operations operations. Operations supported('+', '*', 'min', 'max') " +
                        "double _FUNC_(map<string, double> vec, array<string> set, Double scalar, string op) - \n" +
                        "  'precision_at_k' - vec precision for first k (scalar) entries given ground truth set (set)" +
                        "  'ndcg' - vec precision for first k (scalar) entries given ground truth set (set) \n" +
                        "      more info here http://en.wikipedia.org/wiki/Discounted_cumulative_gain \n" +
                        "      ndcg implementation considers the relevance value of the key in groundTruthLabels map\n" +
                        "        and evaluates a given order against the ideal ordering of the keys i.e. a sorted by value map of the grounTruthLabels\n" +
                        "      Expectation is client will normalize the relevance value in the groundTruthLabels according to use case" +
                        "double _FUNC_(map<string, double> vec, array<string> positiveSet,  array<string> negativeSet,  Double scalar, string op) - " +
                        "   'precision_at_k' - vec precision for first k (scalar) entries given complete positive and negative set  " +
                        "       entries that are not in set are not evaluated for precision. \n")

public class VectorAggOpUDF extends UDF {
    private static final Logger LOG = Logger.getLogger(VectorAggOp.class);

    // -------------------------------------------------------------------------------------------------------------------
    // --------------------------------------- VEC SCALAR AGG OPERATIONS -------------------------------------------------
    // -------------------------------------------------------------------------------------------------------------------

    /**
     * Performs unary operation on a single vector to convert to scalar.
     * @param vec
     * @param op
     * @return java.util.Map
     * @throws
     */
    public Double evaluate(Map<String, Double> vec,
                           String op) throws Exception {
        return VectorAggOp.evaluate(vec, op);
    }


    /**
     * Performs binary operation on two input vectors to convert to scalar.
     * @param vec1
     * @param vec2
     * @param op
     * @return java.util.Map
     * @throws
     */
    public Double evaluate(Map<String, Double> vec1,
                           Map<String, Double> vec2,
                           String op) throws Exception {
        return VectorAggOp.evaluate(vec1, vec2, op);
    }

    /**
     * Performs some of the metrics evaluation (rank / precision metrics).
     * @param vec
     * @param set
     * @param scalar
     * @param op
     * @return
     * @throws Exception
     */
    public Double evaluate(Map<String, Double> vec,
                           List<String> set,
                           Double scalar,
                           String op) throws Exception {
        return VectorAggOp.evaluate(vec, set, scalar, op);
    }

    /**
     * Performs some of the metrics evaluation (rank / precision metrics).
     * @param vec
     * @param positiveSet
     * @param negativeSet
     * @param scalar
     * @param op
     * @return
     * @throws Exception
     */
    public Double evaluate(Map<String, Double> vec,
                           List<String> positiveSet,
                           List<String> negativeSet,
                           Double scalar,
                           String op) throws Exception {
        return VectorAggOp.evaluate(vec, positiveSet, negativeSet, scalar, op);
    }

}
