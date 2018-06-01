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

import brickhouse.experimental.vector.util.VectorOp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 *
 * Simple udf to support vector operations that may be useful when dealing with bags of words.
 *
 */
@Description(
        name = "vector_op",
        value =
                "Simple udf to support vector operations ('+', '-', '*' - vector to vec is scalar product, \n" +
                        " and '/' - vec to vec division is performed on same key values. Non existing values in the vectors are assumed to be 0.0.\n\n" +
                        "map<string, double> _FUNC_(map<string, double> vec, string scalar, string op) - scalar operation where scalar is string. \n" +
                        "map<string, double> _FUNC_(map<string, double> vec, double scalar, string op) - scalar operation. \n" +
                        "map<string, double> _FUNC_(map<string, double> vec1, map<string, double> vec2, string op) - vector operation.\n" +
                        "map<string, double> _FUNC_(map<string, double> vec1, string op) - function op where op can be \n" +
                        " exp, ln, log10, sin, cos, tn, asin, normalize, vector_normalize, acos, atan, sinh, cosh, cbrt, sqrt, " +
                        "round (for eps precision too), ceil (for eps precision too), \n " +
                        " > (keeps greater than value), < (keeps lower than value), select_by_key, filter_by_key, \n" +
                        " add_key_suffix, add_key_prefix, lower_key, upper_key, index (replaces values by it's index/rank in map), sort, \n" +
                        " sort_index (replaces value of map with values's rank within the map ), sanitize (cleans nan and inf values ), \n" +
                        "top_k, convolution_circular, convolution, min/max (across 2 vectors), cap_upper, cap_lower " +
                        "pow, ^, ... pretty much all standard math functions.\n"
)
public class VectorOpUDF extends UDF {
    private static final Logger LOG = Logger.getLogger(VectorOpUDF.class);

    /**
     * Performs vector scalar operation.
     * @param vec
     * @param op
     * @return
     * @throws java.util.Map
     */
    public Map<String, Double> evaluate(Map<String, Double> vec,
                                        String op) throws Exception {
        return VectorOp.evaluate(vec, op);
    }

    /**
     * Performs vector scalar operation.
     * @param vec
     * @param scalar
     * @param op
     * @return
     * @throws Exception
     */
    public Map<String, Double> evaluate(Map<String, Double> vec,
                                        Double scalar,
                                        String op) throws Exception {
        return VectorOp.evaluate(vec, scalar, op);
    }

    /**
     * Same as above method except making sure we cast Int to double and not let hive guess to which sug method should it
     * cast.
     */
    public Map<String, Double> evaluate(Map<String, Double> vec,
                                        Integer scalar,
                                        String op) throws Exception {
        return VectorOp.evaluate(vec, scalar, op);
    }

    /**
     * Same as above method except making sure we cast Int to double and not let hive guess to which sug method should it
     * cast.
     */
    public Map<String, Double> evaluate(Map<String, Double> vec,
                                        Long scalar,
                                        String op) throws Exception {
        return VectorOp.evaluate(vec, scalar, op);
    }

    /**
     * Performs vector scalar operation.
     * @param vec
     * @param scalar
     * @param op
     * @return
     * @throws Exception
     */
    public Map<String, Double> evaluate(Map<String, Double> vec,
                                        String scalar,
                                        String op) throws Exception {
        return VectorOp.evaluate(vec, scalar, op);
    }

    /**
     * Performs vector to vector operation.
     * @param vec1
     * @param vec2
     * @param op
     * @return
     * @throws Exception
     */
    public Map<String, Double> evaluate(Map<String, Double> vec1,
                                        Map<String, Double> vec2,
                                        String op) throws Exception {
        return VectorOp.evaluate(vec1, vec2, op);
    }
}
