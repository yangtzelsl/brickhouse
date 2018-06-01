package brickhouse.experimental.vector.util;

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


import org.apache.log4j.Logger;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public class VectorOp {
    private static final Logger LOG = Logger.getLogger(VectorAggOp.class);

    /**
     * Performs vector scalar operation.
     * @param vec
     * @param op
     * @return
     * @throws java.util.Map
     */
    public static Map<String, Double> evaluate(Map<String, Double> vec,
                                               String op) throws Exception {
        if (vec == null || op == null) return null;


        // Deal with non numeric ops.
        if (op.equals("lower_key")) {
            return changeKeyCaseAndAggregate(vec, true);
        } else if (op.equals("upper_key")) {
            return changeKeyCaseAndAggregate(vec, false);
        } else if (op.equals("normalize")) {
            return normalize(vec);
        } else if (op.equals("vector_normalize")) {
            return vectorNormalize(vec);
        } if (op.equals("index")) {
            return index(vec);
        } else if (op.equals("sort_index")) {
            return index(sort(vec));
        } else if (op.equals("sort")) {
            return sort(vec);
        } else if (op.equals("sanitize")) {
            return sanitize(vec);
        }

        TreeMap<String, Double> out = new TreeMap<String,Double>();

        for (Map.Entry<String, Double> entry : vec.entrySet()) {
            Double result = op(entry.getValue(), op);
            if (result == null || result.equals(0.0)) continue;
            out.put(entry.getKey(), result);
        }
        return out;
    }

    /**
     * Same as evaluate but instead of throwing exception it returns null.
     * @param vec
     * @param op
     * @return
     */
    public static Map<String, Double> safeEvaluate(Map<String, Double> vec,
                                                   String op)  {
        Map<String, Double> output = null;
        try {
            output = evaluate(vec, op);
        } catch (Exception e) {
        }
        return output;
    }

    /**
     * Performs vector scalar operation.
     * @param vec
     * @param scalar
     * @param op
     * @return
     * @throws Exception
     */
    public static Map<String, Double> evaluate(Map<String, Double> vec,
                                               Double scalar,
                                               String op) throws Exception {
        if (vec == null || scalar == null || op == null) return null;

        if (op.equals("top_k")) {
            return topK(vec, scalar.intValue());
        } else if (op.equals("cap_upper")) {
            return capVector(vec, scalar, true);
        } else if (op.equals("cap_lower")) {
            return capVector(vec, scalar, false);
        }

        TreeMap<String, Double> out = new TreeMap<String,Double>();

        for (Map.Entry<String, Double> entry : vec.entrySet()) {
            String key = entry.getKey();
            Double result = op(entry.getValue(), scalar, op);
            if (result == null || result.equals(0.0)) continue;
            out.put(key, result);
        }
        return out;
    }

    /**
     * Same as evaluate but instead of throwing exception it returns null.
     * @param vec
     * @param scalar
     * @param op
     * @return
     */
    public static Map<String, Double> safeEvaluate(Map<String, Double> vec,
                                                   Double scalar,
                                                   String op)  {
        Map<String, Double> output = null;
        try {
            output = evaluate(vec, scalar, op);
        } catch (Exception e) {
        }
        return output;
    }

    /**
     * Same as above method except making sure we cast Int to double and not let hive guess to which sug method should it
     * cast.
     */
    public static Map<String, Double> evaluate(Map<String, Double> vec,
                                               Integer scalar,
                                               String op) throws Exception {
        if (scalar == null) return null;
        return evaluate(vec, (double)scalar, op);
    }

    /**
     * Same as evaluate but instead of throwing exception it returns null.
     * @param vec
     * @param scalar
     * @param op
     * @return
     */
    public static Map<String, Double> safeEvaluate(Map<String, Double> vec,
                                                   Integer scalar,
                                                   String op)  {
        Map<String, Double> output = null;
        try {
            output = evaluate(vec, scalar, op);
        } catch (Exception e) {
        }
        return output;
    }

    /**
     * Same as above method except making sure we cast Int to double and not let hive guess to which sug method should it
     * cast.
     */
    public static Map<String, Double> evaluate(Map<String, Double> vec,
                                               Long scalar,
                                               String op) throws Exception {
        if (scalar == null) return null;
        return evaluate(vec, (double)scalar, op);
    }

    /**
     * Same as evaluate but instead of throwing exception it returns null.
     * @param vec
     * @param scalar
     * @param op
     * @return
     */
    public static Map<String, Double> safeEvaluate(Map<String, Double> vec,
                                                   Long scalar,
                                                   String op)  {
        Map<String, Double> output = null;
        try {
            output = evaluate(vec, scalar, op);
        } catch (Exception e) {
        }
        return output;
    }

    // All the private methods go below this line.

    /**
     * Performs vector scalar operation.
     * @param vec
     * @param scalar
     * @param op
     * @return
     * @throws Exception
     */
    public static Map<String, Double> evaluate(Map<String, Double> vec,
                                               String scalar,
                                               String op) throws Exception {
        if (vec == null || scalar == null || op == null) return null;

        if (op.equals("add_key_suffix")) {
            return renameKey(vec, scalar, false);
        } else if(op.equals("add_key_prefix")) {
            return renameKey(vec, scalar, true);
        }
        throw new IOException("Operator '" +  op + "' not supported.");
    }

    /**
     * Same as evaluate but instead of throwing exception it returns null.
     * @param vec
     * @param scalar
     * @param op
     * @return
     */
    public static Map<String, Double> safeEvaluate(Map<String, Double> vec,
                                                   String scalar,
                                                   String op)  {
        Map<String, Double> output = null;
        try {
            output = evaluate(vec, scalar, op);
        } catch (Exception e) {
        }
        return output;
    }

    /**
     * Performs vector to vector operation.
     * @param vec1
     * @param vec2
     * @param op
     * @return
     * @throws Exception
     */
    public static Map<String, Double> evaluate(Map<String, Double> vec1,
                                               Map<String, Double> vec2,
                                               String op) throws Exception {
        if (op == null) return null;
        // If one of the vector's is null just assume it's empty vector.
        if (vec1 == null) {
            vec1 = new HashMap<String, Double>();
        }
        if (vec2 == null) {
            vec2 = new HashMap<String, Double>();
        }

        // Handle special non arithmetic ops.
        if (op.equals("select_by_key")) {
            return selectByKey(vec1, vec2);
        } else if (op.equals("filter_by_key")) {
            return filterByKey(vec1, vec2);
        }  else if (op.equals("filter_by_prefix")) {
            return filterByPrefix(vec1, vec2);
        } else if (op.equals("filter_by_key_with_defaults")) {
            return selectByKeyWithDefaults(vec1, vec2);
        } else if (op.equals("filter_if_contains")) {
            return filterIfContains(vec1, vec2, true);
        } else if (op.equals("filter_not_if_contains")) {
            return filterIfContains(vec1, vec2, false);
        } else if (op.equals("convolution")) {
            return convolve(vec1, vec2, false);
        } else if (op.equals("convolution_circular")) {
            return convolve(vec1, vec2, true);
        } else if (op.equals("min")) {
            return minMaxBound(vec1, vec2, true);
        } else if (op.equals("max")) {
            return minMaxBound(vec1, vec2, false);
        }

        TreeMap<String, Double> vec = new TreeMap<String,Double>();

        for (Map.Entry<String, Double> entry : vec1.entrySet()) {
            String key = entry.getKey();
            Double lhs = entry.getValue();
            Double rhs = vec2.containsKey(key) ? vec2.get(key) : 0.0;
            Double result = op(lhs, rhs, op);
            if (result == null || result.equals(0.0)) continue;
            vec.put(key, result);
        }

        // If multiplication or division, then job is done no need to iterate vec2.
        if (op.equals("*") || op.equals("/")) return vec;

        // Deal with leftovers from vec2.
        for (Map.Entry<String, Double> entry : vec2.entrySet()) {
            String key = entry.getKey();
            if (vec1.containsKey(key)) continue;
            Double lhs = vec1.containsKey(key) ? vec1.get(key) : 0.0;
            Double rhs = entry.getValue();
            Double result = op(lhs, rhs, op);
            if (result.equals(0.0)) continue;
            vec.put(key, result);
        }
        return vec;
    }

    /**
     * Same as evaluate but instead of throwing exception it returns null.
     * @param vec1
     * @param vec2
     * @param op
     * @return
     */
    public static Map<String, Double> safeEvaluate(Map<String, Double> vec1,
                                                   Map<String, Double> vec2,
                                                   String op)  {
        Map<String, Double> output = null;
        try {
            output = evaluate(vec1, vec2, op);
        } catch (Exception e) {
        }
        return output;
    }


    // The operators.
    private static Map<String, Double> changeKeyCaseAndAggregate(Map<String, Double> vec, boolean toLower) {
        TreeMap<String, Double> out = new TreeMap<String,Double>();
        for (Map.Entry<String, Double> entry : vec.entrySet()) {
            String key = entry.getKey();
            String newKey = toLower ? key.toLowerCase() : key.toUpperCase();
            Double result = out.get(newKey);
            result = (result == null) ? entry.getValue() : result + entry.getValue();
            out.put(newKey, result);
        }
        return out;
    }

    private static Map<String, Double> normalize(Map<String, Double> vec) throws Exception {
        Double sum = 0.0;
        for (Map.Entry<String, Double> entry : vec.entrySet()) {
            sum += entry.getValue();
        }
        return evaluate(vec, sum, "/");
    }

    public static Map<String, Double> vectorNormalize(final Map<String, Double> vec) throws Exception  {
        double sum = 0.0;
        for (double val : vec.values()) {
            sum += val * val;
        }
        if (sum <= 0.0) return null;
        return evaluate(vec, Math.sqrt(sum), "/");
    }

    private static Map<String, Double> index(Map<String, Double> vec) {
        LinkedHashMap indexLabeledMap = new LinkedHashMap<String, Double>();
        Double index = 0.0;
        for (Map.Entry<String, Double> entry : vec.entrySet()) {
            indexLabeledMap.put(entry.getKey(), index);
            index += 1;
        }
        return indexLabeledMap;
    }

    private static Map<String, Double> sort(Map<String, Double> vec) {

        if (vec == null || vec.size() == 0) return null;
        TreeMap<String, Double> sortedMap = new TreeMap<String,Double>(new DoubleMapValueComparator(vec));
        sortedMap.putAll(vec);

        LinkedHashMap sortedOutputMap = new LinkedHashMap<String, Double>();
        for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
            sortedOutputMap.put(entry.getKey(), entry.getValue());
        }
        return sortedOutputMap;
    }


    private static Map<String, Double> sanitize(Map<String, Double> vec) {
        if (vec == null || vec.size() == 0) return null;
        LinkedHashMap<String, Double> vecOut = new LinkedHashMap<String,Double>();
        for (Map.Entry<String, Double> entry : vec.entrySet()) {
            Double val = entry.getValue();
            if (val == null || val.isInfinite() || val.isNaN()) {
                continue;
            }
            vecOut.put(entry.getKey(), val);
        }
        if (vecOut.size() == 0) return null;
        return vecOut;
    }


    private static Double op(Double lhs, Double rhs, String op) throws Exception {
        if (lhs == null || rhs == null) return null;
        Double result = null;
        if (op.equals("+")) {
            result = lhs + rhs;
        } else if (op.equals("-")) {
            result = lhs - rhs;
        } else if (op.equals("*")) {
            result = lhs * rhs;
        } else if (op.equals("/")) {
            result = lhs / rhs;
        } else if (op.equals(">")) {
            // rhs is assumed to be threshold
            if (lhs > rhs) return lhs;
            return null;
        } else if (op.equals("<")) {
            // rhs is assumed to be threshold
            if (lhs < rhs) return lhs;
            return null;
        } else if (op.equals("round_digit")) {
            BigDecimal bigDecimal = new BigDecimal(lhs);
            return  bigDecimal.setScale(rhs.intValue(), BigDecimal.ROUND_HALF_UP).doubleValue();
        } else if(op.equals("round")) {
            return Math.round(lhs / rhs) * rhs;
        } else if(op.equals("ceil")) {
            return Math.ceil(lhs / rhs) * rhs;
        } else if (op.equals("pow") || op.equals("^")) {
            result = Math.pow(lhs, rhs);
        } else {
            throw new IOException("Operator '" +  op + "' not supported.");
        }
        return result;
    }

    private static Double op(Double value, String op) throws Exception {
        if (value == null || op == null) return null;
        Double result = null;
        if (op.equals("exp")) {
            result = Math.exp(value);
        } else if (op.equals("expm1")) {
            result = Math.expm1(value);
        } else if (op.equals("log")) {
            result = Math.log(value);
        } else if (op.equals("log10")) {
            result = Math.log10(value);
        } else if (op.equals("log1p")) {
            result = Math.log1p(value);
        } else if (op.equals("abs")) {
            result = Math.abs(value);
        } else if (op.equals("floor")) {
            result = Math.floor(value);
        } else if (op.equals("ceil")) {
            result = Math.ceil(value);
        } else if (op.equals("cos")) {
            result = Math.cos(value);
        } else if (op.equals("sin")) {
            result = Math.sin(value);
        } else if (op.equals("tan")) {
            result = Math.tan(value);
        } else if (op.equals("acos")) {
            result = Math.acos(value);
        } else if (op.equals("asin")) {
            result = Math.asin(value);
        } else if (op.equals("atan")) {
            result = Math.atan(value);
        } else if (op.equals("sinh")) {
            result = Math.sinh(value);
        } else if (op.equals("cosh")) {
            result = Math.cosh(value);
        } else if (op.equals("cbrt")) {
            result = Math.cbrt(value);
        } else if (op.equals("sqrt")) {
            result = Math.sqrt(value);
        } else {
            throw new IOException("Operator '" +  op + "' not supported.");
        }
        return result;
    }

    /**
     * Selects from vec1 all the keys that are in vec2, and returns it.
     * @param vec1
     * @param vec2
     * @return
     * @throws Exception
     */
    private static Map<String, Double> selectByKey(Map<String, Double> vec1,
                                                   Map<String, Double> vec2) throws Exception {
        //    DoubleMapValueComparator bvc =  new DoubleMapValueComparator(vec1);
        //    TreeMap<String, Double> out = new TreeMap<String,Double>(bvc);
        Map<String, Double> out = new HashMap<String,Double>();

        for (Map.Entry<String, Double> entry : vec1.entrySet()) {
            if (entry.getKey() == null) continue;
            if (vec2.get(entry.getKey()) != null) {
                out.put(entry.getKey(), entry.getValue());
            }
        }
        return out;
    }

    /**
     *
     * @param vec1
     * @param vec2
     * @param min - if true calculate lower bound otherwise assumes max, and calculates upper bound
     * @return
     * @throws Exception
     */
    private static Map<String, Double> minMaxBound(Map<String, Double> vec1,
                                                   Map<String, Double> vec2,
                                                   boolean min) throws Exception {
        Map<String, Double> out = new HashMap<String,Double>();
        Set<String> keys = new LinkedHashSet<String>();
        keys.addAll(vec1.keySet());
        keys.addAll(vec2.keySet());
        for (String key : keys) {
            Double value1 = vec1.get(key);
            Double value2 = vec2.get(key);
            if (value1 == null && value2 == null) {
                continue;
            }
            value1 = value1 == null ? value2 : value1;
            value2 = value2 == null ? value1 : value2;
            out.put(key, min ? Math.min(value1, value2) : Math.max(value1, value2));
        }
        return out;
    }

    /**
     *
     * @param vec
     * @param scalar
     * @param upper
     * @return
     * @throws Exception
     */
    private static Map<String, Double> capVector(Map<String, Double> vec,
                                                 Double scalar,
                                                 boolean upper) throws Exception {
        if (vec == null || scalar == null) {
            return null;
        }
        Map<String, Double> out = new LinkedHashMap<String,Double>();
        for (Map.Entry<String, Double> entry : vec.entrySet()) {
            Double value = entry.getValue();
            value = (upper && value > scalar) || (!upper && value < scalar) ? scalar : value;
            out.put(entry.getKey(), value);
        }
        return out;
    }


    /**
     * Removes (filters out) from vec1 all the keys that are in vec2, and returns it.
     * @param vec1
     * @param vec2
     * @return
     * @throws Exception
     */
    private static Map<String, Double> filterByKey(Map<String, Double> vec1,
                                                   Map<String, Double> vec2) throws Exception {
        Map<String, Double> out = new HashMap<String,Double>();
        for (Map.Entry<String, Double> entry : vec1.entrySet()) {
            if (entry.getKey() == null || vec2.get(entry.getKey()) != null)
                continue;
            out.put(entry.getKey(), entry.getValue());
        }
        return out;
    }

    /**
     * Removes (filters out) from vec1 all the keys that begin with prefixes in vec2, and returns it.
     * @param vec1
     * @param vec2
     * @return
     * @throws Exception
     */
    private static Map<String, Double> filterByPrefix(Map<String, Double> vec1,
                                                      Map<String, Double> vec2) throws Exception {
        Map<String, Double> out = new HashMap<String,Double>();
        for (Map.Entry<String, Double> entry : vec1.entrySet()) {
            if (entry.getKey() == null) continue;
            boolean filterKey = false;
            for (Map.Entry<String, Double> prefix : vec2.entrySet()) {
                if (entry.getKey().startsWith(prefix.getKey())) {
                    filterKey = true;
                }
            }
            if (!filterKey) {
                out.put(entry.getKey(), entry.getValue());
            }
        }
        return out;
    }

    /**
     * Filters out (or keeps) from vec1 all the keys that contains key in vec2, and returns it.
     * @param vec1
     * @param vec2
     * @return
     * @throws Exception
     */
    private static Map<String, Double> filterIfContains(Map<String, Double> vec1,
                                                        Map<String, Double> vec2,
                                                        Boolean filter) throws Exception {
        Map<String, Double> out = new HashMap<String,Double>();
        for (Map.Entry<String, Double> entry : vec1.entrySet()) {
            if (entry.getKey() == null) continue;
            boolean contains = false;
            for (Map.Entry<String, Double> vec2Entry : vec2.entrySet()) {
                if (entry.getKey().contains(vec2Entry.getKey())) {
                    contains = true;
                    break;
                }
            }
            if (filter != contains) {
                out.put(entry.getKey(), entry.getValue());
            }
        }
        return out;
    }

    /**
     * Selects from vec1 all the keys that are in vec2, and add default
     * key and value from vec2 if key is missed.
     * @param vec1
     * @param vec2
     * @return
     * @throws Exception
     */
    private static Map<String, Double> convolve(Map<String, Double> vec1,
                                                Map<String, Double> vec2,
                                                boolean circular) throws Exception {
        if (vec1 == null || vec2 == null) return null;
        // Consistency tests
        // Bucket differentials tests
        // Some other data integrity tests
        // Do we encode double or int as str on output ? ask Ad.

        Map<String, Double> out = new LinkedHashMap<String,Double>();

        // Sort base value functions.
        TreeMap<Integer, Double> sortedBaseFunction = new TreeMap<Integer, Double>();
        for (Map.Entry<String, Double> entry : vec1.entrySet()) {
            Integer value = new Double(Double.parseDouble(entry.getKey())).intValue();
            sortedBaseFunction.put(value, entry.getValue());
        }
        TreeMap<Integer, Double> sortedFilterFunction = new TreeMap<Integer, Double>();
        for (Map.Entry<String, Double> entry : vec2.entrySet()) {
            Integer value = new Double(Double.parseDouble(entry.getKey())).intValue();
            sortedFilterFunction.put(value, entry.getValue());
        }
        Integer maxBaseKey = sortedBaseFunction.lastKey();
        Integer modCycle = maxBaseKey + 1;
        for (Map.Entry<Integer, Double> entryBase : sortedBaseFunction.entrySet()) {
            Integer baseKey = entryBase.getKey();
            Double integralValue = 0.0;
            for (Map.Entry<Integer, Double> entryFilter : sortedFilterFunction.entrySet()) {
                Integer filterKey = entryFilter.getKey();
                Integer baseKeyIt = baseKey + filterKey;
                if (circular && (baseKeyIt > maxBaseKey || baseKeyIt < 0)) {
                    if (maxBaseKey == 0) break;
                    baseKeyIt = ((baseKeyIt % modCycle) + modCycle) % modCycle;
                }

                Double baseValue = sortedBaseFunction.get(baseKeyIt);
                Double filterValue = entryFilter.getValue();
                Double convolvedValue =
                        (baseValue == null ? 0.0 : baseValue) *
                                (filterValue == null ? 0.0 : filterValue);
                integralValue += convolvedValue;
            }
            out.put(baseKey.toString(), integralValue);
        }
        return out;
    }

    /**
     * Selects from vec1 all the keys that are in vec2, and add default
     * key and value from vec2 if key is missed.
     * @param vec1
     * @param vec2
     * @return
     * @throws Exception
     */
    private static Map<String, Double> selectByKeyWithDefaults(Map<String, Double> vec1,
                                                               Map<String, Double> vec2) throws Exception {
        Map<String, Double> out = new HashMap<String,Double>();

        for (Map.Entry<String, Double> entry : vec2.entrySet()) {
            if (entry.getKey() == null) continue;
            Double value = vec1.get(entry.getKey());
            if (value != null) {
                out.put(entry.getKey(), value);
            } else {
                out.put(entry.getKey(), entry.getValue());
            }
        }
        return out;
    }

    private static Map<String, Double> renameKey(Map<String, Double> vec, String appendStr, boolean prefix) {
        TreeMap<String, Double> out = new TreeMap<String,Double>();
        for (Map.Entry<String, Double> entry : vec.entrySet()) {
            String newKey = prefix ? (appendStr + entry.getKey()) : (entry.getKey() + appendStr);
            out.put(newKey, entry.getValue());
        }
        return out;
    }

    private static Map<String, Double> topK(Map<String, Double> map, Integer k)  {
        if (map == null || map.size() == 0) return null;
        TreeMap<String, Double> sortedMap = new TreeMap<String,Double>(new DoubleMapValueComparator(map));

        for (Map.Entry<String, Double> entry : map.entrySet()) {
            Double value = entry.getValue();
            String key =  entry.getKey();
            if (value == null) {
                LOG.debug("Null value for key = " + key);
                continue;
            }
            sortedMap.put(key, value);
            if (sortedMap.size() >  k) {
                sortedMap.pollLastEntry();
            }
        }

        // This looks wasteful but is the only good way to have sorted map not dependent on the DoubleMapValueComparator
        // which causes issues.
        LinkedHashMap sortedOutputMap = new LinkedHashMap<String, Double>();
        for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
            sortedOutputMap.put(entry.getKey(), entry.getValue());
        }
        return sortedOutputMap;
    }
}
