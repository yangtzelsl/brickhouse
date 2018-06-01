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
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.log4j.Logger;

import java.util.*;

/**
 *
 * Vector collector udaf with support for max, min, sum operations.
 *
 */
@Description(
        name = "vector_collector_op",
        value =
                "Applies the op over collected maps, supported ops = max, min, +." +
                        "map<string, double> _FUNC_(map<string, double> map, String op) - \n" +
                        " map<string, double> _FUNC_(map(string key, double value), String op) - \n " +
                        "returns an map containing the op of key->values (of input map of input values) " +
                        "of all elements in the aggregation group."
)
public final class VectorCollectorOpUDAF extends UDAF {
    private static final Logger LOG = Logger.getLogger(VectorAggOp.class);

    static final String[] SUPPORTED_OPERATIONS = {"max", "min", "+"};
    static final Set<String> SUPPORTED_OPERATIONS_SET = new HashSet<String>(Arrays.asList(SUPPORTED_OPERATIONS));

    /**
     * UDAF state.
     */
    public static class UDAFState {
        private Map<String, Double> vector_ = new HashMap<String, Double>();
        private String operationType = new String();
    }

    /**
     * The helper class doing the aggregation.
     */
    public static class VectorCollectorOpEvaluator implements UDAFEvaluator {
        UDAFState state;

        public VectorCollectorOpEvaluator() {
            super();
            state = new UDAFState();
            init();
        }

        /**
         * Reset the state of the aggregation.
         */
        public void init() {
            state.vector_ = new HashMap<String, Double>();
            state.operationType = new String();
        }

        /**
         * Iterates throughout the input map and adds it to the current state.
         */
        public boolean iterate(Map<String, Double> map, String operation) throws HiveException {
            if (map == null || map.size() == 0) return true;

            state.operationType = operation;
            for (Map.Entry<String, Double> entry : map.entrySet()) {
                if (entry.getValue() == null) continue;
                operate(state.vector_, entry.getKey(), entry.getValue(), state.operationType);
            }

            return true;
        }

        /**
         * Simple operate method, that does state update per key / value.
         */
        private static void operate(Map<String, Double> map, String key, Double value, String operation) throws HiveException {
            Double currentValue = map.get(key);

            if (operation == null || !SUPPORTED_OPERATIONS_SET.contains(operation)) {
                throw new HiveException("Operator '" + operation + "'. not supported.");
            }

            try {
                if (currentValue == null) {
                    map.put(key, value);
                } else if (operation.equals("max")) {
                    Double maxValue = (currentValue > value ? currentValue : value);
                    map.put(key, maxValue);
                } else if (operation.equals("min")) {
                    Double minValue = (currentValue > value ? value : currentValue);
                    map.put(key, minValue);
                } else if (operation.equals("+")) {
                    map.put(key, value + currentValue);
                }
            } catch (Exception e) {
                LOG.debug("Current map size= " + map.size() + " operation is set as= " + operation + " exception is");
                e.printStackTrace();
                throw new HiveException(e.getMessage());
            }
        }

        /**
         * Terminate and return state.
         */
        public UDAFState terminatePartial() {
            // This is SQL standard - average of zero items should be null.
            return state.vector_.size() == 0 ? null : state;
        }

        /**
         * Partial aggregation merge.
         */
        public boolean merge(UDAFState o) throws HiveException {
            if (o == null) return true;
            state.operationType = o.operationType;
            for (Map.Entry<String, Double> entry : o.vector_.entrySet()) {
                if (entry.getValue() == null) continue;
                operate(state.vector_, entry.getKey(), entry.getValue(), o.operationType);
            }
            return true;
        }

        /**
         * Terminates the aggregation and return the final result.
         */
        public Map<String, Double> terminate() {
            // This is SQL standard - empty list should be null.
            return state.vector_.size() == 0 ? null : state.vector_;
        }
    }

    /**
     * Need to be private so we prevent instantiation.
     */
    private VectorCollectorOpUDAF() {
    }
}
