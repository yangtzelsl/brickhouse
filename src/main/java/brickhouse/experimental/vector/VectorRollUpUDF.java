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

import brickhouse.experimental.vector.util.DoubleMapValueComparator;
import brickhouse.experimental.vector.util.LineReader;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import java.io.StringWriter;
import java.util.*;

/**
 * Simple UDF to roll up the given vector by given hiararchical schema.
 */
@Description(
        name = "vector_rollup",
        value =
                "Rolls up the vector weights based based on the rollup schema (assumed to be mapping low to top level).\n" +
                        "map<string, double> _FUNC_(map<string, double> vec, map<string, array<string>> roll_up_schema, bool weighted, <optional> bool verbose ) \n" +
                        "map<string, double> _FUNC_(map<string, double> vec, string roll_up_schema_path, string separator, bool weighted, , <optional> bool verbose) \n" +
                        "Example:_FUNC_({a:1, b:2, c:3}, {a:d, b:e, k:z}) returns {'d':4, 'e':2}" +
                        "Example (verbose) : _FUNC_({a:1, b:2, c:3}, {a:d, b:e, k:z}) returns { '{topTerm : d, bottomTerms: [a]  }':4, '{topTerm : e, bottomTerms [b] }':2} \n" +
                        " key is basically verbose json including info to which term rollup happened and list of terms that contributed to it."
)

public class VectorRollUpUDF extends UDF {
    private static final Logger LOG = Logger.getLogger(VectorRollUpUDF.class);
    Map<String, List<String> > rollUpSchema_ = null;

    /**
     *
     * @param vector
     * @param rollUpSchema
     * @param weighted
     * @return
     * @throws Exception
     */
    public Map<String, Double> evaluate(Map<String, Double> vector,
                                        Map<String, List<String>> rollUpSchema,
                                        Boolean weighted,
                                        Boolean versbose) throws Exception {
        if (vector == null || rollUpSchema == null || weighted == null) {
            return null;
        }
        Map<String, Double> rolledUpVector = vector.getClass().newInstance();
        Map<String, List<String> > rolledUpHistoryVector = new HashMap<String, List<String>>();

        for (Map.Entry<String, Double> entry : vector.entrySet()) {
            if (entry.getValue() == null || entry.getKey() == null) continue;
            List<String> rolleUps = rollUpSchema.get(entry.getKey());
            if (rolleUps == null) continue;
            Double weight = (weighted != null && weighted) ? (1.0 / rolleUps.size()) : 1.0;
            for (String rollUp : rolleUps) {
                Double value = rolledUpVector.get(rollUp);
                if (value == null) {
                    value = 0.0;
                }

                // Add rollup.
                rolledUpVector.put(rollUp, value + entry.getValue() * weight);
                if (!rolledUpHistoryVector.containsKey(rollUp)) {
                    rolledUpHistoryVector.put(rollUp, new ArrayList());
                }
                rolledUpHistoryVector.get(rollUp).add(entry.getKey());
            }
        }

        // Sort map for beauty of it.
        rolledUpVector = sortMap(rolledUpVector);

        return !versbose ? rolledUpVector : renderVerboseOutput(rolledUpVector, rolledUpHistoryVector);
    }

    public Map<String, Double> evaluate(Map<String, Double> vector,
                                        Map<String, List<String>> rollUpSchema,
                                        Boolean weighted) throws Exception {
        return evaluate(vector, rollUpSchema, weighted, false);
    }

    /**
     *
     * @param vector
     * @param rollUpSchemaPath
     * @param separator
     * @param weighted
     * @return
     * @throws Exception
     */
    public Map<String, Double> evaluate(Map<String, Double> vector,
                                        String rollUpSchemaPath,
                                        String separator,
                                        Boolean weighted,
                                        Boolean verbose) throws Exception {
        if (rollUpSchemaPath == null || separator == null) return null;
        if (rollUpSchema_ == null) {
            initRollUpSchema(rollUpSchemaPath, separator);
        }
        return evaluate(vector, rollUpSchema_, weighted, verbose);
    }

    public Map<String, Double> evaluate(Map<String, Double> vector,
                                        String rollUpSchemaPath,
                                        String separator,
                                        Boolean weighted) throws Exception {
        return evaluate(vector, rollUpSchemaPath, separator, weighted, false);
    }

    /**
     *
     * @param dictionaryPath
     * @param separator
     * @throws Exception
     */
    private synchronized void initRollUpSchema(String dictionaryPath,
                                               String separator) throws Exception {
        if (rollUpSchema_ != null) {
            LOG.debug("Dictionary already initialized.");
            return;
        }
        rollUpSchema_ = new HashMap<String, List<String> >();
        LineReader reader = new LineReader(dictionaryPath);
        String line = null;
        int cnt = 0;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split("\001");
            if (fields.length >= 2) {
                rollUpSchema_.put(fields[0], Arrays.asList(fields[1].split(separator)));
                if (cnt++ % 1000 == 0) {
                    LOG.debug("Adding:" + fields[0] + " -> " + Arrays.toString(fields[1].split(separator)));
                }
            } else {
                LOG.debug("Ignoring line " + line);
            }
        }
    }

    private Map<String, Double> renderVerboseOutput(Map<String, Double> rolledUpVector,
                                                    Map<String, List<String> > rolledUpHistoryVector) {
        LinkedHashMap<String, Double> outputMap = new LinkedHashMap<String, Double>();

        try {
            for (Map.Entry<String, Double> entry : rolledUpVector.entrySet()) {
                StringWriter writer = new StringWriter();
                JsonFactory jfactory = new JsonFactory();
                JsonGenerator jGenerator = jfactory.createJsonGenerator(writer);
                jGenerator.setPrettyPrinter(new DefaultPrettyPrinter());
                jGenerator.writeStartObject();
                jGenerator.writeStringField("topTerm", entry.getKey());
                jGenerator.writeArrayFieldStart("bottomTerms");

                for (String bottomTerm : rolledUpHistoryVector.get(entry.getKey())) {
                    jGenerator.writeString(bottomTerm);
                }

                jGenerator.writeEndArray();
                jGenerator.writeEndObject();
                jGenerator.close();
                String verboseKey = writer.toString().replace("\n", " ");

                outputMap.put(verboseKey, entry.getValue());
            }
        } catch (Exception e) {
            return null;
        }

        return outputMap;
    }


    private Map<String, Double> sortMap(Map<String, Double> map) {
        if (map == null || map.size() == 0) return null;
        TreeMap<String, Double> sortedMap = new TreeMap<String,Double>(new DoubleMapValueComparator(map));
        for (Map.Entry<String, Double> entry : map.entrySet()) {
            Double value = entry.getValue();
            String key =  entry.getKey();
            if (value == null) {
                continue;
            }
            sortedMap.put(key, value);
        }
        LinkedHashMap sortedOutputMap = new LinkedHashMap<String, Double>();
        for (Map.Entry<String, Double> entry : sortedMap.entrySet()) {
            sortedOutputMap.put(entry.getKey(), entry.getValue());
        }
        return sortedOutputMap;
    }

}
