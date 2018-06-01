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

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VectorRollUpUDFTest {

    @Test
    public void testUDF() throws Exception {
        VectorRollUpUDF udf = new VectorRollUpUDF();
        Map<String, Double> vec = new HashMap<String, Double>();
        vec.put("a", 1.0);
        vec.put("b", 2.0);
        vec.put("c", 3.0);
        vec.put("d", 4.0);
        vec.put("e", 5.0);
        Map<String, List<String>> rollUpSchema = new HashMap<String, List<String>>();
        rollUpSchema.put("a", Arrays.asList("1", "2"));
        rollUpSchema.put("b", Arrays.asList("2"));
        rollUpSchema.put("d", Arrays.asList("3","2"));
        rollUpSchema.put("e", Arrays.asList("4"));
        rollUpSchema.put("z", Arrays.asList("5"));

        // Non weighted
        Map<String, Double> vecRolledUp  = udf.evaluate(vec, rollUpSchema, false);

        Map<String, Double> expectedNonWeightedVec = new HashMap<String, Double>();
        expectedNonWeightedVec.put("3", 4.0);
        expectedNonWeightedVec.put("1", 1.0);
        expectedNonWeightedVec.put("2", 7.0);
        expectedNonWeightedVec.put("4", 5.0);
        assert expectedNonWeightedVec.equals(vecRolledUp);

        // Weighted
        Map<String, Double> expectedWeightedVec = new HashMap<String, Double>();
        expectedWeightedVec.put("3", 2.0);
        expectedWeightedVec.put("1", 0.5);
        expectedWeightedVec.put("2", 4.5);
        expectedWeightedVec.put("4", 5.0);
        vecRolledUp  = udf.evaluate(vec, rollUpSchema, true);
        assert expectedWeightedVec.equals(vecRolledUp);

        // Test verbose roll-up
        vecRolledUp  = udf.evaluate(vec, rollUpSchema, true, true);
        assert vecRolledUp.toString().replace(" ", "").equals(
                "{{\"topTerm\":\"4\",\"bottomTerms\":[\"e\"]}=5.0," +
                        "{\"topTerm\":\"2\",\"bottomTerms\":[\"d\",\"b\",\"a\"]}=4.5," +
                        "{\"topTerm\":\"3\",\"bottomTerms\":[\"d\"]}=2.0," +
                        "{\"topTerm\":\"1\",\"bottomTerms\":[\"a\"]}=0.5}");
    }

}