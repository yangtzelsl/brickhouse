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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;


public class VectorOpUDFTest {
    @Test
    public void testScalarOP() throws Exception {
        HashMap<String, Double> vec = new HashMap<String, Double>();
        vec.put("a", 1.0);
        vec.put("b", 2.0);
        VectorOpUDF udf = new VectorOpUDF();
        assert "{a=3.0, b=4.0}".equals(udf.evaluate(vec, 2.0, "+").toString());
        assert "{a=-1.0}".equals(udf.evaluate(vec, 2.0, "-").toString());
        assert "{a=2.0, b=4.0}".equals(udf.evaluate(vec, 2.0, "*").toString());
        assert "{a=0.5, b=1.0}".equals(udf.evaluate(vec, 2.0, "/").toString());
    }

    @Test
    public void testVectorOP() throws Exception {
        HashMap<String, Double> vec1 = new HashMap<String, Double>();
        vec1.put("a", 1.0);
        vec1.put("b", 2.0);

        HashMap<String, Double> vec2 = new HashMap<String, Double>();
        vec2.put("a", 2.0);
        vec2.put("b", 4.0);

        VectorOpUDF udf = new VectorOpUDF();
        assert "{a=3.0, b=6.0}".equals(udf.evaluate(vec1, vec2, "+").toString());
        assert "{a=-1.0, b=-2.0}".equals(udf.evaluate(vec1, vec2, "-").toString());
        assert "{a=2.0, b=8.0}".equals(udf.evaluate(vec1, vec2, "*").toString());
        assert "{a=0.5, b=0.5}".equals(udf.evaluate(vec1, vec2, "/").toString());
    }

    @Test
    public void testFilterOP() throws Exception {
        HashMap<String, Double> vec1 = new HashMap<String, Double>();
        vec1.put("a", 1.0);
        vec1.put("b", 2.0);
        vec1.put("c", 3.0);

        HashMap<String, Double> vec2 = new HashMap<String, Double>();
        vec2.put("a", 0.5);
        vec2.put("b", 4.0);

        VectorOpUDF udf = new VectorOpUDF();
        assert "{b=2.0, c=3.0}".equals(udf.evaluate(vec1, 1.5, ">").toString());
        assert "{a=1.0}".equals(udf.evaluate(vec1, 1.5, "<").toString());

        assert "{a=1.0, c=3.0}".equals(udf.evaluate(vec1, vec2, ">").toString());
        assert "{b=2.0}".equals(udf.evaluate(vec1, vec2, "<").toString());


        HashMap<String, Double> vec3 = new HashMap<String, Double>();
        vec3.put("a", 1.0);
        vec3.put("b", 2.0);
        vec3.put("c", 3.0);

        HashMap<String, Double> vec4 = new HashMap<String, Double>();
        vec4.put("a", 1.0);
        vec4.put("b", 0.0);

        HashMap<String, Double> vec5 = new HashMap<String, Double>();
        vec5.put("a", 0.0);
        vec5.put("d", 3.0);

        String result = udf.evaluate(vec3, vec4, "select_by_key").toString();
        assert "{a=1.0, b=2.0}".equals(result) ||
                "{b=2.0, a=1.0}".equals(result);
        assert "{c=3.0}".equals(udf.evaluate(vec3, vec4, "filter_by_key").toString());
        result = udf.evaluate(vec4, vec5, "filter_by_key_with_defaults").toString();
        assert "{a=1.0, d=3.0}".equals(result) ||
                "{d=3.0, a=1.0}".equals(result);

        vec1.clear();
        vec1.put("aa", 1.0);
        vec1.put("b", 2.0);
        vec1.put("cde", 3.0);

        result = udf.evaluate(vec1, vec2, "filter_by_prefix").toString();
        assert "{cde=3.0}".equals(result);
    }


    @Test
    public void testKeyStrOpOP() throws Exception {
        HashMap<String, Double> vec1 = new HashMap<String, Double>();
        vec1.put("a", 1.0);
        vec1.put("b", 2.0);
        vec1.put("c", 3.0);

        VectorOpUDF udf = new VectorOpUDF();
        assert "{1:a=1.0, 1:b=2.0, 1:c=3.0}".equals(udf.evaluate(vec1, "1:", "add_key_prefix").toString());
        assert "{a:2=1.0, b:2=2.0, c:2=3.0}".equals(udf.evaluate(vec1, ":2", "add_key_suffix").toString());
    }


    @Test
    public void testLowerKey() throws Exception {
        HashMap<String, Double> vec1 = new HashMap<String, Double>();
        vec1.put("aa", 1.0);
        vec1.put("bb", 2.0);
        vec1.put("Aa", 3.0);
        vec1.put("bB", 4.0);
        vec1.put("cc", 5.0);

        VectorOpUDF udf = new VectorOpUDF();
        assert "{aa=4.0, bb=6.0, cc=5.0}".equals(udf.evaluate(vec1, "lower_key").toString());
    }


    @Test
    public void normalize() throws Exception {
        HashMap<String, Double> vec1 = new HashMap<String, Double>();
        vec1.put("a", 1.0);
        vec1.put("b", 2.0);
        vec1.put("c", 3.0);
        vec1.put("d", 4.0);

        VectorOpUDF udf = new VectorOpUDF();
        assert "{a=0.1, b=0.2, c=0.3, d=0.4}".equals(udf.evaluate(vec1, "normalize").toString());
    }

    @Test
    public void precisionRound() throws Exception {
        HashMap<String, Double> vec1 = new HashMap<String, Double>();
        vec1.put("a", 1.123543);
        vec1.put("b", 2.223423);

        HashMap<String, Double> expectedRound = new HashMap<String, Double>();
        expectedRound.put("a", 1.12);
        expectedRound.put("b", 2.22);

        VectorOpUDF udf = new VectorOpUDF();
        Map<String, Double> val = udf.evaluate(vec1, 0.02 ,"round");
        assert almostEqual(val, expectedRound, 0.0001);
    }

    @Test
    public void precisionCeil() throws Exception {
        HashMap<String, Double> vec1 = new HashMap<String, Double>();
        vec1.put("a", 1.123543);
        vec1.put("b", 2.223423);

        HashMap<String, Double> expectedRound = new HashMap<String, Double>();
        expectedRound.put("a", 1.12);
        expectedRound.put("b", 2.24);

        VectorOpUDF udf = new VectorOpUDF();
        Map<String, Double> val = udf.evaluate(vec1, 0.02 ,"ceil");
        assert almostEqual(val, expectedRound, 0.0001);
    }


    // Helper methods below

    private Boolean almostEqual(Map<String, Double> val, Map<String, Double> expectedVal, Double epsilon) {
        if (val.size() != expectedVal.size()) return false;
        for (Map.Entry<String, Double> entry : val.entrySet()) {
            if (!almostEqual(entry.getValue(), expectedVal.get(entry.getKey()), epsilon)) {
                return false;
            }
        }
        return true;
    }

    private Boolean almostEqual(Double val, Double expectedVal, Double epsilon) {
        return (Math.abs(val) + epsilon) > Math.abs(expectedVal);
    }


    @Test
    public void sortIndexRankTest() throws Exception {
        VectorOpUDF udf = new VectorOpUDF();
        Map<String, Double> val = null;

        LinkedHashMap vec = new LinkedHashMap<String, Double>();
        vec.put("two", 2.2);
        vec.put("one", 1.1);
        vec.put("three", 3.3);

        val = udf.evaluate(vec, "index");
        assert val.toString().equals("{two=0.0, one=1.0, three=2.0}");

        val = udf.evaluate(vec, "sort");
        assert val.toString().equals("{three=3.3, two=2.2, one=1.1}");

        val = udf.evaluate(vec, "sort_index");
        assert val.toString().equals("{three=0.0, two=1.0, one=2.0}");

    }


    @Test
    public void sanitizeIndexRankTest() throws Exception {
        VectorOpUDF udf = new VectorOpUDF();
        Map<String, Double> val = null;

        LinkedHashMap vec = new LinkedHashMap<String, Double>();
        vec.put("one", 1.1);
        vec.put("two", 100 / 0.0);
        vec.put("twoz", -100 / 0.0);
        vec.put("three", 0.0 / 0.0);
        vec.put("four", null);

        val = udf.evaluate(vec, "sanitize");
        assert val.toString().equals("{one=1.1}");

    }


    @Test
    public void convolutionCircularTest() throws Exception {
        VectorOpUDF udf = new VectorOpUDF();
        Map<String, Double> val = null;

        LinkedHashMap vec1 = new LinkedHashMap<String, Double>();
        vec1.put("0.0", 1.0);
        vec1.put("1.0", 1.0);
        vec1.put("2.0", 1.0);
        vec1.put("3.0", 1.0);
        vec1.put("5.0", 2.0);
        vec1.put("4.0", 2.0);
        vec1.put("6.0", 2.0);
        vec1.put("7.0", 2.0);
        vec1.put("8.0", 1.0);
        vec1.put("9.0", 1.0);
        vec1.put("10.0", 1.0);

        LinkedHashMap vec2 = new LinkedHashMap<String, Double>();
        vec2.put("0.0", 0.25);
        vec2.put("1.0", 0.50);
        vec2.put("2.0", 0.25);

        val = udf.evaluate(vec1, vec2, "convolution_circular");
        assert val.toString().equals("{0=1.0, 1=1.0, 2=1.25, 3=1.75, 4=2.0, 5=2.0, 6=1.75, 7=1.25, 8=1.0, 9=1.0, 10=1.0}");

    }

    @Test
    public void convolutionTest() throws Exception {
        VectorOpUDF udf = new VectorOpUDF();
        Map<String, Double> val = null;

        LinkedHashMap vec1 = new LinkedHashMap<String, Double>();
        vec1.put("0.0", 1.0);
        vec1.put("1.0", 0.0);
        vec1.put("2.0", 0.0);

        LinkedHashMap vec2 = new LinkedHashMap<String, Double>();
        vec2.put("-1.0", 0.25);
        vec2.put("0.0", 0.50);
        vec2.put("1.0", 0.25);

        val = udf.evaluate(vec1, vec2, "convolution");
        assert val.toString().equals("{0=0.5, 1=0.25, 2=0.0}");

    }

    @Test
    public void convolutionShiftTest() throws Exception {
        VectorOpUDF udf = new VectorOpUDF();
        Map<String, Double> val = null;

        LinkedHashMap vec1 = new LinkedHashMap<String, Double>();
        vec1.put("0.0", 1.0);
        vec1.put("1.0", 0.0);
        vec1.put("2.0", 0.0);
        vec1.put("4.0", 0.0);

        LinkedHashMap vec2 = new LinkedHashMap<String, Double>();
        vec2.put("1.0", 1.0);

        val = udf.evaluate(vec1, vec2, "convolution_circular");
        assert val.toString().equals("{0=0.0, 1=0.0, 2=0.0, 4=1.0}");

    }


    @Test
    public void testCap() throws Exception {
        VectorOpUDF udf = new VectorOpUDF();
        LinkedHashMap vec = new LinkedHashMap<String, Double>();
        vec.put("a", -1.0);
        vec.put("b", 0.0);
        vec.put("c", 1.0);

        Map<String, Double> result = udf.evaluate(vec, -0.5, "cap_lower");
        assert result.toString().equals("{a=-0.5, b=0.0, c=1.0}") : "Got " + result ;
        result = udf.evaluate(vec, 0.5, "cap_upper");
        assert result.toString().equals("{a=-1.0, b=0.0, c=0.5}") : "Got " + result ;

        result = udf.evaluate(null, 0.5, "cap_lower");
        assert result == null;
    }
}
