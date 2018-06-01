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

import java.util.*;


public class VectorAggOpUDFTest {
    @Test
    public void test() throws Exception {
        HashMap<String, Double> vec = new HashMap<String, Double>();
        vec.put("a", 1.0);
        vec.put("b", 2.0);
        vec.put("c", 6.6);
        vec.put("d", 1.0);
        vec.put("e", -1.0);
        vec.put("f", 1.0);
        vec.put("g", -1.0);

        VectorAggOpUDF udf = new VectorAggOpUDF();
        // +
        assert udf.evaluate(vec, "+").equals(9.6);
        // -
        assert udf.evaluate(vec, "*").equals(13.2);
        // max
        assert udf.evaluate(vec, "max").equals(6.6);
        // min
        assert udf.evaluate(vec, "min").equals(-1.0);
        // Abs
        assert new Long(725).equals(Math.round(100 * udf.evaluate(vec, "abs")));
    }


    @Test
    public void testBinaryOp() throws Exception {
        HashMap<String, Double> vec1 = new HashMap<String, Double>();
        vec1.put("a", 1.0);
        vec1.put("b", 2.0);

        HashMap<String, Double> vec2 = new HashMap<String, Double>();
        vec2.put("b", 1.0);
        vec2.put("c", 2.0);

        VectorAggOpUDF udf = new VectorAggOpUDF();

        // *
        assert udf.evaluate(vec1, vec2, "*").equals(2.0);
        // cos
        assert new Long(40).equals(Math.round(100 * udf.evaluate(vec1, vec2, "cos")));
    }

    @Test
    public void testMetricsRankClassOp() throws Exception {
        HashMap<String, Double> vec1 = new HashMap<String, Double>();
        vec1.put("a", 1.0);
        vec1.put("b", 2.0);
        vec1.put("c", 3.0);
        vec1.put("d", 4.0);

        // changing ground truth set size to demonstrate correct usage
        // explanantion of precision at rank: https://www.youtube.com/watch?v=H7oAofuZjjE&list=PLBv09BD7ez_6nqE9YU9bQXpjJ5jJ1Kgr9&index=10
        String[] arr = {"a", "c", "e", "f", "h", "g"};
        List<String> set = Arrays.asList(arr);

        VectorAggOpUDF udf = new VectorAggOpUDF();

        assert new Double(udf.evaluate(vec1, set, 2.0, "precision_at_k") * 100.0).intValue() == 50;
        assert new Double(udf.evaluate(vec1, set, 3.0, "precision_at_k") * 100.0).intValue() == 33;
        assert new Double(udf.evaluate(vec1, set, 2.0, "ndcg") * 100.0).intValue() == 22;
        assert new Double(udf.evaluate(vec1, set, 4.0, "ndcg") * 100.0).intValue() == 25;

        HashMap<String, Double> vec2 = new HashMap<String, Double>();
        vec2.put("a", 3.0);
        vec2.put("b", 2.0);
        vec2.put("c", 3.0);
        vec2.put("d", 0.0);
        vec2.put("e", 1.0);
        vec2.put("f", 2.0);

        String[] arr1 = {"a", "b", "c", "d", "e", "f"};
        set = Arrays.asList(arr1);
        assert new Double(udf.evaluate(vec2, set, 6.0, "ndcg") * 100.0).intValue() == 94;

        vec2.clear();
        vec2.put("a", 1.0);
        vec2.put("b", 2.0);
        vec2.put("c", 3.0);
        vec2.put("d", 4.0);
        vec2.put("e", 5.0);

        // higher score = higher relevance, so following order of assignedLabel is worst:
        // if score is rank/group_count in a set i.e. less rank = more score, be sure to reverse the order of assignedLabels to get right result
        String[] sortedArr  = {"a", "b", "c", "d", "e"};
        set = Arrays.asList(sortedArr);
        assert new Double(udf.evaluate(vec2, set, 5.0, "ndcg") * 100.0).intValue() == 54;

        // higher score = higher relevance, so following order of assignedLabel is best:
        String[] reverseSortedArr  = {"e", "d", "c", "b", "a"};
        set = Arrays.asList(reverseSortedArr);
        assert new Double(udf.evaluate(vec2, set, 5.0, "ndcg") * 100.0).intValue() == 100;
    }

    @Test
    public void testNDCGRankings() throws Exception {
        VectorAggOpUDF udf = new VectorAggOpUDF();

        HashMap<String, Double> vec1 = new HashMap<String, Double>();
        vec1.put("d1", 3.0);
        vec1.put("d2", 2.0);
        vec1.put("d3", 3.0);
        vec1.put("d4", 0.001);
        vec1.put("d5", 1.0);
        vec1.put("d6", 2.0);

        String[] idealOrder = {"d1", "d3", "d2", "d6", "d5", "d0"};
        String[] worstOrder = {"d0", "d5", "d6", "d2", "d3", "d1"};
        String[] randomOrder = {"d1", "d2", "d3", "d4", "d5", "d6"};
        String[] noSimilarElementsInOrder = {"a", "b", "c", "d", "e"};

        assert new Double(udf.evaluate(vec1, Arrays.asList(idealOrder), 5.0, "ndcg") * 100.0).intValue() == 100;
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 94;
        assert new Double(udf.evaluate(vec1, Arrays.asList(worstOrder), 5.0, "ndcg") * 100.0).intValue() == 59;
        assert new Double(udf.evaluate(vec1, Arrays.asList(noSimilarElementsInOrder), 5.0, "ndcg") * 100.0).intValue() == 0;

        vec1.clear();
        vec1.put("a", 1.0);
        vec1.put("b", 2.0);
        vec1.put("c", 3.0);
        vec1.put("d", 4.0);
        vec1.put("e", 5.0);

        idealOrder = new String[]{"e","d","c","b","a"};
        worstOrder = new String[]{"a","b","c","d","e"};

        assert new Double(udf.evaluate(vec1, Arrays.asList(idealOrder), 5.0, "ndcg") * 100.0).intValue() == 100;
        assert new Double(udf.evaluate(vec1, Arrays.asList(worstOrder), 5.0, "ndcg") * 100.0).intValue() == 54;

        randomOrder = new String[]{"c","b","d","e","a"};
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 66;

        randomOrder = new String[]{"a","b","c","e","d"}; // order is slightly better than worse
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 55;

        randomOrder = new String[]{"e","d","c","a","b"}; // order is slightly worse than best
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 99;

        randomOrder = new String[]{"e","a","c","d","b"}; // order is slightly worse than best
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 93;

        vec1.clear();
        //
        // Test same order of expected ordering but with different values for assignedLabels
        vec1.put("a", 10.0);
        vec1.put("b", 20.0);
        vec1.put("c", 30.0);
        vec1.put("d", 40.0);
        vec1.put("e", 50.0);

        assert new Double(udf.evaluate(vec1, Arrays.asList(idealOrder), 5.0, "ndcg") * 100.0).intValue() == 100;
        assert new Double(udf.evaluate(vec1, Arrays.asList(worstOrder), 5.0, "ndcg") * 100.0).intValue() == 38;

        randomOrder = new String[]{"c","b","d","e","a"};
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 43;

        randomOrder = new String[]{"a","b","c","e","d"}; // order is slightly better than worse
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 43;

        randomOrder = new String[]{"e","d","c","a","b"}; // order is slightly worse than best
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 99;

        randomOrder = new String[]{"e","a","c","d","b"}; // order is slightly worse than best
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 99;

        vec1.clear();
        // Test missing elements in ground truth label map
        vec1.clear();
        vec1.put("a", 1.0);
        vec1.put("b", 2.0);
        vec1.put("c", 3.0);
        vec1.put("d", 4.0);
        vec1.put("e", 5.0);

        idealOrder = new String[]{"f","e","d","c","b","a"};
        worstOrder = new String[]{"a","b","c","d","e","f"};

        assert new Double(udf.evaluate(vec1, Arrays.asList(idealOrder), 5.0, "ndcg") * 100.0).intValue() == 69;
        assert new Double(udf.evaluate(vec1, Arrays.asList(worstOrder), 5.0, "ndcg") * 100.0).intValue() == 54;

        randomOrder = new String[]{"c","f","b","d","e","a"};
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 59;

        randomOrder = new String[]{"a","b","c","e","d","f"};
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 55;

        randomOrder = new String[]{"e","f","d","c","a","b"};
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 94;

        randomOrder = new String[]{"e","a","f","c","d","b"};
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 90;

        // Test missing labels in assigned label
        vec1.clear();
        vec1.put("a", 1.0);
        vec1.put("b", 2.0);
        vec1.put("c", 3.0);
        vec1.put("d", 4.0);
        vec1.put("e", 5.0);
        vec1.put("f", 6.0);

        idealOrder = new String[]{"e","d","c","b","a"};
        worstOrder = new String[]{"a","b","c","d","e"};

        assert new Double(udf.evaluate(vec1, Arrays.asList(idealOrder), 5.0, "ndcg") * 100.0).intValue() == 48;
        assert new Double(udf.evaluate(vec1, Arrays.asList(worstOrder), 5.0, "ndcg") * 100.0).intValue() == 25;

        randomOrder = new String[]{"c","b","d","e","a"};
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 31;

        randomOrder = new String[]{"a","b","c","e","d"};
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 26;

        randomOrder = new String[]{"e","d","c","a","b"};
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 47;

        randomOrder = new String[]{"e","a","c","d","b"};
        assert new Double(udf.evaluate(vec1, Arrays.asList(randomOrder), 5.0, "ndcg") * 100.0).intValue() == 44;
    }

    @Test
    public void testKloutRankings() throws Exception {
        VectorAggOpUDF udf = new VectorAggOpUDF();

        HashMap<String, Double> vec= new HashMap<String, Double>();
        vec.put("Roger Federer", 9.0);
        vec.put("Novak Djokovic", 10.0);
        vec.put("Andy Murray", 8.0);
        vec.put("Stan Wawrinka", 7.0);
        vec.put("Kei Nishikori", 6.0);
        vec.put("Rafael Nadal", 1.0);
        vec.put("Milos Raonic", 3.0);
        vec.put("Tomas Berdych", 5.0);
        vec.put("David Ferrer", 4.0);
        vec.put("Marin Cilic", 2.0);

        String[] arr = {"Roger Federer","Novak Djokovic","Andy Murray","Stan Wawrinka","Kei Nishikori","Rafael Nadal","Milos Raonic","Tomas Berdych","David Ferrer","Marin Cilic"};
        List<String> set = Arrays.asList(arr);
        assert new Double(udf.evaluate(vec, set, 10.0, "ndcg") * 100.0).intValue() == 87;

        vec.clear();
        vec.put("Hillary Clinton", 10.0);
        vec.put("Oprah Winfrey", 1.0);
        vec.put("Michelle Obama", 3.0);
        vec.put("Dilma Rousseff", 6.0);
        vec.put("Christine Lagarde", 7.0);
        vec.put("Melinda Gates", 9.0);
        vec.put("Sheryl Sandberg", 5.0);
        vec.put("Park Geun-hye", 2.0);
        vec.put("Susan Wojcicki", 4.0);
        vec.put("Mary Barra", 8.0);

        String[] arr1 = {"Hillary Clinton","Oprah Winfrey","Michelle Obama","Dilma Rousseff","Christine Lagarde","Melinda Gates","Sheryl Sandberg","Park Geun-hye","Susan Wojcicki","Mary Barra"};
        set = Arrays.asList(arr1);
        assert new Double(udf.evaluate(vec, set, 10.0, "ndcg") * 100.0).intValue() == 87;

    }

    @Test
    public void covarianceTest() throws Exception {
        VectorAggOpUDF udf = new VectorAggOpUDF();

        // Test1: symmetric input
        Map vec1 = new LinkedHashMap<String, Double>();
        vec1.put("0", 3.0);
        vec1.put("1", 1.0);
        vec1.put("2", 3.0);
        vec1.put("3", 9.0);

        Map vec2 = new LinkedHashMap<String, Double>();
        vec2.put("0", 4.0);
        vec2.put("1", 4.0);
        vec2.put("2", 8.0);
        vec2.put("3", 8.0);

        Double cov = udf.evaluate(vec1, vec2, "covariance");
        assert cov == 4.0;

        // Test2: asymmetric input
        vec1.clear();
        vec2.clear();

        vec1.put("a", 3.0);
        vec1.put("b", 1.0);
        vec1.put("2", 3.0);

        vec2.put("2", 4.0);
        vec2.put("1", 4.0);
        vec2.put("c", 8.0);
        vec2.put("a", 8.0);
        cov = udf.evaluate(vec1, vec2, "covariance");
        assert cov == 0.4800000000000002;

        // Test3: empty input
        vec1.clear();
        vec2.clear();

        vec2.put("2", 4.0);
        vec2.put("1", 4.0);
        vec2.put("c", 8.0);
        vec2.put("a", 8.0);
        cov = udf.evaluate(vec1, vec2, "covariance");
        assert cov == 0.0;

        // Test3: zero input
        vec1.clear();
        vec2.clear();

        vec1.put("a", 0.0);
        vec1.put("b", 0.0);
        vec1.put("2", 0.0);

        vec2.put("2", 0.0);
        vec2.put("1", 0.0);
        vec2.put("c", 0.0);
        vec2.put("a", 0.0);
        cov = udf.evaluate(vec1, vec2, "covariance");
        assert cov == 0.0;

        // Test4: null input
        vec1 = null;
        vec2 = null;
        cov = udf.evaluate(vec1, vec2, "covariance");
        assert cov == null;
    }

    @Test
    public void correlationTest() throws Exception {
        VectorAggOpUDF udf = new VectorAggOpUDF();

        // Test1: symmetric input
        LinkedHashMap vec1 = new LinkedHashMap<String, Double>();
        vec1.put("0", 3.0);
        vec1.put("1", 1.0);
        vec1.put("2", 3.0);
        vec1.put("3", 9.0);

        LinkedHashMap vec2 = new LinkedHashMap<String, Double>();
        vec2.put("0", 4.0);
        vec2.put("1", 4.0);
        vec2.put("2", 8.0);
        vec2.put("3", 8.0);

        Double corr = udf.evaluate(vec1, vec2, "correlation");
        assert corr == 0.6666666666666666;

        // Test2: asymmetric input
        vec1.clear();
        vec2.clear();

        vec1.put("a", 3.0);
        vec1.put("b", 1.0);
        vec1.put("2", 3.0);

        vec2.put("2", 4.0);
        vec2.put("1", 4.0);
        vec2.put("c", 8.0);
        vec2.put("a", 8.0);
        corr = udf.evaluate(vec1, vec2, "correlation");
        assert corr == 0.2545584412271572;

        // Test3: empty input
        vec1.clear();
        vec2.clear();

        vec2.put("2", 4.0);
        vec2.put("1", 4.0);
        vec2.put("c", 8.0);
        vec2.put("a", 8.0);
        corr = udf.evaluate(vec1, vec2, "correlation");
        assert corr == null;

        // Test3: zero input
        vec1.clear();
        vec2.clear();

        vec1.put("a", 0.0);
        vec1.put("b", 0.0);
        vec1.put("2", 0.0);

        vec2.put("2", 0.0);
        vec2.put("1", 0.0);
        vec2.put("c", 0.0);
        vec2.put("a", 0.0);
        corr = udf.evaluate(vec1, vec2, "correlation");
        assert corr == null;

        // Test4: null input
        vec1 = null;
        vec2 = null;
        corr = udf.evaluate(vec1, vec2, "correlation");
        assert corr == null;
    }
}
