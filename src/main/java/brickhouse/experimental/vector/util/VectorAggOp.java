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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * Text vector operations to collapse vector(s) to scalar(s).
 *
 */
public class VectorAggOp {
  private static final Logger LOG = Logger.getLogger(VectorAggOp.class);

  // -------------------------------------------------------------------------------------------------------------------
  // --------------------------------------- VEC -> SCALAR AGG OPERATIONS ----------------------------------------------
  // -------------------------------------------------------------------------------------------------------------------
  public enum UnaryOperator {
    ABS
  }

  public enum BinaryOperator {
    COS,
    CORRELATION,
    COVARIANCE,
    SMALL_LARGE_MULTIPLY
  }

  public enum OrderingOperator {
    PRECISION_AT_K,
    NDCG
  }

  /**
   * Performs vector scalar operation.
   *
   * @param vec
   * @param op
   * @return
   * @throws java.util.Map
   */
  public static Double evaluate(Map<String, Double> vec, String op) throws Exception {
    if (vec == null || op == null) return null;

    if (op.equals(UnaryOperator.ABS.toString().toLowerCase())) {
      return Math.sqrt(evaluate(vec, vec, "*"));
    }

    Double agg = initAgg(op);
    for (Map.Entry<String, Double> entry : vec.entrySet()) {
      if (entry.getValue() == null) continue;
      agg = op(agg, entry.getValue(), op);
    }
    return agg;
  }


  public static Double safeEvaluate(Map<String, Double> vec, String op)  {
    Double output = null;
    try { output = evaluate(vec, op); } catch (Exception e) { }
    return output;
  }


  /**
   * Performs vector scalar operation.
   *
   * @param vec1
   * @param vec2
   * @param op
   * @return
   * @throws java.util.Map
   */
  public static Double evaluate(Map<String, Double> vec1,
                                Map<String, Double> vec2,
                                String op) throws Exception {
    if (op == null) return null;
    if (op.equals(BinaryOperator.COS.toString().toLowerCase())) {
      return evaluate(vec1, vec2, "*") / (evaluate(vec1, UnaryOperator.ABS.toString().toLowerCase()) *
          evaluate(vec2, UnaryOperator.ABS.toString().toLowerCase()));
    } else if (op.equals("*")) {
      return evaluate(VectorOp.evaluate(vec1, vec2, "*"), "+");
    } else if (op.equals(BinaryOperator.CORRELATION.toString().toLowerCase())) {
      return correlation(vec1, vec2);
    } else if (op.equals(BinaryOperator.COVARIANCE.toString().toLowerCase())) {
      return covariance(vec1, vec2);
    } else if (op.equals(BinaryOperator.SMALL_LARGE_MULTIPLY.toString().toLowerCase())) {
      return multiplySmallWithLarge(vec1, vec2);
    }
    throw new Exception("Operator '" + op + "' not supported.");
  }

  public static Double safeEvaluate(Map<String, Double> vec1, Map<String, Double> vec2, String op)  {
    Double output = null;
    try { output = evaluate(vec1, vec2, op); } catch (Exception e) { }
    return output;
  }

  /**
   * Performs some of the metrics evaluation (rank / precision metrics).
   *
   * @param vec
   * @param set
   * @param scalar
   * @param op
   * @return
   * @throws Exception
   */
  public static Double evaluate(Map<String, Double> vec,
                                List<String> set,
                                Double scalar,
                                String op) throws Exception {
    if (op == null || scalar == null) return null;
    if (op.toLowerCase().equals(OrderingOperator.PRECISION_AT_K.toString().toLowerCase())) {
      return precisionAtK(vec, set, scalar.intValue());
    }
    if (op.toLowerCase().equals(OrderingOperator.NDCG.toString().toLowerCase())) {
      return nDCG(vec, set, scalar.intValue());
    }
    throw new Exception("Operator '" + op + "' not supported.");
  }

  public static Double safeEvaluate(Map<String, Double> vec, List<String> set, Double scalar, String op)  {
    Double output = null;
    try { output = evaluate(vec, set, scalar, op); } catch (Exception e) { }
    return output;
  }

  /**
   * Performs some of the metrics evaluation (rank / precision metrics).
   *
   * @param vec
   * @param positiveSet
   * @param negativeSet
   * @param scalar
   * @param op
   * @return
   * @throws Exception
   */
  public static Double evaluate(Map<String, Double> vec,
                                List<String> positiveSet,
                                List<String> negativeSet,
                                Double scalar,
                                String op) throws Exception {
    if (op == null || scalar == null) return null;
    if (op.toLowerCase().equals(OrderingOperator.PRECISION_AT_K.toString().toLowerCase())) {
      return precisionAtK(vec, positiveSet, negativeSet, scalar.intValue());
    }
    throw new Exception("Operator '" + op + "' not supported.");
  }

  public static Double safeEvaluate(Map<String, Double> vec,
                                    List<String> positiveSet,
                                    List<String> negativeSet,
                                    Double scalar,
                                    String op)  {
    Double output = null;
    try { output = evaluate(vec, positiveSet, negativeSet, scalar, op); } catch (Exception e) { }
    return output;
  }

  // -------------------------------------------------------------------------------------------------------------------
  // --------------------------------------- OPERATIONS HELPERS --------------------------------------------------------
  // -------------------------------------------------------------------------------------------------------------------


  private static Double op(Double agg, Double value, String op) throws Exception {
    if (agg == null || value == null) return null;
    if (op.equals("+")) {
      return agg + value;
    } else if (op.equals("*")) {
      return agg * value;
    } else if (op.equals("max")) {
      return Math.max(agg, value);
    } else if (op.equals("min")) {
      return Math.min(agg, value);
    }
    throw new Exception("Operator '" + op + "' not supported.");
  }


  private static Double initAgg(String op) throws Exception {
    if (op.equals("+")) {
      return 0.0;
    } else if (op.equals("*")) {
      return 1.0;
    } else if (op.equals("max")) {
      return Double.MIN_VALUE;
    } else if (op.equals("min")) {
      return Double.MAX_VALUE;
    }

    throw new Exception("Operator '" + op + "' not supported.");
  }

  // -------------------------------------------------------------------------------------------------------------------
  // --------------------------------------- SUPPORTED OPERATIONS ------------------------------------------------------
  // -------------------------------------------------------------------------------------------------------------------


  public static Double precisionAtK(Map<String, Double> assignedLabels, List<String> groundTruthLabels, Integer k) {
    if (groundTruthLabels == null || k == null || k <= 0.0) {
      return null;
    }
    if (assignedLabels == null || assignedLabels.size() == 0) {
      return 0.0;
    }
    Map<String, Double> topK = null;
    try {
      topK = VectorOp.evaluate(assignedLabels, k.intValue(), "top_k");
    } catch (Exception e) {
      LOG.trace(e);
      return null;
    }
    Double sum = countTruePositives(topK, groundTruthLabels);
    return sum / Math.min(topK.size(), assignedLabels.size());
  }


  public static Double precisionAtK(Map<String, Double> assignedLabels,
                                    List<String> groundTruthPositiveLabels,
                                    List<String> groundTruthNegativeLabels,
                                    Integer k) {
    if (k == null || k <= 0.0) {
      return null;
    }
    if (assignedLabels == null || assignedLabels.size() == 0) {
      return 0.0;
    }
    Map<String, Double> topK = null;
    try {
      topK = VectorOp.evaluate(assignedLabels, k.intValue(), "top_k");
    } catch (Exception e) {
      LOG.trace(e);
      return null;
    }
    Double truePositive = countTruePositives(topK, groundTruthPositiveLabels);
    Double falsePositive = countFalsePositives(topK, groundTruthNegativeLabels);
    if ((truePositive + falsePositive) == 0) {
      return null;
    }
    return truePositive / (truePositive + falsePositive);
  }


  public static Double countTruePositives(Map<String, Double> assignedLabels, List<String> groundTruthTrueLabels) {
    if (groundTruthTrueLabels == null || groundTruthTrueLabels.size() == 0) {
      return 0.0;
    }
    Set<String> positiveLabelSet = new HashSet<String>(groundTruthTrueLabels);
    Double truePositive = 0.0;
    for (Map.Entry<String, Double> guessedEntry : assignedLabels.entrySet()) {
      if (positiveLabelSet.contains(guessedEntry.getKey())) {
        truePositive += 1.0;
      }
    }
    return truePositive;
  }


  public static Double countFalsePositives(Map<String, Double> assignedLabels, List<String> groundTruthFalseLabels) {
    if (groundTruthFalseLabels == null || groundTruthFalseLabels.size() == 0) {
      return 0.0;
    }
    Set<String> negativeLabelSet = new HashSet<String>(groundTruthFalseLabels);
    Double sum = 0.0;
    for (Map.Entry<String, Double> guessedEntry : assignedLabels.entrySet()) {
      if (negativeLabelSet.contains(guessedEntry.getKey())) {
        sum += 1.0;
      }
    }
    return sum;
  }

  public static Double nDCG(Map<String, Double> groundTruthLabels, List<String> assignedLabels, Integer k) {
    if (groundTruthLabels == null || groundTruthLabels.size() == 0) {
      return 0.0;
    }

//    final Double log2 = Math.log(2);
//    final Double MISSING_SCORE = 0.0; // https://en.wikipedia.org/wiki/Discounted_cumulative_gain#Limitations  -- Point 2
//    Double dcg = 0.0;
//    Double idcg = 0.0;
//
//    Map<String, Double> sortedAssignedLabelsMap = null;
//    try {
//      sortedAssignedLabelsMap =  VectorOp.evaluate(VectorOp.evaluate(assignedLabels, k.intValue(), "top_k"), "sort");
//      System.out.println(sortedAssignedLabelsMap.toString());
//    } catch (Exception e) {
//      LOG.exception(e);
//      return null;
//    }
//
//    Integer position = 1;
//    for (String entry: groundTruthLabels) {
//      System.out.println("checking idcg: " + entry);
//      Double relI = sortedAssignedLabelsMap.get(entry);
//      if (relI == null) {
//        relI = MISSING_SCORE;
//      }
//      idcg += (Math.pow(2.0, relI) - 1)/ (Math.log(position + 1) / log2);
//      System.out.println("idcg: \t" + idcg);
//      ++position;
//    }
//
//    position = 1;
//    for (Map.Entry<String, Double> entry: sortedAssignedLabelsMap.entrySet()) {
//      System.out.println("checking dcg: " + entry);
//      Double relI = entry.getValue();
//      dcg += (Math.pow(2.0, relI) - 1)/ (Math.log(position + 1) / log2);
//      System.out.println("dcg: \t" + dcg);
//      ++position;
//    }
//
//    return dcg / idcg;

    final Double log2 = Math.log(2);
    final Double MISSING_SCORE = 0.0; // https://en.wikipedia.org/wiki/Discounted_cumulative_gain#Limitations  -- Point 2
    Double dcg = 0.0;
    Double idcg = 0.0;
    Integer position = 1;

    Map<String, Double> sortedAssignedLabelsMap = null;
    try {
      sortedAssignedLabelsMap = VectorOp.evaluate(VectorOp.evaluate(groundTruthLabels, k.intValue(), "top_k"), "sort");
    } catch (Exception e) {
      LOG.trace(e);
      return null;
    }

    for (String entry : assignedLabels) {
      Double relI = sortedAssignedLabelsMap.get(entry);
      if (relI == null) {
        relI = MISSING_SCORE;
      }
      dcg += (Math.pow(2.0, relI) - 1) / (Math.log(position + 1) / log2);
      ++position;
    }

    position = 1;
    for (Map.Entry<String, Double> entry : sortedAssignedLabelsMap.entrySet()) {
      idcg += (Math.pow(2.0, entry.getValue()) - 1) / (Math.log(position + 1) / log2);
      ++position;
    }

    return dcg / idcg;
  }

  /**
   * correlation: normalized covariance
   * Note that expectation of this function is that full vectors are passed, and not sparse vectors.
   *
   * @param vec1
   * @param vec2
   * @return
   */
  public static Double correlation(Map<String, Double> vec1,
                                   Map<String, Double> vec2) throws Exception {
    if (vec1 == null || vec2 == null) return null;

    Double cov = covariance(vec1, vec2);
    Double cov1 = covariance(vec1, vec1);
    Double cov2 = covariance(vec2, vec2);

    if (cov == null || cov1 == null || cov2 == null || cov1 == 0.0 || cov2 == 0.0) return null;

    return cov / (Math.sqrt(cov1) * Math.sqrt(cov2));
  }

  /**
   * covariance : to measure the similarity of two variables,
   * Note that expectation of this function is that full vectors are passed, and not sparse vectors.
   *
   * @param vec1
   * @param vec2
   * @return
   */
  public static Double covariance(Map<String, Double> vec1,
                                  Map<String, Double> vec2) throws Exception {
    if (vec1 == null || vec2 == null) return null;
    if (vec1.isEmpty() || vec2.isEmpty()) return 0.0;

    Set<String> keys = new HashSet<String>();
    keys.addAll(vec1.keySet());
    keys.addAll(vec2.keySet());

    if (keys.isEmpty()) return null;

    double mean1 = evaluate(vec1, "+") / keys.size();
    double mean2 = evaluate(vec2, "+") / keys.size();

    double cov = 0.0;

    for (String key : keys) {
      double a = 0;
      double b = 0;
      Double val1 = vec1.get(key);
      Double val2 = vec2.get(key);
      a = (val1 == null) ? (0.0 - mean1) : (val1 - mean1);
      b = (val2 == null) ? (0.0 - mean2) : (val2 - mean2);
      cov += (a * b) / keys.size();
    }

    return cov;
  }

  public static Double multiplySmallWithLarge(Map<String, Double> smallerMap,
                                              Map<String, Double> largerMap) {
    if (smallerMap == null || largerMap == null) return 0.0;
    Double sum = 0.0;
    for(Map.Entry<String, Double> entry: smallerMap.entrySet()) {
      Double otherVal = largerMap.get(entry.getKey());
      if (otherVal != null) {
        sum += entry.getValue() * otherVal;
      }
    }
    return sum;
  }
}
