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

import brickhouse.experimental.vector.util.Numeric;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;


public class VectorKernelFactoryUDFTest {
  private static final Logger LOG = Logger.getLogger(VectorKernelFactoryUDFTest.class);
  private static VectorKernelFactoryUDF udf_ = new VectorKernelFactoryUDF();
  private static Double EPS = 0.01;

  private static final String TESTER_FILE_OUT = "/tmp/convolution_kernel_test.txt";


  Map<String, String> makeTestConfig(VectorKernelFactoryUDF.Kernel kernelType) {
    Map<String, String> config = new LinkedHashMap<String, String>();
    config.put(VectorKernelFactoryUDF.KERNEL, kernelType.name());
    config.put(VectorKernelFactoryUDF.CENTER, "0.0");
    config.put(VectorKernelFactoryUDF.STEP, "1.0");
    config.put(VectorKernelFactoryUDF.NEIGHBOUR_REACH, "3.0");
    config.put(VectorKernelFactoryUDF.GAUSSIAN_SIGMA, "1.0");
    return config;
  }

  @Test
  public void testGaussian() throws Exception {
    Map<String, String> config = makeTestConfig(VectorKernelFactoryUDF.Kernel.GAUSSIAN);
    Map<String, Double> out = udf_.evaluate(config);
    assert Numeric.isNear(out.get("0.0"), 0.399, EPS);
    assert Numeric.isNear(out.get("-1.0"), 0.242, EPS);
    assert Numeric.isNear(out.get("1.0"), 0.242, EPS);
  }


  @Test
  public void testLinear() throws Exception {
    Map<String, String> config = makeTestConfig(VectorKernelFactoryUDF.Kernel.LINEAR);
    Map<String, Double> out = udf_.evaluate(config);
    assert Numeric.isNear(out.get("0.0"), 0.25, EPS);
    assert Numeric.isNear(out.get("-1.0"), 0.1875, EPS);
    assert Numeric.isNear(out.get("2.0"), 0.125, EPS);
    assert Numeric.isNear(out.get("-3.0"), 0.0625, EPS);
  }


  @Test
     public void testNearestNeighbor() throws Exception {
    Map<String, String> config = makeTestConfig(VectorKernelFactoryUDF.Kernel.NEAREST_NEIGHBOR);
    Map<String, Double> out = udf_.evaluate(config);
    assert Numeric.isNear(out.get("0.0"), 0.1428, EPS);
    assert Numeric.isNear(out.get("-1.0"), 0.1428, EPS);
    assert Numeric.isNear(out.get("2.0"), 0.1428, EPS);
  }


  /**
   * This is tester function made to check if the filters work as expected. Primary testing is done based on visual
   * inspection in GnuPlot.
   * @throws Exception
   */
  @Test
  public void convolveTest() throws Exception {
//    if (!EXTERNAL_SERVICE_DEPENDENCY_ON) return;
    VectorOpUDF udf = new VectorOpUDF();
    Map<String, Double> val = null;

    // Sigma function is best for unit test !!! as it's intuitive for us humans.
    LinkedHashMap<String, Double> vec1 = new LinkedHashMap<String, Double>();
    vec1.put("0.0", 1.0);
    vec1.put("1.0", 0.0);
    vec1.put("2.0", 0.0);
    vec1.put("3.0", 0.0);
    vec1.put("5.0", 0.0);
    vec1.put("4.0", 0.0);
    vec1.put("6.0", 0.0);
    vec1.put("7.0", 0.0);
    vec1.put("8.0", 0.0);
    vec1.put("9.0", 0.0);

    LinkedHashMap<String, String> lines = new LinkedHashMap<String, String>();
    VectorKernelFactoryUDF.Kernel[] kernels = {
        VectorKernelFactoryUDF.Kernel.IDENTITY,
        VectorKernelFactoryUDF.Kernel.GAUSSIAN,
        VectorKernelFactoryUDF.Kernel.LINEAR,
        VectorKernelFactoryUDF.Kernel.NEAREST_NEIGHBOR,
    };
    StringBuffer plotCommand = new StringBuffer();


    for (int i = 0; i < kernels.length; ++i) {
      VectorKernelFactoryUDF.Kernel kernel = kernels[i];
      plotCommand.append(plotCommand.length() == 0 ? "plot " : ", ");
      plotCommand.append(" '" + TESTER_FILE_OUT + "' using 1:" + (i + 2) + " title '"+ kernel.name() + "' w l ");

      Map<String, String> config = makeTestConfig(kernel);

      Map<String, Double> filterFunction = udf_.evaluate(config);
      val = udf.evaluate(vec1, filterFunction, "convolution_circular");
      for (Map.Entry<String, Double> entry : val.entrySet()) {
        String oldLine = lines.get(entry.getKey());
        if (oldLine != null) {
          lines.put(entry.getKey(), lines.get(entry.getKey()) + " " + entry.getValue());
        } else {
          lines.put(entry.getKey(), entry.getKey() + " " + entry.getValue());
        }
      }
    }

    StringBuffer buff = new StringBuffer();
    for (Map.Entry<String, String> entry : lines.entrySet()) {
      buff.append(entry.getValue() + "\n");
    }

    PrintWriter out = new PrintWriter(TESTER_FILE_OUT);
    out.println(buff.toString());
    out.flush();
    out.close();

    LOG.info("GnuPlot command > \n" + plotCommand.toString());

  }
}
