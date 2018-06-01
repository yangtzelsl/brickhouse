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

import brickhouse.experimental.vector.util.SafeParse;
import brickhouse.experimental.vector.util.VectorOp;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

@Description(
        name = "vector_kernel_factory",
        value =  " Given configuration returns vector kernel that can be used for convolution on other vector op for example.\n" +
                "map<string, string> _FUNC_(map<string, string> config) \n" +
                "  Config should have following parameters specified kernel, center, step, neighbour_reach, gaussian.sigma some \n" +
                " of which may be optional. \n" +
                "Example of config: \n" +
                "  map('kernel', 'GAUSSIAN', 'center', '0.0', 'step', '1.0', 'neighbour_reach', '2.0', 'gaussian.sigma', '1.0') \n" +
                "Supported kernels and examples for params above: \n" +
                "  IDENTITY - eg. {0.0=1.0} \n" +
                "  GAUSSIAN - eg. {-1.0=0.24203, -2.0=0.05400, -3.0=0.00443, 0.0=0.39905, 1.0=0.24203, 2.0=0.05400, 3.0=0.00443} \n" +
                "  LINEAR - eg. {-1.0=0.2222, -2.0=0.11111, 0.0=0.33333, 1.0=0.22222, 2.0=0.11111} \n" +
                "  NEAREST_NEIGHBOR - eg. {-2.0=0.2, -1.0=0.2, 0.0=0.2, 1.0=0.2, 2.0=0.2} \n"
)
public class VectorKernelFactoryUDF extends UDF {
    private static long MAX_LOG = 1000;
    private static final Logger LOG = Logger.getLogger(VectorKernelFactoryUDF.class);

    private static final Map<String, Map<String, Double>> kernelCache_ = new HashMap<String, Map<String, Double>>();
    private static long MAX_CACHE_SIZE = 1000;

    public static final String KERNEL          = "kernel";
    public static final String CENTER          = "center";
    public static final String STEP            = "step";
    public static final String NEIGHBOUR_REACH = "neighbour_reach";
    public static final String GAUSSIAN_SIGMA  = "gaussian.sigma";
    public static final String[] SUPPORTED_PARAMS = {KERNEL, CENTER, STEP, NEIGHBOUR_REACH, GAUSSIAN_SIGMA};
    public static final Set<String> SUPPORTED_PARAMS_SET = new HashSet<String>(Arrays.asList(SUPPORTED_PARAMS));

    enum Kernel {
        IDENTITY,
        GAUSSIAN,
        NEAREST_NEIGHBOR,
        LINEAR
    }

    public Map<String, Double> evaluate(Map<String, String> config) throws Exception {
        if (kernelCache_.size() > MAX_CACHE_SIZE) {
            kernelCache_.clear();
        }
        String confStr = config.toString();
        if (!kernelCache_.containsKey(confStr)) {
            checkConfig(config);
            kernelCache_.put(confStr, generateKernel(config));
        }
        return kernelCache_.get(confStr);
    }

    private Map<String, Double> generateKernel(Map<String, String> config) throws Exception {
        String kernelStr = config.get(KERNEL);
        if (kernelStr == null) {
            return null;
        }
        Kernel kernel = Kernel.valueOf(kernelStr);

        Double center = SafeParse.parseDouble(config.get(CENTER), null);
        Double step = SafeParse.parseDouble(config.get(STEP), null);
        Double sigma = SafeParse.parseDouble(config.get(GAUSSIAN_SIGMA), null);
        Integer neighbourReach = SafeParse.parseInteger(config.get(NEIGHBOUR_REACH), null);
        switch (kernel) {
            case IDENTITY:
                return identity(center);
            case GAUSSIAN:
                return gaussian(center, step, sigma, neighbourReach);
            case NEAREST_NEIGHBOR:
                return nearestNeighbour(center, step, neighbourReach);
            case LINEAR:
                return linear(center, step, neighbourReach);
            default:
                return null;
        }
    }

    private static Map<String, Double> identity(Double center) {
        if (center == null) return null;
        LinkedHashMap<String, Double> kernel = new LinkedHashMap<String, Double>();
        kernel.put(center.toString(), 1.0);
        return kernel;
    }

    private static Map<String, Double> gaussian(Double center, Double step, Double sigma, Integer neighbourReach) {
        if (center == null || step == null || sigma == null || neighbourReach == null) return null;
        LinkedHashMap<String, Double> kernel = new LinkedHashMap<String, Double>();
        for (Integer i = - neighbourReach; i <= neighbourReach; ++i) {
            Double x = center + step * i;
            Double value = Math.exp( - (x - center) * (x - center) / (2 * sigma * sigma));
            kernel.put(x.toString(), value);
        }
        return normalize(kernel);
    }

    private static Map<String, Double> nearestNeighbour(Double center, Double step, Integer neighbourReach) {
        if (center == null || step == null || neighbourReach == null) return null;
        Double value = 1.0 / (1.0 + neighbourReach * 2);
        LinkedHashMap<String, Double> kernel = new LinkedHashMap<String, Double>();
        for (Integer i = - neighbourReach; i <= neighbourReach; ++i) {
            Double x = center + step * i;
            kernel.put(x.toString(), value);
        }
        return kernel;
    }

    private static Map<String, Double> linear(Double center, Double step, Integer neighbourReach) {
        if (center == null || step == null || neighbourReach == null) return null;
        LinkedHashMap<String, Double> kernel = new LinkedHashMap<String, Double>();
        for (Integer i = - neighbourReach - 1; i <= neighbourReach + 1; ++i) {
            Double x = center + step * i;
            kernel.put(x.toString(), (neighbourReach + 1 - Math.abs(i) * step));
        }
        return normalize(kernel);
    }

    private static Map<String, Double> normalize(Map<String, Double> kernel) {
        return VectorOp.safeEvaluate(kernel, "normalize");
    }

    private static void checkConfig(Map<String, String> conf) throws IOException {
        for (Map.Entry<String, String> entry : conf.entrySet()) {
            if (!SUPPORTED_PARAMS_SET.contains(entry.getKey())) {
                throw new IOException("Unsupported key : '" + entry.getKey() + "' found in config = " +
                                              conf.toString()  + " " +  " key is expected to be within " +
                                              Arrays.toString(SUPPORTED_PARAMS));
            }
        }
    }

}
