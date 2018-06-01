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

import java.util.Comparator;
import java.util.Map;


/**
 * Simple value map comparator.
 */
public class DoubleMapValueComparator implements Comparator<String> {

    Map<String, Double> base;
    public DoubleMapValueComparator(Map<String, Double> base) {
        this.base = base;
    }

    public int compare(String a, String b) {
        // Same key must be equal since it collapses and should be one value anyway.
        if (a == null || b == null) {
            if (a == null && b == null) return 0;
            return (a == null) ? -1 : 1;
        }
        if (a.equals(b)) {
            return 0;
        }
        int cmp = -1 * base.get(a).compareTo(base.get(b));
        if (cmp == 0) return a.compareTo(b);
        return cmp;
    }
}