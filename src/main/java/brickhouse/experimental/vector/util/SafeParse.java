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


/**
 *
 * Utility to parse numeric types without throwing exceptions or logging on the failiure.
 *
 */
public class SafeParse {
    static public Double parseDouble(final String numberStr, final Double alternative) {
        if (numberStr == null) return alternative;
        try {
            return Double.parseDouble(numberStr);
        } catch (Exception e) {
        }
        return alternative;
    }

    static public Float parseFloat(final String numberStr, final Float alternative) {
        if (numberStr == null) return alternative;
        try {
            return Float.parseFloat(numberStr);
        } catch (Exception e) {
        }
        return alternative;
    }

    static public Integer parseInteger(final String numberStr, final Integer alternative) {
        if (numberStr == null) return alternative;
        try {
            Integer result = Integer.valueOf(numberStr);
            return result;
        } catch (Exception e) {
        }
        Double result = SafeParse.parseDouble(numberStr, null);
        if (result == null || result > Integer.MAX_VALUE || result < Integer.MIN_VALUE) {
            return alternative;
        }
        return result.intValue();
    }

    static public Long parseLong(final String numberStr, final Long alternative) {
        if (numberStr == null) return alternative;
        try {
            Long result = Long.valueOf(numberStr);
            return result;
        } catch (Exception e) {
        }
        Double result = SafeParse.parseDouble(numberStr, null);
        if (result == null || result > Long.MAX_VALUE || result < Long.MIN_VALUE) {
            return alternative;
        }
        return result.longValue();
    }

    static public Short parseShort(final String numberStr, final Short alternative) {
        if (numberStr == null) return alternative;
        try {
            Short result = Short.valueOf(numberStr);
            return result;
        } catch (Exception e) {
        }
        Double result = SafeParse.parseDouble(numberStr, null);
        if (result == null || result > Short.MAX_VALUE || result < Short.MIN_VALUE) {
            return alternative;
        }
        return result.shortValue();
    }

    static public Boolean parseBoolean(final String booleanStr, final Boolean alternative) {
        if (booleanStr == null) return alternative;
        try {
            Boolean result = Boolean.valueOf(booleanStr);
            return result;
        } catch (Exception e) {
        }
        return alternative;
    }
}