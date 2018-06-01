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

import org.junit.Test;

public class SafeParseTest {

    @Test
    public void testDouble() throws Exception {
        assert SafeParse.parseDouble(null, null)   == null;
        assert SafeParse.parseDouble(null, 0.0)    == 0.0;
        assert SafeParse.parseDouble("zzz", 0.0)   == 0.0;
        assert SafeParse.parseDouble("12.34", 0.0) == 12.34;
    }

    @Test
    public void testInteger() throws Exception {
        assert SafeParse.parseInteger(null, null) == null;
        assert SafeParse.parseInteger(null, 0)    == 0;
        assert SafeParse.parseInteger("zzz", 0)   == 0.0;
        assert SafeParse.parseInteger("1234", 0)  == 1234;
    }

    @Test
    public void testLong() throws Exception {
        assert SafeParse.parseLong(null, null) == null;
        assert SafeParse.parseLong(null, 0l)    == 0;
        assert SafeParse.parseLong("zzz", 0l)   == 0.0;
        assert SafeParse.parseLong("1234", 0l)  == 1234l;
    }
}
