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

import java.io.*;
import java.util.Stack;

/**
 *
 * Line reader reading file or directory. Basically one provides path and then we just read lines.
 *
 */
public class LineReader {
    private static final Logger LOG = Logger.getLogger(LineReader.class);

    private static Double GIGABYTE = 1024.0 * 1024.0 * 1024.0;
    private static int LOG_FREQUENCY = 10000;
    BufferedReader reader_ = null;
    int linesCounted_ = 0;

    Stack<String> pathStack_ = new Stack<String>();

    /**
     *
     * @param path
     * @throws IOException
     */
    public LineReader(String path) throws IOException {
        initPath(path);
    }

    public String readLine() throws IOException {
        if (reader_ == null) {
            if (pathStack_.size() == 0) {
                LOG.debug("Finished reading all the files.");
                return null;
            }
            initNextFile();
        }

        if (reader_ == null) {
            LOG.debug("Finished reading all the files.");
        }

        String line = reader_.readLine();
        if (line != null) {
            if (linesCounted_++ % LOG_FREQUENCY == 0) {
                LOG.debug("Adding [" + linesCounted_ + "]" + line);
                logMemoryUsage();
            }

            return line;
        }

        // Ok end of this file close it, and set reader to null.
        reader_.close();
        reader_ = null;

        // Do next read triggering new file read and return whatever it is.
        return readLine();
    }

    /**
     * Populates stack with files that should be read.
     * @param dictionaryPath
     * @param pathStack
     * @throws IOException
     */
    private static void populatePathStack(String dictionaryPath, Stack pathStack) throws IOException {
        if (isCRC(dictionaryPath)) {
            LOG.debug("Ignoring CRC " + dictionaryPath);
            return;
        }

        File dictFile  = new File(dictionaryPath);
        if (!dictFile.exists())  {
            throw new IOException("File/dir "  + dictionaryPath + " does not exists maybe you forgot to add it to " +
                                          "distributed cache.");
        }
        // If the path is dicrectory we push new paths to the directory.
        if (dictFile.getCanonicalFile().isDirectory() || !dictFile.isFile()) {
            String[] subDictFiles = dictFile.list();
            for (String subFile : subDictFiles) {
                LOG.debug("Looking recursively at " + subFile);
                populatePathStack(dictionaryPath + "/" + subFile, pathStack);
            }
        } else {
            // Not the directory then it's file we want to process.
            LOG.debug("Pushing to path stack " + dictionaryPath);
            pathStack.push(dictionaryPath);
        }
    }

    /**
     * Initializes next file on stack and removes it from queue.
     * @throws IOException
     */
    private void initNextFile() throws IOException {
        if (pathStack_.size() == 0) return;
        String dictPath = pathStack_.pop();
        LOG.debug("Reading: " + dictPath);
        reader_ = new BufferedReader(new InputStreamReader(new FileInputStream(dictPath)));
    }

    /**
     * Constructor helper..
     * @param dictionaryPath
     * @throws IOException
     */
    public void initPath(String dictionaryPath) throws IOException {
        // Look at the path and if directory pop on to the stack.
        populatePathStack(dictionaryPath, pathStack_);
    }

    /**
     * Returns true if it's CRC file.
     * @param dictionaryPath
     * @return
     */
    private static boolean isCRC(String dictionaryPath) {
        return dictionaryPath.endsWith("crc");
    }

    /**
     * Logs memory consumption.
     */
    private void logMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        LOG.debug("Memory Usage: "  + runtime.totalMemory() / GIGABYTE + "G of " +
                          runtime.maxMemory() / GIGABYTE + "G");
    }
}