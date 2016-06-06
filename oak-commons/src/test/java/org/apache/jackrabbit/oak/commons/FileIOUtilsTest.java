/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.commons;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.symmetricDifference;
import static com.google.common.collect.Sets.union;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.readStringsAsSet;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link FileIOUtils}
 */
public class FileIOUtilsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("./target"));

    @Test
    public void writeReadStringsTest() throws IOException {
        Set<String> added = newHashSet("a", "z", "e", "b");
        File f = folder.newFile();
        writeStrings(added.iterator(), f);
        Set<String> retrieved = readStringsAsSet(new FileInputStream(f));
        Assert.assertTrue(symmetricDifference(added, retrieved).isEmpty());
    }

    @Test
    public void sortTest() throws IOException {
        List<String> list = newArrayList("a", "z", "e", "b");
        File f = folder.newFile();
        writeStrings(list.iterator(), f);

        FileIOUtils.sort(f);
        BufferedReader reader = new BufferedReader(new FileReader(f));
        String line = null;
        List<String> retrieved = newArrayList();
        while ((line = reader.readLine()) != null) {
            retrieved.add(line);
        }
        Collections.sort(list);
        assertArrayEquals( Arrays.toString(list.toArray()),
            list.toArray(), retrieved.toArray());
    }

    @Test
    public void appendTest() throws IOException {
        Set<String> added1 = newHashSet("a", "z", "e", "b");
        File f1 = folder.newFile();
        writeStrings(added1.iterator(), f1);

        Set<String> added2 = newHashSet("2", "3", "5", "6");
        File f2 = folder.newFile();
        writeStrings(added2.iterator(), f2);

        Set<String> added3 = newHashSet("t", "y", "8", "9");
        File f3 = folder.newFile();
        writeStrings(added3.iterator(), f3);

        FileIOUtils.append(newArrayList(f2, f3), f1, true);
        Assert.assertTrue(symmetricDifference(union(union(added1, added2), added3),
            readStringsAsSet(new FileInputStream(f1))).isEmpty());
    }
}
