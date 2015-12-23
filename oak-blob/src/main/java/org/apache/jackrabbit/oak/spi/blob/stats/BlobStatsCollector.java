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

package org.apache.jackrabbit.oak.spi.blob.stats;

import java.util.concurrent.TimeUnit;

import aQute.bnd.annotation.ConsumerType;

/**
 * BlobStoreStatsCollector receives callback when blobs are written and read
 * from BlobStore
 */
@ConsumerType
public interface BlobStatsCollector {
    BlobStatsCollector NOOP = new BlobStatsCollector() {
        @Override
        public void uploaded(long timeTaken, TimeUnit unit, long size) {

        }

        @Override
        public void downloaded(String blobId, long timeTaken, TimeUnit unit, long size) {

        }
    };

    /**
     * Called when a binary content is written to BlobStore
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     * @param size size of binary content being written
     */
    void uploaded(long timeTaken, TimeUnit unit, long size);

    /**
     * Called when a binary content is read from BlobStore
     *
     * @param blobId id of blob whose content are being read
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     * @param size size of binary content being read
     */
    void downloaded(String blobId, long timeTaken, TimeUnit unit, long size);
}
