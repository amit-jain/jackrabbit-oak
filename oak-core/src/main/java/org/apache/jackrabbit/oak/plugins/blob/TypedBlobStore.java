/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;

/**
 * Interface to provide ability to write blob to the {@link GarbageCollectableBlobStore} with
 * {@link BlobOptions}.
 */
public interface TypedBlobStore extends GarbageCollectableBlobStore {
    /**
     * Write a blob with specified options.
     *
     * @param in the input stream to write
     * @param options the options to use
     * @return
     * @throws IOException
     */
    String writeBlob(InputStream in, BlobOptions options) throws IOException;
}
