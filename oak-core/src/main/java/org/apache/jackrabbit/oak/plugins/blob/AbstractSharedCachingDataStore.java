package org.apache.jackrabbit.oak.plugins.blob;

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
import java.io.File;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

/**
 * {@link org.apache.jackrabbit.oak.plugins.blob.SharedDataStore} implementation.
 */
public abstract class AbstractSharedCachingDataStore extends AbstractCachingDataStore
    implements SharedDataStore {

    @Override
    public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
        backend.addMetadataRecord(stream, name);
    }

    @Override
    public void addMetadataRecord(File f, String name) throws DataStoreException {
        backend.addMetadataRecord(f, name);
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        return backend.getMetadataRecord(name);
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        return backend.getAllMetadataRecords(prefix);
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        return backend.deleteMetadataRecord(name);
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        backend.deleteAllMetadataRecords(prefix);
    }

    @Override
    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        return backend.getAllRecords();
    }

    @Override
    public DataRecord getRecordForId(DataIdentifier identifier) throws DataStoreException {
        return backend.getRecord(identifier);
    }

    @Override
    public Type getType() {
        return Type.SHARED;
    }
}
