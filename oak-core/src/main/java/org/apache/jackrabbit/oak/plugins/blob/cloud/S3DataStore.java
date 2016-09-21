/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.blob.cloud;

import java.util.Properties;

import com.google.common.base.Strings;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.S3Backend;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An Amazon S3 data store.
 */
public class S3DataStore extends AbstractSharedCachingDataStore {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(S3DataStore.class);

    protected Properties properties;

    @Override
    protected SharedBackend createBackend() {
        S3Backend backend = new S3Backend();
        if(properties != null){
            backend.setProperties(properties);
        }
        return backend;
    }

    /**
     * Properties required to configure the S3Backend
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    /**
     * Look in the backend for a record matching the given identifier.  Returns true
     * if such a record exists.
     *
     * @param identifier - An identifier for the record.
     * @return true if a record for the provided identifier can be found.
     */
    public boolean haveRecordForIdentifier(final String identifier) {
        try {
            if (!Strings.isNullOrEmpty(identifier)) {
                return backend.exists(new DataIdentifier(identifier));
            }
        }
        catch (DataStoreException e) {
            LOG.warn(String.format("Data Store Exception caught checking for %s in pending uploads",
                identifier), e);
        }
        return false;
    }
}
