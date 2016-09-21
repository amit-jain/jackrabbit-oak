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
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import javax.jcr.RepositoryException;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheLoader;
import com.google.common.io.Closeables;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.AbstractDataRecord;
import org.apache.jackrabbit.core.data.AbstractDataStore;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.util.TransientFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 */
public abstract class AbstractCachingDataStore extends AbstractDataStore implements MultiDataStoreAware {
    /**
     * Logger instance.
     */
    static final Logger LOG = LoggerFactory.getLogger(org.apache.jackrabbit.core.data.LocalCache.class);

    /**
     * The digest algorithm used to uniquely identify records.
     */
    private static final String DIGEST = "SHA-1";

    /**
     * The root path
     */
    private String path;

    /**
     * The minimum size of an object that should be stored in this data store.
     */
    private int minRecordLength = 16 * 1024;

    private String secret;

    /**
     * The number of bytes in the cache. The default value is 64 GB.
     */
    private long cacheSize = 64L * 1024 * 1024 * 1024;

    /**
     * The % of cache utilized for upload staging.
     */
    private int stagingSplitPercentage = 10;

    /**
     * The number of upload threads used for asynchronous uploads from staging.
     */
    private int uploadThreads = 10;

    /**
     * The root rootDirectory where the files are created.
     */
    private File rootDirectory;

    /**
     * The rootDirectory where tmp files are created.
     */
    private File tmp;

    /**
     * Statistics provider.
     */
    private StatisticsProvider statisticsProvider;

    /**
     * DataStore cache
     */
    private CompositeDataStoreCache cache;

    /**
     * The delegate backend
     */
    protected SharedBackend backend;

    public void init(String homeDir) {
        if (path == null) {
            path = homeDir + "/repository/datastore";
        }

        checkArgument(stagingSplitPercentage >= 0 && stagingSplitPercentage <= 50,
            "Staging percentage cache should be between 0 and 50");

        this.rootDirectory = new File(path);
        this.tmp = new File(rootDirectory, "tmp");
        this.backend = createBackend();
        this.cache = new CompositeDataStoreCache(path, cacheSize, stagingSplitPercentage, uploadThreads,
            new CacheLoader<String, InputStream>() {
                @Override public InputStream load(String key) throws Exception {
                    InputStream is = null;
                    boolean threw = true;
                    try {
                        is = backend.read(new DataIdentifier(key));
                        threw = false;
                    } finally {
                        Closeables.close(is, threw);
                    }
                    return is;
                }
            }, new StagingUploader() {
                @Override
                public void write(String id, File file) throws DataStoreException {
                    backend.write(new DataIdentifier(id), file);
                }
            }, statisticsProvider, null /*TODO*/, null, 0);
    }

    protected abstract SharedBackend createBackend();

    @Override
    public DataRecord getRecordIfStored(DataIdentifier dataIdentifier)
        throws DataStoreException {
        // Return file attributes from cache only if corresponding file is cached
        // This avoids downloading the file for just accessing the meta data
        File cached = cache.getIfPresent(dataIdentifier.toString());
        if (cached != null && cached.exists()) {
            return new FileCacheDataRecord(this, dataIdentifier, cached.length(),
                cached.lastModified());
        }

        // File not in cache so, retrieve the meta data from the backend explicitly
        DataRecord record;
        try {
            record = backend.getRecord(dataIdentifier);
        } catch (Exception e) {
            LOG.error("Error retrieving record [{}] from backend", dataIdentifier, e);
            throw new DataStoreException(e);
        }
        return new FileCacheDataRecord(this, dataIdentifier, record.getLength(),
            record.getLastModified());
    }

    @Override
    public DataRecord addRecord(InputStream inputStream) throws DataStoreException {
        Stopwatch watch = Stopwatch.createStarted();
        try {
            TransientFileFactory fileFactory = TransientFileFactory.getInstance();
            File tmpFile = fileFactory.createTransientFile("upload", null, tmp);

            // Copy the stream to the temporary file and calculate the
            // stream length and the message digest of the stream
            MessageDigest digest = MessageDigest.getInstance(DIGEST);
            OutputStream output = new DigestOutputStream(new FileOutputStream(tmpFile), digest);
            long length = 0;
            try {
                length = IOUtils.copyLarge(inputStream, output);
            } finally {
                output.close();
            }

            DataIdentifier identifier = new DataIdentifier(encodeHexString(digest.digest()));
            LOG.debug("SHA1 of [{}], length =[{}] took [{}]ms ",
                new Object[] {identifier, length, watch.elapsed(TimeUnit.MILLISECONDS)});

            // asynchronously stage for upload if the size limit of staging cache permits
            if (!cache.stage(identifier.toString(), tmpFile)) {
                backend.write(identifier, tmpFile);
                // Update the last modified for the file if present in the download cache
                File cachedFile = cache.getIfPresent(identifier.toString());
                if (cachedFile != null) {
                    FileUtils.touch(cachedFile);
                }
            }

            return new FileCacheDataRecord(this, identifier, tmpFile.length(), tmpFile.lastModified());
        } catch (Exception e) {
            LOG.error("Error in adding record");
            throw new DataStoreException("Error in adding record ", e);
        }
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return backend.getAllIdentifiers();
    }

    @Override
    public void deleteRecord(DataIdentifier dataIdentifier) throws DataStoreException {
        cache.invalidate(dataIdentifier.toString());
        backend.deleteRecord(dataIdentifier);
    }

    @Override
    public void close() throws DataStoreException {
        backend.close();
        cache.close();
    }

    @Override
    protected byte[] getOrCreateReferenceKey() throws DataStoreException {
        try {
            return secret.getBytes("UTF-8");
        } catch (Exception e) {
            LOG.info("Error in creating reference key", e);
            throw new DataStoreException(e);
        }
    }

    public void setStatisticsProvider(StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
    }

    @Override
    public int getMinRecordLength() {
        return minRecordLength;
    }

    /**
     * Setter for configuration based secret
     *
     * @param secret
     *            the secret used to sign reference binaries
     */
    public void setSecret(String secret) {
        this.secret = secret;
    }

    /**
     * Need a DataRecord implementation that
     * * decorates the data record of the backened if available
     * * creates a record from the paramaters of the file in cache
     *
     */
    static class FileCacheDataRecord extends AbstractDataRecord {
        private long length;
        private long lastModified;
        private AbstractCachingDataStore store;

        public FileCacheDataRecord(AbstractCachingDataStore store, DataIdentifier identifier, long length,
            long lastModified) {
            super(store, identifier);
            this.length = length;
            this.lastModified = lastModified;
            this.store = store;
        }

        @Override
        public long getLength() throws DataStoreException {
            return length;
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            try {
                return new FileInputStream(store.cache.get(getIdentifier().toString()));
            } catch (final Exception e) {
                throw new DataStoreException(
                    "Error opening input stream for identifier " + getIdentifier(), e);
            }
        }

        @Override
        public long getLastModified() {
            return lastModified;
        }
    }


    /**-------------------------unimplemented methods-------------------------------*/

    //Not used anymore
    @Override
    public void clearInUse() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    //Not used anymore
    @Override
    public void updateModifiedDateOnAccess(long l) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    // Not used anymore
    @Override
    public int deleteAllOlderThan(long l) throws DataStoreException {
        throw new UnsupportedOperationException("Operation not supported");
    }
}
