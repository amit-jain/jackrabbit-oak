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

package org.apache.jackrabbit.oak.blob.cloud;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.util.NamedThreadFactory;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.currentThread;
import static org.apache.jackrabbit.oak.blob.cloud.CloudUtils.configuredLocation;
import static org.apache.jackrabbit.oak.blob.cloud.CloudUtils.getBlobStore;
import static org.jclouds.Constants.PROPERTY_CREDENTIAL;
import static org.jclouds.Constants.PROPERTY_IDENTITY;
import static org.jclouds.blobstore.options.CopyOptions.builder;
import static org.jclouds.blobstore.options.ListContainerOptions.NONE;
import static org.jclouds.blobstore.options.PutOptions.Builder.multipart;

/**
 * A data store backend that stores data on a cloud blob store.
 */
public class CloudBackend implements SharedBackend {

    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(CloudBackend.class);

    public static final String DASH = "-";

    private static final String KEY_PREFIX = "dataStore_";

    private static final String META_KEY = "META";

    private static final String META_KEY_PREFIX = "META/";

    private String bucket;

    private Properties properties;

    private Date startTime;

    private BlobStore blobStore;

    public void init() throws DataStoreException {
        LOG.debug("init");

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            startTime = new Date();
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            this.blobStore = getBlobStore(properties);

            if (bucket == null || "".equals(bucket.trim())) {
                bucket = properties.getProperty("bucket");
            }

            if (!blobStore.containerExists(bucket)) {
                blobStore.createContainerInLocation(configuredLocation(properties), bucket);
                LOG.info("Created bucket [{}] in [{}] ", bucket);
            } else {
                LOG.info("Using bucket [{}] in [{}] ", bucket);
            }

            String renameKeyProp = properties.getProperty("renameKeys");
            boolean renameKeyBool = (renameKeyProp == null || "".equals(renameKeyProp)) ?
                false :
                Boolean.parseBoolean(renameKeyProp);
            LOG.info("Rename keys [{}]", renameKeyBool);
            if (renameKeyBool) {
                renameKeys();
            }
            LOG.debug("Cloud backend initialized in [{}] ms",
                (System.currentTimeMillis() - startTime.getTime()));
        } catch (Exception e) {
            LOG.debug("  error ", e);
            Map<String, String> filteredMap = Maps.newHashMap();
            if (properties != null) {
                filteredMap =
                    Maps.filterKeys(Maps.fromProperties(properties), new Predicate<String>() {
                        @Override public boolean apply(String input) {
                            return !input.equals(PROPERTY_IDENTITY) && !input
                                .equals(PROPERTY_CREDENTIAL);
                        }
                    });
            }
            throw new DataStoreException("Could not initialize cloud blobstore from " + filteredMap,
                e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    /**
     * It uploads file to the cloud blob store. If file size is greater than 5MB, this
     * method uses parallel concurrent connections to upload.
     */
    @Override
    public void write(DataIdentifier identifier, File file) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);

        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            try {
                BlobMetadata metadata = blobStore.blobMetadata(bucket, key);
                // check if the same record already exists
                if (metadata != null) {
                    long l = metadata.getSize();
                    if (l != file.length()) {
                        throw new DataStoreException(
                            "Collision: " + key + " new length: " + file.length() + " old length: "
                                + l);
                    }

                    LOG.debug("[{}]'s exists, lastmodified = [{}]", key,
                        metadata.getLastModified().getTime());

                    String eTag = blobStore.copyBlob(bucket, key, bucket, key,
                        builder().contentMetadata(metadata.getContentMetadata()).build());
                    LOG.debug("Blob [{}] already exists, refreshed with [{}]", key, eTag);
                } else {
                    Blob blob = blobStore.blobBuilder(key).payload(file).build();
                    String eTag = blobStore.putBlob(bucket, blob, multipart());
                    LOG.debug("Blob [{}] with eTag [{}] ", key, eTag);
                }
            } catch (Exception e) {
                throw new DataStoreException(e);
            }
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        LOG.debug("write of [{}], length=[{}], in [{}]ms",
            new Object[] {identifier, file.length(), (System.currentTimeMillis() - start)});
    }

    /**
     * Check if record identified by identifier exists in the cloud blob store.
     */
    @Override
    public boolean exists(DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            boolean exists = blobStore.blobExists(bucket, key);
            LOG.trace("exists [{}]: [{}] took [{}] ms.", identifier, exists,
                (System.currentTimeMillis() - start));
            return exists;
        } catch (Exception e) {
            throw new DataStoreException(
                "Error occured in checking existence for key [" + identifier.toString() + "]", e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public InputStream read(DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            Blob blob = blobStore.getBlob(bucket, key);
            if (blob == null) {
                throw new DataStoreException("Object not found: " + key);
            }

            InputStream in = blob.getPayload().openStream();
            LOG.debug("[{}] read took [{}]ms", identifier, (System.currentTimeMillis() - start));
            return in;
        } catch (Exception e) {
            throw new DataStoreException("Error reading blob for [{}]: " + key, e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return new RecordsIterator<DataIdentifier>(new Function<StorageMetadata, DataIdentifier>() {
            @Override public DataIdentifier apply(StorageMetadata input) {
                return new DataIdentifier(getIdentifierName(input.getName()));
            }
        });
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            blobStore.removeBlob(bucket, key);
            LOG.debug("Identifier [{}] deleted. It took [{}]ms.",
                new Object[] {identifier, (System.currentTimeMillis() - start)});
        } catch (Exception e) {
            throw new DataStoreException("Could not delete dataIdentifier " + identifier, e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void close() {
        blobStore.getContext().close();
        LOG.info("CloudBackend closed.");
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    /**
     * Properties used to configure the backend. If provided explicitly
     * before init is invoked then these take precedence
     *
     * @param properties to configure Backend
     */
    public void setProperties(Properties properties) {
        this.properties = CloudUtils.getProperties(properties);
    }

    @Override
    public void addMetadataRecord(final InputStream input, final String name)
        throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            File temp = File.createTempFile("metadata", null);
            temp.deleteOnExit();
            FileUtils.copyInputStreamToFile(input, temp);
            IOUtils.closeQuietly(input);
            Blob blob = blobStore.blobBuilder(addMetaKeyPrefix(name)).payload(temp).build();
            String eTag = blobStore.putBlob(bucket, blob, multipart());
            LOG.debug("Uploaded [{}] with eTag [{}]", name, eTag);
        } catch (Exception e) {
            LOG.error("Error in uploading metadata [{}]", name, e);
            throw new DataStoreException("Error in uploading", e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void addMetadataRecord(File input, String name) throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            Blob blob = blobStore.blobBuilder(addMetaKeyPrefix(name)).payload(input).build();
            String eTag = blobStore.putBlob(bucket, blob, multipart());
            LOG.debug("Uploaded [{}] with eTag [{}]", name, eTag);
        } catch (Exception e) {
            LOG.error("Error in uploading metadata [{}]", name, e);
            throw new DataStoreException("Error in uploading", e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            BlobMetadata metadata = blobStore.blobMetadata(bucket, addMetaKeyPrefix(name));
            return new CloudDataRecord(blobStore, bucket, name,
                metadata.getLastModified().getTime(), metadata.getSize(), true);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            Iterator<DataRecord> iter =
                new RecordsIterator<DataRecord>(new Function<StorageMetadata, DataRecord>() {
                    @Override public DataRecord apply(StorageMetadata input) {
                        return new CloudDataRecord(blobStore, bucket,
                            stripMetaKeyPrefix(input.getName()), input.getLastModified().getTime(),
                            input.getSize(), true);
                    }
                }, true, prefix);
            return Lists.newArrayList(iter);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            blobStore.removeBlob(bucket, addMetaKeyPrefix(name));
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return true;
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            List<String> deleteList = Lists.newArrayList();
            List<DataRecord> list = getAllMetadataRecords(prefix);
            for (DataRecord rec : list) {
                deleteList.add(META_KEY_PREFIX + rec.getIdentifier().toString());
            }

            if (deleteList.size() > 0) {
                blobStore.removeBlobs(bucket, deleteList);
            }
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Iterator<DataRecord> getAllRecords() {
        return new RecordsIterator<DataRecord>(new Function<StorageMetadata, DataRecord>() {
            @Override public DataRecord apply(StorageMetadata input) {
                return new CloudDataRecord(blobStore, bucket, getIdentifierName(input.getName()),
                    input.getLastModified().getTime(), input.getSize());
            }
        });
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            BlobMetadata metadata = blobStore.blobMetadata(bucket, key);
            if (metadata == null) {
                throw new IOException("Key not found " + identifier.toString());
            }

            CloudDataRecord record = new CloudDataRecord(blobStore, bucket, identifier.toString(),
                metadata.getLastModified().getTime(), metadata.getSize());
            LOG.debug("Identifier [{}]'s getRecord = [{}] took [{}]ms.",
                new Object[] {identifier, record, (System.currentTimeMillis() - start)});

            return record;
        } catch (Exception e) {
            LOG.info("getRecord:Identifier [{}] not found. Took [{}] ms.", identifier,
                (System.currentTimeMillis() - start));
            throw new DataStoreException(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    /**
     * Returns an iterator over the Cloud blobstore objects
     *
     * @param <T>
     */
    class RecordsIterator<T> extends AbstractIterator<T> {
        PageSet<? extends StorageMetadata> prevObjectListing;
        Queue<StorageMetadata> queue;
        long size;
        Function<StorageMetadata, T> transformer;
        boolean meta;
        String prefix;
        ListContainerOptions options;

        public RecordsIterator(Function<StorageMetadata, T> transformer) {
            queue = Lists.newLinkedList();
            this.transformer = transformer;
            options = NONE;
        }

        public RecordsIterator(Function<StorageMetadata, T> transformer, boolean meta,
            String prefix) {
            queue = Lists.newLinkedList();
            this.transformer = transformer;
            this.meta = meta;
            this.prefix = prefix;
            options = ListContainerOptions.Builder.inDirectory(META_KEY_PREFIX).maxResults(5);
        }

        @Override protected T computeNext() {
            if (queue.isEmpty()) {
                loadBatch();
            }

            if (!queue.isEmpty()) {
                return transformer.apply(queue.remove());
            }

            return endOfData();
        }

        private boolean loadBatch() {
            ClassLoader contextClassLoader = currentThread().getContextClassLoader();
            long start = System.currentTimeMillis();
            try {
                currentThread().setContextClassLoader(getClass().getClassLoader());

                // initialize the listing the first time
                if (prevObjectListing == null) {
                    prevObjectListing = blobStore.list(bucket, options);
                } else if (prevObjectListing.getNextMarker()
                    != null) { //already initialized more objects available
                    prevObjectListing = blobStore
                        .list(bucket, options.afterMarker(prevObjectListing.getNextMarker()));
                } else { // no more available
                    return false;
                }

                List<StorageMetadata> listing = Lists.newArrayList(Iterators
                    .filter(prevObjectListing.iterator(), new Predicate<StorageMetadata>() {
                        @Override public boolean apply(StorageMetadata input) {
                            if (!meta) {
                                return !input.getName().startsWith(META_KEY_PREFIX) && !input
                                    .getName().startsWith(META_KEY);
                            } else {
                                return input.getName().startsWith(META_KEY_PREFIX + prefix);
                            }
                        }
                    }));

                // After filtering no elements
                if (listing.isEmpty()) {
                    return false;
                }

                size += listing.size();
                queue.addAll(listing);

                LOG.info("Loaded batch of size [{}] in [{}] ms.", listing.size(),
                    (System.currentTimeMillis() - start));

                return true;
            } catch (Exception e) {
                LOG.warn("Could not list objects", e);
            } finally {
                if (contextClassLoader != null) {
                    currentThread().setContextClassLoader(contextClassLoader);
                }
            }
            return false;
        }
    }

    private static String addMetaKeyPrefix(String key) {
        return META_KEY_PREFIX + key;
    }

    private static String stripMetaKeyPrefix(String name) {
        if (name.startsWith(META_KEY_PREFIX)) {
            return name.substring(META_KEY_PREFIX.length());
        }
        return name;
    }

    /**
     * CloudDataRecord which lazily retrieves the input stream of the record.
     */
    static class CloudDataRecord implements DataRecord {
        private BlobStore blobStore;
        private DataIdentifier identifier;
        private long length;
        private long lastModified;
        private String bucket;
        private boolean isMeta;

        public CloudDataRecord(BlobStore blobStore, String bucket, String key, long lastModified,
            long length) {
            this(blobStore, bucket, key, lastModified, length, false);
        }

        public CloudDataRecord(BlobStore blobStore, String bucket, String key, long lastModified,
            long length, boolean isMeta) {
            this.blobStore = blobStore;
            this.identifier = new DataIdentifier(key);
            this.lastModified = lastModified;
            this.length = length;
            this.bucket = bucket;
            this.isMeta = isMeta;
        }

        @Override public DataIdentifier getIdentifier() {
            return identifier;
        }

        @Override public String getReference() {
            return identifier.toString();
        }

        @Override public long getLength() throws DataStoreException {
            return length;
        }

        @Override public InputStream getStream() throws DataStoreException {
            String id = getKeyName(identifier);
            if (isMeta) {
                id = addMetaKeyPrefix(identifier.toString());
            }
            try {
                return blobStore.getBlob(bucket, id).getPayload().openStream();
            } catch (IOException e) {
                throw new DataStoreException(e);
            }
        }

        @Override public long getLastModified() {
            return lastModified;
        }

        @Override public String toString() {
            return "CloudDataRecord{" + "identifier=" + identifier + ", length=" + length
                + ", lastModified=" + lastModified + ", bucket='" + bucket + '\'' + '}';
        }
    }

    /**
     * This method rename object keys in CloudBlobStore concurrently. The number of
     * concurrent threads is defined by 'maxConnections' property in
     * aws.properties. As Cloud blobStore doesn't have "move" command, this method simulate
     * move as copy object object to new key and then delete older key.
     */
    private void renameKeys() throws DataStoreException {
        long startTime = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        long count = 0;
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            Iterator<String> iter =
                new RecordsIterator<String>(new Function<StorageMetadata, String>() {
                    @Override public String apply(StorageMetadata input) {
                        return input.getName();
                    }
                });
            List<String> deleteList = new ArrayList<String>();
            int nThreads = Integer.parseInt(properties.getProperty("maxConnections"));
            ExecutorService executor = Executors
                .newFixedThreadPool(nThreads, new NamedThreadFactory("cloud-object-rename-worker"));
            boolean taskAdded = false;
            while (iter.hasNext()) {
                String key = iter.next();
                executor.execute(new KeyRenameThread(key));
                taskAdded = true;
                count++;
                // delete the object if it follows old key name format
                if (key.startsWith(KEY_PREFIX)) {
                    deleteList.add(key);
                }
            }

            // This will make the executor accept no new threads
            // and finish all existing threads in the queue
            executor.shutdown();

            try {
                // Wait until all threads are finish
                while (taskAdded && !executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOG.info("Rename cloud blob store keys tasks timedout. Waiting again");
                }
            } catch (InterruptedException ie) {

            }
            LOG.info("Renamed [{}] keys, time taken [{}]sec", count,
                ((System.currentTimeMillis() - startTime) / 1000));
            // Delete older keys.
            if (deleteList.size() > 0) {
                int batchSize = 500, startIndex = 0, size = deleteList.size();
                List<List<String>> partitions = Lists.partition(deleteList, batchSize);

                for (List<String> partition : partitions) {
                    blobStore.removeBlobs(bucket, partition);
                    LOG.info("Records[{}] deleted in datastore from index", partition);
                }
            }
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    /**
     * The method convert old key format to new format. For e.g. this method
     * converts old key dataStore_004cb70c8f87d78f04da41e7547cb434094089ea to
     * 004c-b70c8f87d78f04da41e7547cb434094089ea.
     */
    private static String convertKey(String oldKey) throws IllegalArgumentException {
        if (!oldKey.startsWith(KEY_PREFIX)) {
            return oldKey;
        }
        String key = oldKey.substring(KEY_PREFIX.length());
        return key.substring(0, 4) + DASH + key.substring(4);
    }

    /**
     * Get key from data identifier. Object is stored with key in cloud blob store.
     */
    private static String getKeyName(DataIdentifier identifier) {
        String key = identifier.toString();
        return key.substring(0, 4) + DASH + key.substring(4);
    }

    /**
     * Get data identifier from key.
     */
    private static String getIdentifierName(String key) {
        if (!key.contains(DASH)) {
            return null;
        } else if (key.contains(META_KEY_PREFIX)) {
            return key;
        }
        return key.substring(0, 4) + key.substring(5);
    }


    /**
     * The class renames object key in cloud blob store in a thread.
     */
    private class KeyRenameThread implements Runnable {

        private String oldKey;

        public void run() {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            String newKey = null;
            try {
                Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
                newKey = convertKey(oldKey);
                blobStore.copyBlob(bucket, oldKey, bucket, newKey, CopyOptions.NONE);
                LOG.debug("[{}] renamed to [{}] ", oldKey, newKey);
            } catch (Exception ie) {
                LOG.error(" Exception in renaming [{}] to [{}] ",
                    new Object[] {ie, oldKey, newKey});
            } finally {
                if (contextClassLoader != null) {
                    Thread.currentThread().setContextClassLoader(contextClassLoader);
                }
            }
        }

        public KeyRenameThread(String oldKey) {
            this.oldKey = oldKey;
        }
    }
}
