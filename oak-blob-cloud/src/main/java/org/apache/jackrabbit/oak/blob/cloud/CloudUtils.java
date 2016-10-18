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

import java.util.Properties;

import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.jclouds.Constants;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.jclouds.Constants.PROPERTY_CREDENTIAL;
import static org.jclouds.Constants.PROPERTY_IDENTITY;
import static org.jclouds.Constants.PROPERTY_PROVIDER;
import static org.jclouds.ContextBuilder.newBuilder;
import static org.jclouds.location.reference.LocationConstants.PROPERTY_REGIONS;

/**
 * Cloud BlobStore utilities.
 */
public final class CloudUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CloudUtils.class);

    /**
     * private constructor so that class cannot initialized from outside.
     */
    private CloudUtils() {
    }

    public static Properties getProperties(Properties props) {
        Properties properties = new Properties();

        set(properties, Constants.PROPERTY_IDENTITY, props, S3Constants.ACCESS_KEY);
        set(properties, Constants.PROPERTY_CREDENTIAL, props, S3Constants.SECRET_KEY);
        set(properties, Constants.PROPERTY_PROVIDER, props, "provider");
        set(properties, "bucket", props, S3Constants.S3_BUCKET);
        set(properties, PROPERTY_REGIONS, props, S3Constants.S3_REGION);
        set(properties, "renameKeys", props, S3Constants.S3_RENAME_KEYS);
        set(properties, Constants.PROPERTY_MAX_CONNECTIONS_PER_CONTEXT, props, S3Constants.S3_MAX_CONNS);
        set(properties, Constants.PROPERTY_MAX_RETRIES, props, S3Constants.S3_MAX_ERR_RETRY);
        set(properties, Constants.PROPERTY_SO_TIMEOUT, props, S3Constants.S3_SOCK_TIMEOUT);
        set(properties, Constants.PROPERTY_CONNECTION_TIMEOUT, props, S3Constants.S3_CONN_TIMEOUT);
        set(properties, Constants.PROPERTY_PROXY_HOST, props, S3Constants.PROXY_HOST);
        set(properties, Constants.PROPERTY_PROXY_PORT, props, S3Constants.PROXY_PORT);
        set(properties, Constants.PROPERTY_ENDPOINT, props, S3Constants.S3_END_POINT);

        return properties;
    }

    public static BlobStore getBlobStore(Properties properties) {
        String accessKey = (String) properties.get(PROPERTY_IDENTITY);
        String secretKey = (String) properties.get(PROPERTY_CREDENTIAL);
        String cloudProvider = (String) properties.get(PROPERTY_PROVIDER);

        return newBuilder(cloudProvider)
            .credentials(accessKey, secretKey)
            .overrides(properties)
            .buildView(BlobStoreContext.class).getBlobStore();
    }

    public static Location configuredLocation(Properties properties) {
        String region = properties.getProperty(PROPERTY_REGIONS);
        Location parent = new LocationBuilder().scope(LocationScope.PROVIDER)
            .id(properties.getProperty(Constants.PROPERTY_PROVIDER))
            .description(properties.getProperty(Constants.PROPERTY_PROVIDER)).build();
        return new LocationBuilder().scope(LocationScope.REGION).id(region).description(region)
                .parent(parent).build();
    }


    private static void set(Properties newProps, String newProp, Properties props, String prop) {
        if (props.containsKey(prop)) {
            newProps.setProperty(newProp, props.getProperty(prop));
        }
    }
}
