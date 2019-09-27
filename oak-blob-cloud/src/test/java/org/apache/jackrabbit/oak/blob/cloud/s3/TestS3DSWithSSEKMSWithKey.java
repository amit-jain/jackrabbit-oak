package org.apache.jackrabbit.oak.blob.cloud.s3;


import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test S3DataStore operation with SSE_KMS with custom key encryption.
 * It requires to pass aws config file via system property  or system properties by prefixing with 'ds.'.
 * See details @ {@link S3DataStoreUtils}.
 * For e.g. -Dconfig=/opt/cq/aws.properties. Sample aws properties located at
 * src/test/resources/aws.properties

 */
public class TestS3DSWithSSEKMSWithKey extends TestS3DSWithSSES3 {

        protected static final Logger LOG = LoggerFactory.getLogger(TestS3DSWithSSES3.class);

        @Override
        @Before
        public void setUp() throws Exception {
            super.setUp();
            String randomKey = "c8d21356-27c7-494e-8af3-6d10eda1f6c5";
            props.setProperty(S3Constants.S3_ENCRYPTION, S3Constants.S3_ENCRYPTION_SSE_KMS);
            props.setProperty(S3Constants.S3_SSE_KMS_KEYID, randomKey);
            props.setProperty("cacheSize", "0");
        }
}
