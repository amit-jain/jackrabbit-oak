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
package org.apache.jackrabbit.oak.security.authentication.token;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.SimpleCredentials;
import javax.security.auth.login.LoginException;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.api.security.authentication.token.TokenCredentials;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenInfo;
import org.apache.jackrabbit.oak.spi.security.authentication.token.TokenProvider;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TokenAuthenticationTest extends AbstractTokenTest {

    private TokenAuthentication authentication;
    private TokenProviderImpl tokenProvider;
    private String userId;

    @Before
    public void before() throws Exception {
        super.before();
        tokenProvider = createTokenProvider(root, getUserConfiguration());

        root.commit();
        authentication = new TokenAuthentication(tokenProvider);
        userId = getTestUser().getID();
    }

    @Test
    public void testAuthenticateWithInvalidCredentials() throws Exception {
        List<Credentials> invalid = new ArrayList<Credentials>();
        invalid.add(new GuestCredentials());
        invalid.add(new SimpleCredentials(userId, new char[0]));

        for (Credentials creds : invalid) {
            assertFalse(authentication.authenticate(creds));
        }
    }

    @Test
    public void testAuthenticateWithInvalidTokenCredentials() throws Exception {
        try {
            authentication.authenticate(new TokenCredentials(UUID.randomUUID().toString()));
            fail("LoginException expected");
        } catch (LoginException e) {
            // success
        }
    }

    @Test
    public void testAuthenticate() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertTrue(authentication.authenticate(new TokenCredentials(info.getToken())));
    }

    @Test
    public void testGetTokenInfoBeforeAuthenticate() {
        try {
            authentication.getTokenInfo();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            // success
        }
    }

    @Test
    public void testGetTokenInfoAfterAuthenticate() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        authentication.authenticate(new TokenCredentials(info.getToken()));

        TokenInfo info2 = authentication.getTokenInfo();
        assertNotNull(info2);
        assertEquals(info.getUserId(), info2.getUserId());
    }

    @Test
    public void testAuthenticateNotMatchingToken() {
        TokenInfo info = tokenProvider.createToken(userId, ImmutableMap.of(TokenConstants.TOKEN_ATTRIBUTE + "_mandatory", "val"));
        try {
            authentication.authenticate(new TokenCredentials(info.getToken()));
            fail("LoginException expected");
        } catch (LoginException e) {
            // success
        }
    }

    @Test
    public void testAuthenticateExpiredToken() {
        TokenProvider tp = new TokenProviderImpl(root,
                ConfigurationParameters.of(TokenProvider.PARAM_TOKEN_EXPIRATION, 1),
                getUserConfiguration());

        TokenInfo info = tp.createToken(userId, Collections.<String, Object>emptyMap());
        waitUntilExpired(info);

        try {
            new TokenAuthentication(tp).authenticate(new TokenCredentials(info.getToken()));
            fail("LoginException expected");
        } catch (LoginException e) {
            // success
        }

        // expired token must have been removed
        assertNull(tp.getTokenInfo(info.getToken()));
    }

    @Test(expected = IllegalStateException.class)
    public void testGetUserIdBeforeLogin() {
        authentication.getUserId();
    }

    @Test
    public void testGetUserId() throws LoginException {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertTrue(authentication.authenticate(new TokenCredentials(info.getToken())));
        assertEquals(userId, authentication.getUserId());
    }

    @Test(expected = IllegalStateException.class)
    public void testGetUserPrincipalBeforeLogin() {
        authentication.getUserPrincipal();
    }

    @Test
    public void testGetUserPrincipal() throws Exception {
        TokenInfo info = tokenProvider.createToken(userId, Collections.<String, Object>emptyMap());
        assertTrue(authentication.authenticate(new TokenCredentials(info.getToken())));
        assertEquals(getTestUser().getPrincipal(), authentication.getUserPrincipal());
    }

    @Test
    public void testGetUserPrincipalNonDefaultProvider() throws Exception {
        TokenInfo info = mock(TokenInfo.class);
        when(info.isExpired(anyLong())).thenReturn(false);
        when(info.matches(any(TokenCredentials.class))).thenReturn(true);

        TokenProvider tp = when(mock(TokenProvider.class).getTokenInfo(anyString())).thenReturn(info).getMock();
        TokenAuthentication ta = new TokenAuthentication(tp);

        assertTrue(ta.authenticate(new TokenCredentials("token")));
        assertNull(ta.getUserPrincipal());
    }

    @Test
    public void testAuthenticateRefreshToken() throws Exception {
        TokenCredentials tc = new TokenCredentials("token");
        TokenProvider tp = mock(TokenProvider.class);
        TokenInfo ti = mock(TokenInfo.class);

        when(tp.getTokenInfo(anyString())).thenReturn(ti);
        when(ti.isExpired(anyLong())).thenReturn(false);
        when(ti.matches(tc)).thenReturn(true);

        TokenAuthentication auth = new TokenAuthentication(tp);
        try {
            assertTrue(auth.authenticate(tc));
            Mockito.verify(ti).resetExpiration(anyLong());
        } catch (LoginException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testAuthenticateSkipRefreshToken() throws Exception {
        TokenCredentials tc = new TokenCredentials("token");
        tc.setAttribute(TokenConstants.TOKEN_SKIP_REFRESH, "");

        TokenProvider tp = mock(TokenProvider.class);
        TokenInfo ti = mock(TokenInfo.class);

        when(tp.getTokenInfo(anyString())).thenReturn(ti);
        when(ti.isExpired(anyLong())).thenReturn(false);
        when(ti.matches(tc)).thenReturn(true);

        TokenAuthentication auth = new TokenAuthentication(tp);
        try {
            assertTrue(auth.authenticate(tc));
            Mockito.verify(ti, Mockito.never()).resetExpiration(anyLong());
        } catch (LoginException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testAuthenticateExpiredTokenMock() {
        TokenCredentials tc = new TokenCredentials("token");
        TokenProvider tp = mock(TokenProvider.class);
        TokenInfo ti = mock(TokenInfo.class);

        when(tp.getTokenInfo(anyString())).thenReturn(ti);
        when(ti.isExpired(anyLong())).thenReturn(true);

        TokenAuthentication auth = new TokenAuthentication(tp);
        try {
            auth.authenticate(tc);
            fail("LoginException expected");
        } catch (LoginException e) {
            // success
        }

        Mockito.verify(ti, Mockito.never()).matches(any());
        Mockito.verify(ti, Mockito.never()).resetExpiration(anyLong());
    }
}
