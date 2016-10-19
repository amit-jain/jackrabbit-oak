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

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_NAME;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

class IncludeMatcher implements Matcher {
    private final Include include;
    private final int depth;
    private final String matchedPath;

    public IncludeMatcher(Include include) {
        this(include, 0, ROOT_NAME);
    }

    private IncludeMatcher(Include include, int depth, String matchedPath) {
        this.include = include;
        this.depth = depth;
        this.matchedPath = matchedPath;
    }

    @Override
    public Matcher next(String name) {
        if (hasMore()) {
            if (include.match(name, depth)) {
                String nextPath = concat(matchedPath, name);
                if (lastEntry() && include.getDirective() == Include.Directive.ALL) {
                    return new IncludeAllMatcher(nextPath, depth + 1);
                }
                return new IncludeMatcher(include, depth + 1, nextPath);
            } else {
                return Matcher.FAILED;
            }
        }
        return Matcher.FAILED;
    }

    @Override
    public boolean isMatch() {
        return true;
    }

    @Override
    public String getMatchedPath() {
        return matchedPath;
    }

    @Override
    public int depth() {
        return depth;
    }

    @Override
    public String toString() {
        return "IncludeMatcher{" +
                "include=" + include +
                ", depth=" + depth +
                ", matchedPath='" + matchedPath + '\'' +
                '}';
    }

    private boolean hasMore() {
        return depth < include.size();
    }

    private boolean lastEntry() {
        return depth == include.size() - 1;
    }
}
