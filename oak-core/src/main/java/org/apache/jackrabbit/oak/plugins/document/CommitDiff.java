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
package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.json.BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingHandler;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

/**
 * Implementation of a {@link NodeStateDiff}, which translates the diffs into
 * {@link UpdateOp}s of a commit.
 */
class CommitDiff implements NodeStateDiff {

    private final DocumentNodeStore store;

    private final Commit commit;

    private final JsopBuilder builder;

    private final BlobSerializer blobs;

    private final BundlingHandler bundlingHandler;

    CommitDiff(@Nonnull DocumentNodeStore store, @Nonnull Commit commit,
               @Nonnull BlobSerializer blobs) {
        this(checkNotNull(store), checkNotNull(commit), store.getBundlingRoot(),
                new JsopBuilder(), checkNotNull(blobs));
    }

    private CommitDiff(DocumentNodeStore store, Commit commit, BundlingHandler bundlingHandler,
               JsopBuilder builder, BlobSerializer blobs) {
        this.store = store;
        this.commit = commit;
        this.bundlingHandler = bundlingHandler;
        this.builder = builder;
        this.blobs = blobs;
        setMetaProperties();
        informCommitAboutBundledNodes();
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        setProperty(after);
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        setProperty(after);
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        commit.updateProperty(bundlingHandler.getRootBundlePath(), bundlingHandler.getPropertyPath(before.getName()), null);
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        BundlingHandler child = bundlingHandler.childAdded(name, after);
        if (child.isBundlingRoot()) {
            //TODO Handle case for leaf node optimization
            commit.addNode(new DocumentNodeState(store, child.getRootBundlePath(),
                    new RevisionVector(commit.getRevision())));
        }
        return after.compareAgainstBaseState(EMPTY_NODE,
                new CommitDiff(store, commit, child, builder, blobs));
    }

    @Override
    public boolean childNodeChanged(String name,
                                    NodeState before,
                                    NodeState after) {
        //TODO [bundling] Handle change of primaryType
        BundlingHandler child = bundlingHandler.childChanged(name, after);
        return after.compareAgainstBaseState(before,
                new CommitDiff(store, commit, child, builder, blobs));
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        BundlingHandler child = bundlingHandler.childDeleted(name, before);
        if (child.isBundlingRoot()) {
            //TODO [bundling] Handle delete
            commit.removeNode(child.getRootBundlePath(), before);
        }
        return MISSING_NODE.compareAgainstBaseState(before,
                new CommitDiff(store, commit, child, builder, blobs));
    }

    //----------------------------< internal >----------------------------------

    private void setMetaProperties() {
        for (PropertyState ps : bundlingHandler.getMetaProps()){
            setProperty(ps);
        }
    }

    private void informCommitAboutBundledNodes() {
        if (bundlingHandler.isBundledNode()){
            commit.addBundledNode(bundlingHandler.getNodeFullPath());
        }
    }

    private void setProperty(PropertyState property) {
        builder.resetWriter();
        JsonSerializer serializer = new JsonSerializer(builder, blobs);
        serializer.serialize(property);
        commit.updateProperty(bundlingHandler.getRootBundlePath(), bundlingHandler.getPropertyPath(property.getName()),
                 serializer.toString());
        if ((property.getType() == Type.BINARY)
                || (property.getType() == Type.BINARIES)) {
            this.commit.markNodeHavingBinary(bundlingHandler.getRootBundlePath());
        }
    }
}
