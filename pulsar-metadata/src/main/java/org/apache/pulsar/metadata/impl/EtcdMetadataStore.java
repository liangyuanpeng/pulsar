/**
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
package org.apache.pulsar.metadata.impl;

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.*;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class EtcdMetadataStore extends AbstractMetadataStore implements MetadataStoreExtended, Watcher, MetadataStoreLifecycle {

    private final String metadataURL;
    private final MetadataStoreConfig metadataStoreConfig;

    public EtcdMetadataStore(String metadataURL, MetadataStoreConfig metadataStoreConfig) throws MetadataStoreException {
        log.info("metadata.EtcdMetadataStore");
        try {
            this.metadataURL = metadataURL;
            this.metadataStoreConfig = metadataStoreConfig;
        } catch (Throwable t) {
            throw new MetadataStoreException(t);
        }
    }

    @VisibleForTesting
    public EtcdMetadataStore(ZooKeeper zkc) {
        this.metadataURL = null;
        this.metadataStoreConfig = null;
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        log.info("metadata.get");
        CompletableFuture<Optional<GetResult>> future = new CompletableFuture<>();

//        [18:34:31.390] INFO  org.apache.pulsar.metadata.impl.ZKMetadataStore 124 get - getResult:/loadbalance/leader|{
//                "present" : false
//}

        return future;
    }

    @Override
    public CompletableFuture<List<String>> getChildrenFromStore(String path) {
        log.info("metadata.getChildrenFromStore");
        CompletableFuture<List<String>> future = new CompletableFuture<>();

        return future;
    }

    @Override
    public CompletableFuture<Boolean> existsFromStore(String path) {
        log.info("metadata.existsFromStore");
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        return future;
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> optExpectedVersion) {
        log.info("metadata.put");
        return put(path, value, optExpectedVersion, EnumSet.noneOf(CreateOption.class));
    }

    @Override
    public CompletableFuture<Stat> storePut(String path, byte[] value, Optional<Long> optExpectedVersion,
            EnumSet<CreateOption> options) {
        log.info("metadata.storePut");
        boolean hasVersion = optExpectedVersion.isPresent();
        int expectedVersion = optExpectedVersion.orElse(-1L).intValue();

        CompletableFuture<Stat> future = new CompletableFuture<>();
        Stat stat = new Stat("1", 0, 0, 0, false, true);
        future.complete(stat);

//        [18:32:27.447] INFO  org.apache.pulsar.metadata.impl.ZKMetadataStore 267 storePut - stat:/counters/producer-name/-|{
//                "path" : "/counters/producer-name/-0000000006",
//                "version" : 0,
//                "creationTimestamp" : 0,
//                "modificationTimestamp" : 0,
//                "ephemeral" : true,
//                "createdBySelf" : true
//}

        return future;
    }

    @Override
    public CompletableFuture<Void> storeDelete(String path, Optional<Long> optExpectedVersion) {
        log.info("metadata.storeDelete");
        int expectedVersion = optExpectedVersion.orElse(-1L).intValue();

        CompletableFuture<Void> future = new CompletableFuture<>();

        return future;
    }

    @Override
    public void close() throws Exception {
        log.info("metadata.close");
        super.close();
    }

    private static MetadataStoreException getException(Code code, String path) {
        KeeperException ex = KeeperException.create(code, path);

        switch (code) {
        case BADVERSION:
            return new BadVersionException(ex);
        case NONODE:
            return new NotFoundException(ex);
        case NODEEXISTS:
            return new AlreadyExistsException(ex);
        default:
            return new MetadataStoreException(ex);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        log.info("metadata.process");
        if (log.isDebugEnabled()) {
            log.debug("Received ZK watch : {}", event);
        }
        String path = event.getPath();
        if (path == null) {
            // Ignore Session events
            return;
        }

        NotificationType type;
        switch (event.getType()) {
        case NodeCreated:
            type = NotificationType.Created;
            break;

        case NodeDataChanged:
            type = NotificationType.Modified;
            break;

        case NodeChildrenChanged:
            type = NotificationType.ChildrenChanged;
            break;

        case NodeDeleted:
            type = NotificationType.Deleted;
            break;

        default:
            return;
        }

        receivedNotification(new Notification(type, event.getPath()));
    }

    private static CreateMode getCreateMode(EnumSet<CreateOption> options) {
        if (options.contains(CreateOption.Ephemeral)) {
            if (options.contains(CreateOption.Sequential)) {
                return CreateMode.EPHEMERAL_SEQUENTIAL;
            } else {
                return CreateMode.EPHEMERAL;
            }
        } else if (options.contains(CreateOption.Sequential)) {
            return CreateMode.PERSISTENT_SEQUENTIAL;
        } else {
            return CreateMode.PERSISTENT;
        }
    }

    @Override
    public CompletableFuture<Void> initializeCluster() {
        log.info("metadata.initializeCluster");

        if (this.metadataURL == null) {
            return FutureUtil.failedFuture(new MetadataStoreException("metadataURL is not set"));
        }
        if (this.metadataStoreConfig == null) {
            return FutureUtil.failedFuture(new MetadataStoreException("metadataStoreConfig is not set"));
        }
        int chrootIndex = metadataURL.indexOf("/");
        if (chrootIndex > 0) {
            String chrootPath = metadataURL.substring(chrootIndex);
            String zkConnectForChrootCreation = metadataURL.substring(0, chrootIndex);
            try (ZooKeeper chrootZk = ZooKeeperClient.newBuilder()
                    .connectString(zkConnectForChrootCreation)
                    .sessionTimeoutMs(metadataStoreConfig.getSessionTimeoutMillis())
                    .connectRetryPolicy(
                            new BoundExponentialBackoffRetryPolicy(metadataStoreConfig.getSessionTimeoutMillis(),
                                    metadataStoreConfig.getSessionTimeoutMillis(), 0))
                    .build()) {
                if (chrootZk.exists(chrootPath, false) == null) {
                    ZkUtils.createFullPathOptimistic(chrootZk, chrootPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                    log.info("Created zookeeper chroot path {} successfully", chrootPath);
                }
            } catch (Exception e) {
                return FutureUtil.failedFuture(e);
            }
        }
        return CompletableFuture.completedFuture(null);
    }
}
