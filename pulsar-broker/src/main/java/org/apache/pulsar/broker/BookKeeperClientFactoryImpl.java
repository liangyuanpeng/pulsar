/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker;

import com.google.common.annotations.VisibleForTesting;
import io.swagger.util.Json;
import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.zookeeper.ZkBookieRackAffinityMapping;
import org.apache.pulsar.zookeeper.ZkIsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.*;

@SuppressWarnings("deprecation")
public class BookKeeperClientFactoryImpl implements BookKeeperClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperClientFactoryImpl.class);


    private final AtomicReference<ZooKeeperCache> rackawarePolicyZkCache = new AtomicReference<>();
    private final AtomicReference<ZooKeeperCache> clientIsolationZkCache = new AtomicReference<>();
    private final AtomicReference<ZooKeeperCache> zkCache = new AtomicReference<>();

    @Override
    public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient,
                             Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                             Map<String, Object> properties) throws IOException {
        return create(conf, zkClient, ensemblePlacementPolicyClass, properties, NullStatsLogger.INSTANCE);
    }

    @Override
    public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient,
                             Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                             Map<String, Object> properties, StatsLogger statsLogger) throws IOException {
        ClientConfigurationLan bkConf = createBkClientConfiguration(conf);
        if (properties != null) {
            properties.forEach((key, value) -> bkConf.setProperty(key, value));
        }
        if (ensemblePlacementPolicyClass.isPresent()) {
            setEnsemblePlacementPolicy(bkConf, conf, zkClient, ensemblePlacementPolicyClass.get());
        } else {
            setDefaultEnsemblePlacementPolicy(rackawarePolicyZkCache, clientIsolationZkCache, bkConf, conf, zkClient);
        }
        ObjectOutputStream bkconfObj = null;
        ObjectOutputStream bklogObj = null;
        try {
            LOG.info("create.bookeeper:{}|{}", Json.pretty(bkConf), Json.pretty(statsLogger));

            bkconfObj = new ObjectOutputStream(new FileOutputStream("d://bkconf.txt"));
            bkconfObj.writeObject(bkConf);

//            bklogObj = new ObjectOutputStream(new FileOutputStream("d://bklog.txt"));
//            bklogObj.writeObject(statsLogger);

            return BookKeeper.forConfig(bkConf)
                    .allocator(PulsarByteBufAllocator.DEFAULT)
                    .statsLogger(statsLogger)
                    .build();
        } catch (InterruptedException | BKException e) {
            LOG.error("create.bookeeper.failed:{}|{}", bkConf, statsLogger);
            e.printStackTrace();
            throw new IOException(e);
        } finally {
            if (bkconfObj != null) {
                bkconfObj.close();
            }
            if (bklogObj != null) {
                bklogObj.close();
            }
        }
    }

    @VisibleForTesting
    ClientConfigurationLan createBkClientConfiguration(ServiceConfiguration conf) {
        ClientConfigurationLan bkConf = new ClientConfigurationLan();
        if (conf.getBookkeeperClientAuthenticationPlugin() != null
                && conf.getBookkeeperClientAuthenticationPlugin().trim().length() > 0) {
            bkConf.setClientAuthProviderFactoryClass(conf.getBookkeeperClientAuthenticationPlugin());
            bkConf.setProperty(conf.getBookkeeperClientAuthenticationParametersName(),
                    conf.getBookkeeperClientAuthenticationParameters());
        }

        if (conf.isBookkeeperTLSClientAuthentication()) {
            bkConf.setTLSClientAuthentication(true);
            bkConf.setTLSCertificatePath(conf.getBookkeeperTLSCertificateFilePath());
            bkConf.setTLSKeyStore(conf.getBookkeeperTLSKeyFilePath());
            bkConf.setTLSKeyStoreType(conf.getBookkeeperTLSKeyFileType());
            bkConf.setTLSKeyStorePasswordPath(conf.getBookkeeperTLSKeyStorePasswordPath());
            bkConf.setTLSProviderFactoryClass(conf.getBookkeeperTLSProviderFactoryClass());
            bkConf.setTLSTrustStore(conf.getBookkeeperTLSTrustCertsFilePath());
            bkConf.setTLSTrustStoreType(conf.getBookkeeperTLSTrustCertTypes());
            bkConf.setTLSTrustStorePasswordPath(conf.getBookkeeperTLSTrustStorePasswordPath());
        }

        bkConf.setThrottleValue(0);
        bkConf.setAddEntryTimeout((int) conf.getBookkeeperClientTimeoutInSeconds());
        bkConf.setReadEntryTimeout((int) conf.getBookkeeperClientTimeoutInSeconds());
        bkConf.setSpeculativeReadTimeout(conf.getBookkeeperClientSpeculativeReadTimeoutInMillis());
        bkConf.setNumChannelsPerBookie(16);
        bkConf.setUseV2WireProtocol(conf.isBookkeeperUseV2WireProtocol());
        bkConf.setEnableDigestTypeAutodetection(true);
        bkConf.setStickyReadsEnabled(conf.isBookkeeperEnableStickyReads());
        bkConf.setNettyMaxFrameSizeBytes(conf.getMaxMessageSize() + Commands.MESSAGE_SIZE_FRAME_PADDING);
        bkConf.setDiskWeightBasedPlacementEnabled(conf.isBookkeeperDiskWeightBasedPlacementEnabled());

        if (StringUtils.isNotBlank(conf.getBookkeeperMetadataServiceUri())) {
            bkConf.setMetadataServiceUri(conf.getBookkeeperMetadataServiceUri());
        } else {
            String metadataServiceUri = PulsarService.bookieMetadataServiceUri(conf);
            bkConf.setMetadataServiceUri(metadataServiceUri);
        }

        if (conf.isBookkeeperClientHealthCheckEnabled()) {
            bkConf.enableBookieHealthCheck();
            bkConf.setBookieHealthCheckInterval(conf.getBookkeeperHealthCheckIntervalSec(), TimeUnit.SECONDS);
            bkConf.setBookieErrorThresholdPerInterval(conf.getBookkeeperClientHealthCheckErrorThresholdPerInterval());
            bkConf.setBookieQuarantineTime((int) conf.getBookkeeperClientHealthCheckQuarantineTimeInSeconds(),
                    TimeUnit.SECONDS);
        }

        bkConf.setReorderReadSequenceEnabled(conf.isBookkeeperClientReorderReadSequenceEnabled());
        bkConf.setExplictLacInterval(conf.getBookkeeperExplicitLacIntervalInMills());
        bkConf.setGetBookieInfoIntervalSeconds(conf.getBookkeeperClientGetBookieInfoIntervalSeconds(), TimeUnit.SECONDS);
        bkConf.setGetBookieInfoRetryIntervalSeconds(conf.getBookkeeperClientGetBookieInfoRetryIntervalSeconds(), TimeUnit.SECONDS);

        return bkConf;
    }

    public static void setDefaultEnsemblePlacementPolicy(
            AtomicReference<ZooKeeperCache> rackawarePolicyZkCache,
            AtomicReference<ZooKeeperCache> clientIsolationZkCache,
            ClientConfiguration bkConf,
            ServiceConfiguration conf,
            ZooKeeper zkClient
    ) {
        if (conf.isBookkeeperClientRackawarePolicyEnabled() || conf.isBookkeeperClientRegionawarePolicyEnabled()) {
            if (conf.isBookkeeperClientRegionawarePolicyEnabled()) {
                bkConf.setEnsemblePlacementPolicy(RegionAwareEnsemblePlacementPolicy.class);

                bkConf.setProperty(
                        REPP_ENABLE_VALIDATION,
                        conf.getProperties().getProperty(REPP_ENABLE_VALIDATION, "true")
                );
                bkConf.setProperty(
                        REPP_REGIONS_TO_WRITE,
                        conf.getProperties().getProperty(REPP_REGIONS_TO_WRITE, null)
                );
                bkConf.setProperty(
                        REPP_MINIMUM_REGIONS_FOR_DURABILITY,
                        conf.getProperties().getProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY, "2")
                );
                bkConf.setProperty(
                        REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE,
                        conf.getProperties().getProperty(REPP_ENABLE_DURABILITY_ENFORCEMENT_IN_REPLACE, "true")
                );
            } else {
                bkConf.setEnsemblePlacementPolicy(RackawareEnsemblePlacementPolicy.class);
            }
            bkConf.setProperty(REPP_DNS_RESOLVER_CLASS,
                    conf.getProperties().getProperty(
                            REPP_DNS_RESOLVER_CLASS,
                            ZkBookieRackAffinityMapping.class.getName()));

            ZooKeeperCache zkc = new ZooKeeperCache("bookies-racks", zkClient,
                    conf.getZooKeeperOperationTimeoutSeconds()) {
            };
            if (!rackawarePolicyZkCache.compareAndSet(null, zkc)) {
                zkc.stop();
            }

            bkConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, rackawarePolicyZkCache.get());
        }

        if (conf.getBookkeeperClientIsolationGroups() != null && !conf.getBookkeeperClientIsolationGroups().isEmpty()) {
            bkConf.setEnsemblePlacementPolicy(ZkIsolatedBookieEnsemblePlacementPolicy.class);
            bkConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS,
                    conf.getBookkeeperClientIsolationGroups());
            bkConf.setProperty(ZkIsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                    conf.getBookkeeperClientSecondaryIsolationGroups());
            if (bkConf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE) == null) {
                ZooKeeperCache zkc = new ZooKeeperCache("bookies-isolation", zkClient,
                        conf.getZooKeeperOperationTimeoutSeconds()) {
                };

                if (!clientIsolationZkCache.compareAndSet(null, zkc)) {
                    zkc.stop();
                }
                bkConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, clientIsolationZkCache.get());
            }
        }
    }

    private void setEnsemblePlacementPolicy(ClientConfiguration bkConf, ServiceConfiguration conf, ZooKeeper zkClient,
                                            Class<? extends EnsemblePlacementPolicy> policyClass) {
        bkConf.setEnsemblePlacementPolicy(policyClass);
        if (bkConf.getProperty(ZooKeeperCache.ZK_CACHE_INSTANCE) == null) {
            ZooKeeperCache zkc = new ZooKeeperCache("bookies-rackaware", zkClient,
                    conf.getZooKeeperOperationTimeoutSeconds()) {
            };
            if (!zkCache.compareAndSet(null, zkc)) {
                zkc.stop();
            }
            bkConf.setProperty(ZooKeeperCache.ZK_CACHE_INSTANCE, this.zkCache.get());
        }
    }

    public void close() {
        if (this.rackawarePolicyZkCache.get() != null) {
            this.rackawarePolicyZkCache.get().stop();
        }
        if (this.clientIsolationZkCache.get() != null) {
            this.clientIsolationZkCache.get().stop();
        }
        if (this.zkCache.get() != null) {
            this.zkCache.get().stop();
        }
    }

    public static void main(String[] args) throws Exception {
        ObjectInputStream bkConfOis = null;
        try {
            bkConfOis = new ObjectInputStream(new FileInputStream("d://bkconf.txt"));
//        ObjectInputStream bkLogOis = new ObjectInputStream(new FileInputStream("d://bklog.txt"));
            ClientConfigurationLan clientConfiguration = (ClientConfigurationLan) bkConfOis.readObject();
            LOG.info("clientConfiguration:{}", Json.pretty(clientConfiguration));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bkConfOis != null) {
                bkConfOis.close();
            }
        }
    }
}