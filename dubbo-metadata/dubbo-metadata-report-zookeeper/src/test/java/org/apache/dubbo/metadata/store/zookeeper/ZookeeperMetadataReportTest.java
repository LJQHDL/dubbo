/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.metadata.store.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigItem;
import org.apache.dubbo.common.utils.JsonUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.metadata.MappingChangedEvent;
import org.apache.dubbo.metadata.MappingListener;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.definition.ServiceDefinitionBuilder;
import org.apache.dubbo.metadata.definition.model.FullServiceDefinition;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.identifier.KeyTypeEnum;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.remoting.zookeeper.curator5.DataListener;
import org.apache.dubbo.remoting.zookeeper.curator5.EventType;
import org.apache.dubbo.remoting.zookeeper.curator5.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.curator5.ZookeeperClientManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static org.apache.dubbo.metadata.ServiceNameMapping.DEFAULT_MAPPING_GROUP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 2018/10/9
 */
class ZookeeperMetadataReportTest {
    private ZookeeperMetadataReport zookeeperMetadataReport;
    private URL registryUrl;
    private ZookeeperClient zookeeperClient;
    private ZookeeperClientManager zookeeperClientManager;

    @BeforeEach
    public void setUp() throws Exception {
        this.registryUrl = URL.valueOf("zookeeper://127.0.0.1:2181");
        zookeeperClient = mock(ZookeeperClient.class);
        zookeeperClientManager = mock(ZookeeperClientManager.class);
        when(zookeeperClientManager.connect(any(URL.class))).thenReturn(zookeeperClient);
        this.zookeeperMetadataReport = new ZookeeperMetadataReport(registryUrl, zookeeperClientManager);
    }

    private void deletePath(MetadataIdentifier metadataIdentifier, ZookeeperMetadataReport zookeeperMetadataReport) {
        String category = zookeeperMetadataReport.toRootDir() + metadataIdentifier.getUniqueKey(KeyTypeEnum.PATH);
        zookeeperMetadataReport.zkClient.delete(category);
    }

    @Test
    void testStoreProvider() throws ClassNotFoundException, InterruptedException {
        String interfaceName = "org.apache.dubbo.metadata.store.zookeeper.ZookeeperMetadataReport4TstService";
        String version = "1.0.0.zk.md";
        String group = null;
        String application = "vic.zk.md";
        MetadataIdentifier providerMetadataIdentifier =
                storeProvider(zookeeperMetadataReport, interfaceName, version, group, application);

        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);

        verify(zookeeperClient, atLeastOnce()).createOrUpdate(pathCaptor.capture(), contentCaptor.capture(), eq(false));

        String expectedPath = zookeeperMetadataReport.getNodePath(providerMetadataIdentifier);
        assertEquals(expectedPath, pathCaptor.getValue());
        assertNotNull(contentCaptor.getValue());

        FullServiceDefinition fullServiceDefinition =
                JsonUtils.toJavaObject(contentCaptor.getValue(), FullServiceDefinition.class);
        assertEquals("zkTest", fullServiceDefinition.getParameters().get("paramTest"));

        deletePath(providerMetadataIdentifier, zookeeperMetadataReport);
        verify(zookeeperClient).delete(expectedPath);
    }

    @Test
    void testConsumer() throws ClassNotFoundException, InterruptedException {
        String interfaceName = "org.apache.dubbo.metadata.store.zookeeper.ZookeeperMetadataReport4TstService";
        String version = "1.0.0.zk.md";
        String group = null;
        String application = "vic.zk.md";
        MetadataIdentifier consumerMetadataIdentifier =
                storeConsumer(zookeeperMetadataReport, interfaceName, version, group, application);

        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);

        verify(zookeeperClient, atLeastOnce()).createOrUpdate(pathCaptor.capture(), contentCaptor.capture(), eq(false));

        String expectedPath = zookeeperMetadataReport.getNodePath(consumerMetadataIdentifier);
        assertEquals(expectedPath, pathCaptor.getValue());
        assertEquals("{\"paramConsumerTest\":\"zkCm\"}", contentCaptor.getValue());

        deletePath(consumerMetadataIdentifier, zookeeperMetadataReport);
        verify(zookeeperClient).delete(expectedPath);
    }

    @Test
    void testDoSaveMetadata() throws ExecutionException, InterruptedException {
        String interfaceName = "org.apache.dubbo.metadata.store.zookeeper.ZookeeperMetadataReport4TstService";
        String version = "1.0.0";
        String group = null;
        String application = "etc-metadata-report-consumer-test";
        String revision = "90980";
        String protocol = "xxx";
        URL url = generateURL(interfaceName, version, group, application);
        ServiceMetadataIdentifier serviceMetadataIdentifier =
                new ServiceMetadataIdentifier(interfaceName, version, group, "provider", revision, protocol);
        zookeeperMetadataReport.doSaveMetadata(serviceMetadataIdentifier, url);

        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);

        verify(zookeeperClient).createOrUpdate(pathCaptor.capture(), contentCaptor.capture(), eq(false));

        assertEquals(zookeeperMetadataReport.getNodePath(serviceMetadataIdentifier), pathCaptor.getValue());
        assertEquals(URL.encode(url.toFullString()), contentCaptor.getValue());
    }

    @Test
    void testDoRemoveMetadata() throws ExecutionException, InterruptedException {
        String interfaceName = "org.apache.dubbo.metadata.store.zookeeper.ZookeeperMetadataReport4TstService";
        String version = "1.0.0";
        String group = null;
        String application = "etc-metadata-report-consumer-test";
        String revision = "90980";
        String protocol = "xxx";
        URL url = generateURL(interfaceName, version, group, application);
        ServiceMetadataIdentifier serviceMetadataIdentifier =
                new ServiceMetadataIdentifier(interfaceName, version, group, "provider", revision, protocol);
        zookeeperMetadataReport.doSaveMetadata(serviceMetadataIdentifier, url);

        verify(zookeeperClient).createOrUpdate(anyString(), anyString(), eq(false));

        zookeeperMetadataReport.doRemoveMetadata(serviceMetadataIdentifier);
        verify(zookeeperClient).delete(zookeeperMetadataReport.getNodePath(serviceMetadataIdentifier));
    }

    @Test
    void testDoGetExportedURLs() throws ExecutionException, InterruptedException {
        String interfaceName = "org.apache.dubbo.metadata.store.zookeeper.ZookeeperMetadataReport4TstService";
        String version = "1.0.0";
        String group = null;
        String application = "etc-metadata-report-consumer-test";
        String revision = "90980";
        String protocol = "xxx";
        URL url = generateURL(interfaceName, version, group, application);
        ServiceMetadataIdentifier serviceMetadataIdentifier =
                new ServiceMetadataIdentifier(interfaceName, version, group, "provider", revision, protocol);

        String encodedUrl = URL.encode(url.toFullString());
        when(zookeeperClient.getContent(anyString())).thenReturn(encodedUrl);

        List<String> r = zookeeperMetadataReport.doGetExportedURLs(serviceMetadataIdentifier);
        assertEquals(1, r.size());
        assertEquals(url.toFullString(), r.get(0));
    }

    @Test
    void testDoSaveSubscriberData() throws ExecutionException, InterruptedException {
        String interfaceName = "org.apache.dubbo.metadata.store.zookeeper.ZookeeperMetadataReport4TstService";
        String version = "1.0.0";
        String group = null;
        String application = "etc-metadata-report-consumer-test";
        String revision = "90980";
        String protocol = "xxx";
        URL url = generateURL(interfaceName, version, group, application);
        SubscriberMetadataIdentifier subscriberMetadataIdentifier =
                new SubscriberMetadataIdentifier(application, revision);
        String r = JsonUtils.toJson(Arrays.asList(url.toString()));
        zookeeperMetadataReport.doSaveSubscriberData(subscriberMetadataIdentifier, r);

        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
        verify(zookeeperClient).createOrUpdate(pathCaptor.capture(), contentCaptor.capture(), eq(false));

        assertEquals(zookeeperMetadataReport.getNodePath(subscriberMetadataIdentifier), pathCaptor.getValue());
        assertEquals(r, contentCaptor.getValue());
    }

    @Test
    void testMapping() throws InterruptedException {
        String serviceKey = ZookeeperMetadataReportTest.class.getName();
        URL url = URL.valueOf("test://127.0.0.1:8888/" + serviceKey);
        String appNames = "demo1,demo2";

        CountDownLatch latch = new CountDownLatch(1);

        // Capture the listener registered by getServiceAppMapping
        ArgumentCaptor<DataListener> listenerCaptor = ArgumentCaptor.forClass(DataListener.class);
        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);

        Set<String> serviceAppMapping = zookeeperMetadataReport.getServiceAppMapping(
                serviceKey,
                new MappingListener() {
                    @Override
                    public void onEvent(MappingChangedEvent event) {
                        Set<String> apps = event.getApps();
                        assertEquals(apps.size(), 2);
                        assertTrue(apps.contains("demo1"));
                        assertTrue(apps.contains("demo2"));
                        latch.countDown();
                    }

                    @Override
                    public void stop() {}
                },
                url);

        // Verify addDataListener was called
        verify(zookeeperClient).addDataListener(pathCaptor.capture(), listenerCaptor.capture());

        // Initial result should be empty since we mocked nothing returned
        assertTrue(serviceAppMapping.isEmpty());

        ConfigItem configItem = zookeeperMetadataReport.getConfigItem(serviceKey, DEFAULT_MAPPING_GROUP);
        zookeeperMetadataReport.registerServiceAppMapping(
                serviceKey, DEFAULT_MAPPING_GROUP, appNames, configItem.getTicket());

        // Now simulate the event that would be triggered by registerServiceAppMapping
        // The original test relied on ZK watch. We simulate the watch firing.
        DataListener capturedListener = listenerCaptor.getValue();
        capturedListener.dataChanged(pathCaptor.getValue(), appNames, EventType.NodeDataChanged);

        latch.await(1, TimeUnit.SECONDS);
        assertEquals(0, latch.getCount());
    }

    @Test
    void testAppMetadata() {
        String serviceKey = ZookeeperMetadataReportTest.class.getName();
        String appName = "demo";
        URL url = URL.valueOf("test://127.0.0.1:8888/" + serviceKey);
        MetadataInfo metadataInfo = new MetadataInfo(appName);
        metadataInfo.addService(url);

        SubscriberMetadataIdentifier identifier =
                new SubscriberMetadataIdentifier(appName, metadataInfo.calAndGetRevision());

        // Test getAppMetadata (returns null initially)
        when(zookeeperClient.getContent(anyString())).thenReturn(null);
        MetadataInfo appMetadata = zookeeperMetadataReport.getAppMetadata(identifier, Collections.emptyMap());
        assertNull(appMetadata);

        // Test publishAppMetadata
        zookeeperMetadataReport.publishAppMetadata(identifier, metadataInfo);

        ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
        verify(zookeeperClient).createOrUpdate(anyString(), contentCaptor.capture(), anyBoolean());

        // Test getAppMetadata (returns value)
        // We need to mock getContent to return what we just published
        when(zookeeperClient.getContent(anyString())).thenReturn(contentCaptor.getValue());

        appMetadata = zookeeperMetadataReport.getAppMetadata(identifier, Collections.emptyMap());
        assertNotNull(appMetadata);
        assertEquals(appMetadata.calAndGetRevision(), metadataInfo.calAndGetRevision());
    }

    private MetadataIdentifier storeProvider(
            MetadataReport zookeeperMetadataReport,
            String interfaceName,
            String version,
            String group,
            String application)
            throws ClassNotFoundException, InterruptedException {
        URL url = URL.valueOf("xxx://" + NetUtils.getLocalAddress().getHostName() + ":4444/" + interfaceName
                + "?paramTest=zkTest&version=" + version + "&application=" + application
                + (group == null ? "" : "&group=" + group));

        MetadataIdentifier providerMetadataIdentifier =
                new MetadataIdentifier(interfaceName, version, group, PROVIDER_SIDE, application);
        Class interfaceClass = Class.forName(interfaceName);
        FullServiceDefinition fullServiceDefinition =
                ServiceDefinitionBuilder.buildFullDefinition(interfaceClass, url.getParameters());

        zookeeperMetadataReport.storeProviderMetadata(providerMetadataIdentifier, fullServiceDefinition);
        return providerMetadataIdentifier;
    }

    private MetadataIdentifier storeConsumer(
            MetadataReport zookeeperMetadataReport,
            String interfaceName,
            String version,
            String group,
            String application)
            throws ClassNotFoundException, InterruptedException {
        URL url = URL.valueOf("xxx://" + NetUtils.getLocalAddress().getHostName() + ":4444/" + interfaceName
                + "?version=" + version + "&application=" + application + (group == null ? "" : "&group=" + group));

        MetadataIdentifier consumerMetadataIdentifier =
                new MetadataIdentifier(interfaceName, version, group, CONSUMER_SIDE, application);

        Map<String, String> tmp = new HashMap<>();
        tmp.put("paramConsumerTest", "zkCm");
        zookeeperMetadataReport.storeConsumerMetadata(consumerMetadataIdentifier, tmp);

        return consumerMetadataIdentifier;
    }

    private URL generateURL(String interfaceName, String version, String group, String application) {
        URL url = URL.valueOf("xxx://" + NetUtils.getLocalAddress().getHostName() + ":8989/" + interfaceName
                + "?paramTest=etcdTest&version="
                + version + "&application="
                + application + (group == null ? "" : "&group=" + group));
        return url;
    }
}
