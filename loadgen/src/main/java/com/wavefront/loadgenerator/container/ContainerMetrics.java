package com.wavefront.loadgenerator.container;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ContainerMetrics {

    /*
    * cluster:
    * heapster.cluster.cpu.request k8s-cluster ("type"="cluster" and "cluster"="k8s-cluster") 1531944420000,1600
    * heapster.cluster.cpu.usage_rate k8s-cluster ("type"="cluster" and "cluster"="k8s-cluster") 1531943580000,161
    * heapster.cluster.cpu.limit k8s-cluster ("type"="cluster" and "cluster"="k8s-cluster") 1531944000000,100
    *
    * ns:
    * heapster.ns.cpu.request kube-system-ns ("namespace_name"="kube-system" and "type"="ns" and "cluster"="k8s-cluster") 1531944180000,1500
    * heapster.ns.cpu.usage_rate kube-system-ns ("namespace_name"="kube-system" and "type"="ns" and "cluster"="k8s-cluster") 1531944300000,153
    * heapster.ns.cpu.limit kube-system-ns ("namespace_name"="kube-system" and "type"="ns" and "cluster"="k8s-cluster") 1531944480000,0
    *
    * heapster.ns.memory.usage default-ns ("namespace_name"="default" and "type"="ns" and "cluster"="k8s-cluster") 1531946700000,1703092224
    *
    * node:
    * heapster.node.memory.page_faults k8s-slave-1 ("label.beta.kubernetes.io/os"="linux" and "nodename"="k8s-slave-1" and "label.kubernetes.io/hostname"="k8s-slave-1" and "label.beta.kubernetes.io/arch"="amd64" and "type"="node" and "cluster"="k8s-cluster") 1531946700000,198462
    * heapster.node.memory.page_faults k8s-slave-2 ("label.beta.kubernetes.io/os"="linux" and "label.kubernetes.io/hostname"="k8s-slave-2" and "label.beta.kubernetes.io/arch"="amd64" and "type"="node" and "nodename"="k8s-slave-2" and "cluster"="k8s-cluster") 1531946700000,180632
    * heapster.node.memory.page_faults k8s-master ("label.beta.kubernetes.io/os"="linux" and "nodename"="k8s-master" and "label.beta.kubernetes.io/arch"="amd64" and "label.kubernetes.io/hostname"="k8s-master" and "type"="node" and "cluster"="k8s-cluster") 1531946700000,170602
    *
    * pod:
    * heapster.pod.memory.rss k8s-master ("nodename"="k8s-master" and "pod_name"="kubernetes-dashboard-6948bdb78-hbdjk" and "type"="pod" and "namespace_name"="kube-system" and "label.pod-template-hash"="250468634" and "label.k8s-app"="kubernetes-dashboard" and "cluster"="k8s-cluster") 1531946700000,20623360
    * heapster.pod.memory.rss k8s-master ("cluster"="k8s-cluster" and "label.component"="kube-scheduler" and "nodename"="k8s-master" and "namespace_name"="kube-system" and "type"="pod" and "pod_name"="kube-scheduler-k8s-master" and "label.tier"="control-plane") 1531946700000,15036416
    * heapster.pod.memory.rss k8s-master ("label.pod-template-generation"="1" and "nodename"="k8s-master" and "label.controller-revision-hash"="1151982146" and "type"="pod" and "namespace_name"="kube-system" and "pod_name"="kube-proxy-5r6rc" and "label.k8s-app"="kube-proxy" and "cluster"="k8s-cluster") 1531946700000,11411456
    * heapster.pod.memory.rss k8s-master ("nodename"="k8s-master" and "namespace_name"="kube-system" and "type"="pod" and "pod_name"="coredns-78fcdf6894-7lqwd" and "label.pod-template-hash"="3497892450" and "label.k8s-app"="kube-dns" and "cluster"="k8s-cluster") 1531946700000,9334784
    * heapster.pod.memory.rss k8s-master ("nodename"="k8s-master" and "namespace_name"="kube-system" and "type"="pod" and "pod_name"="coredns-78fcdf6894-dcbth" and "label.pod-template-hash"="3497892450" and "label.k8s-app"="kube-dns" and "cluster"="k8s-cluster") 1531946700000,9273344
    *
    * pod_container:
    * heapster.pod_container.memory.usage k8s-slave-1 ("container_name"="calico-node" and "label.pod-template-generation"="1" and "nodename"="k8s-slave-1" and "namespace_name"="kube-system" and "type"="pod_container" and "label.controller-revision-hash"="1316067084" and "container_base_image"="quay.io/calico/node:v3.1.3" and "pod_name"="canal-7vtww" and "label.k8s-app"="canal" and "cluster"="k8s-cluster") 1531946700000,35672064
    * heapster.pod_container.memory.usage k8s-master ("container_name"="kubernetes-dashboard" and "container_base_image"="k8s.gcr.io/kubernetes-dashboard-amd64:v1.8.3" and "nodename"="k8s-master" and "pod_name"="kubernetes-dashboard-6948bdb78-hbdjk" and "type"="pod_container" and "namespace_name"="kube-system" and "label.pod-template-hash"="250468634" and "label.k8s-app"="kubernetes-dashboard" and "cluster"="k8s-cluster") 1531946700000,22294528
    * heapster.pod_container.memory.usage k8s-slave-1 ("label.version"="v6" and "container_name"="heapster" and "container_base_image"="wavefronthq/heapster-amd64:latest" and "nodename"="k8s-slave-1" and "namespace_name"="kube-system" and "type"="pod_container" and "pod_name"="heapster-jfcd9" and "label.k8s-app"="heapster" and "cluster"="k8s-cluster") 1531946700000,20811776
    * heapster.pod_container.memory.usage k8s-slave-2 ("container_name"="kube-proxy" and "label.pod-template-generation"="1" and "container_base_image"="k8s.gcr.io/kube-proxy-amd64:v1.11.0" and "label.controller-revision-hash"="1151982146" and "type"="pod_container" and "namespace_name"="kube-system" and "pod_name"="kube-proxy-8mnhr" and "nodename"="k8s-slave-2" and "label.k8s-app"="kube-proxy" and "cluster"="k8s-cluster") 1531946700000,12525568
    *
    * sys_container:
    * heapster.sys_container.memory.major_page_faults k8s-master ("cluster"="k8s-cluster" and "type"="sys_container" and "container_name"="docker-daemon" and "nodename"="k8s-master") 1531946700000,465
    * heapster.sys_container.memory.major_page_faults k8s-master ("cluster"="k8s-cluster" and "type"="sys_container" and "container_name"="kubelet" and "nodename"="k8s-master") 1531946700000,1
    * heapster.sys_container.memory.major_page_faults k8s-master ("cluster"="k8s-cluster" and "type"="sys_container" and "container_name"="pods" and "nodename"="k8s-master") 1531946700000,0
    * heapster.sys_container.memory.major_page_faults k8s-slave-1 ("cluster"="k8s-cluster" and "type"="sys_container" and "container_name"="docker-daemon" and "nodename"="k8s-slave-1") 1531946700000,472
    * heapster.sys_container.memory.major_page_faults k8s-slave-1 ("cluster"="k8s-cluster" and "type"="sys_container" and "container_name"="kubelet" and "nodename"="k8s-slave-1") 1531946700000,3
    * heapster.sys_container.memory.major_page_faults k8s-slave-1 ("cluster"="k8s-cluster" and "type"="sys_container" and "container_name"="pods" and "nodename"="k8s-slave-1") 1531946700000,0
    * heapster.sys_container.memory.major_page_faults k8s-slave-2 ("nodename"="k8s-slave-2" and "type"="sys_container" and "container_name"="docker-daemon" and "cluster"="k8s-cluster") 1531946700000,473
    * heapster.sys_container.memory.major_page_faults k8s-slave-2 ("nodename"="k8s-slave-2" and "type"="sys_container" and "container_name"="kubelet" and "cluster"="k8s-cluster") 1531946700000,0
    * heapster.sys_container.memory.major_page_faults k8s-slave-2 ("nodename"="k8s-slave-2" and "type"="sys_container" and "container_name"="pods" and "cluster"="k8s-cluster") 1531946700000,0
    */


    public static final List<ContainerMetricRange> baseMetrics = Arrays.asList(
            new ContainerMetricRange("cpu.limit",100.0,100.0),
            new ContainerMetricRange("cpu.request",100.0,100.0),
            new ContainerMetricRange("cpu.usage_rate",0.0,100.0),
            new ContainerMetricRange("memory.limit",524288000.0,524288000.0),
            new ContainerMetricRange("memory.request",209715200.0,524288000.0),
            new ContainerMetricRange("memory.usage",2045083648.0,2147483648.0)
    );

    public static final List<ContainerMetricRange> nodeMetrics = Arrays.asList(
            new ContainerMetricRange("cpu.limit", 0.0, 100.0),
            new ContainerMetricRange("cpu.node_allocatable", 4000.0, 4000.0),
            new ContainerMetricRange("cpu.node_capacity", 4000.0, 4000.0),
            new ContainerMetricRange("cpu.node_reservation", 0.0, 1.0),
            new ContainerMetricRange("cpu.node_utilization", 0.0, 1.0),
            new ContainerMetricRange("cpu.request", 1000.0, 1000.0),
            new ContainerMetricRange("cpu.usage", 30122621893292.0, -1.0), // linear increment
            new ContainerMetricRange("cpu.usage_rate", 40.0, 100.0),
            new ContainerMetricRange("filesystem.available", 68719476736.0, 68719476736.0),
            new ContainerMetricRange("filesystem.inodes", 13100000.0, 13100000.0),
            new ContainerMetricRange("filesystem.inodes_free", 11500000.0, 12800000.0),
            new ContainerMetricRange("filesystem.limit", 73014444032.0, 73014444032.0),
            new ContainerMetricRange("filesystem.usage", 5368709120.0, 59055800320.0),
            new ContainerMetricRange("memory.limit", 32212254720.0, 32212254720.0),
            new ContainerMetricRange("memory.major_page_faults", 0, 100),
            new ContainerMetricRange("memory.major_page_faults_rate", 0.0, 0.1),
            new ContainerMetricRange("memory.node_allocatable", 68719476736.0, 68719476736.0),
            new ContainerMetricRange("memory.node_capacity", 73014444032.0, 73014444032.0),
            new ContainerMetricRange("memory.node_reservation", 0.0, 0.5),
            new ContainerMetricRange("memory.node_utilization", 0.2, 0.9),
            new ContainerMetricRange("memory.page_faults", 2000.0, 5000.0),
            new ContainerMetricRange("memory.page_faults_rate", 1000.0, 1200.0),
            new ContainerMetricRange("memory.request", 31138512896.0, 31138512896.0),
            new ContainerMetricRange("memory.rss", 858993459.0, 1073741824.0),
            new ContainerMetricRange("memory.usage", 28991029248.0, 31138512896.0),
            new ContainerMetricRange("memory.working_set", 27991029248.0, 30138512896.0),
            new ContainerMetricRange("uptime", 0.0, -1.0) // linear increment
    );

    public static final List<ContainerMetricRange> podMetrics = Arrays.asList(
            new ContainerMetricRange("cpu.limit", 0.0, 100.0),
            new ContainerMetricRange("cpu.request", 1000.0, 1000.0),
            new ContainerMetricRange("cpu.usage_rate", 40.0, 100.0),
            new ContainerMetricRange("filesystem.available", 68719476736.0, 68719476736.0),
            new ContainerMetricRange("filesystem.inodes", 13100000.0, 13100000.0),
            new ContainerMetricRange("filesystem.inodes_free", 11500000.0, 12800000.0),
            new ContainerMetricRange("filesystem.limit", 73014444032.0, 73014444032.0),
            new ContainerMetricRange("filesystem.usage", 5368709120.0, 59055800320.0),
            new ContainerMetricRange("memory.limit", 32212254720.0, 32212254720.0),
            new ContainerMetricRange("memory.major_page_faults_rate", 0.0, 0.1),
            new ContainerMetricRange("memory.page_faults_rate", 1000.0, 1200.0),
            new ContainerMetricRange("memory.request", 31138512896.0, 31138512896.0),
            new ContainerMetricRange("memory.rss", 858993459.0, 1073741824.0),
            new ContainerMetricRange("memory.usage", 28991029248.0, 31138512896.0),
            new ContainerMetricRange("memory.working_set", 27991029248.0, 30138512896.0),
            new ContainerMetricRange("network.rx", 350000000000.0, 370000000000.0),
            new ContainerMetricRange("network.rx_errors", 0.0, 2.0),
            new ContainerMetricRange("network.rx_errors_rate", 0.0, 2.0),
            new ContainerMetricRange("network.rx_rate", 140000.0, 160000.0),
            new ContainerMetricRange("network.tx", 450000000000.0, 470000000000.0),
            new ContainerMetricRange("network.tx_errors", 0.0, 2.0),
            new ContainerMetricRange("network.tx_errors_rate", 0.0, 2.0),
            new ContainerMetricRange("network.tx_rate", 180000.0, 200000.0),
            new ContainerMetricRange("restart_count", 0.0, 2.0),
            new ContainerMetricRange("uptime", 0.0, -1.0) // linear increment
    );

    public static final List<ContainerMetricRange> sysContainerMetrics = Arrays.asList(
            new ContainerMetricRange("cpu.usage", 30122621893292.0, -1.0), // linear increment
            new ContainerMetricRange("cpu.usage_rate", 40.0, 100.0),
            new ContainerMetricRange("memory.cache", 3000000.0, 3000000.0),
            new ContainerMetricRange("memory.major_page_faults", 0, 100),
            new ContainerMetricRange("memory.major_page_faults_rate", 0.0, 0.1),
            new ContainerMetricRange("memory.page_faults", 2000.0, 5000.0),
            new ContainerMetricRange("memory.page_faults_rate", 50.0, 120.0),
            new ContainerMetricRange("memory.rss", 858993459.0, 1073741824.0),
            new ContainerMetricRange("memory.usage", 1610612736.0, 2147483648.0),
            new ContainerMetricRange("memory.working_set", 1073741824.0, 1610612736.0),
            new ContainerMetricRange("uptime", 0.0, -1.0) // linear increment
    );

    public static final List<ContainerMetricRange> podContainerMetrics = Arrays.asList(
            new ContainerMetricRange("cpu.limit", 0.0, 100.0),
            new ContainerMetricRange("cpu.request", 1000.0, 1000.0),
            new ContainerMetricRange("cpu.usage", 30122621893292.0, -1.0), // linear increment
            new ContainerMetricRange("cpu.usage_rate", 40.0, 100.0),
            new ContainerMetricRange("filesystem.available", 68719476736.0, 68719476736.0),
            new ContainerMetricRange("filesystem.inodes", 13100000.0, 13100000.0),
            new ContainerMetricRange("filesystem.inodes_free", 11500000.0, 12800000.0),
            new ContainerMetricRange("filesystem.limit", 73014444032.0, 73014444032.0),
            new ContainerMetricRange("filesystem.usage", 5368709120.0, 59055800320.0),
            new ContainerMetricRange("memory.limit", 32212254720.0, 32212254720.0),
            new ContainerMetricRange("memory.major_page_faults_rate", 0.0, 0.1),
            new ContainerMetricRange("memory.page_faults_rate", 1000.0, 1200.0),
            new ContainerMetricRange("memory.request", 31138512896.0, 31138512896.0),
            new ContainerMetricRange("memory.rss", 858993459.0, 1073741824.0),
            new ContainerMetricRange("memory.usage", 28991029248.0, 31138512896.0),
            new ContainerMetricRange("memory.working_set", 27991029248.0, 30138512896.0),
            new ContainerMetricRange("restart_count", 0.0, 2.0),
            new ContainerMetricRange("uptime", 0.0, -1.0) // linear increment
    );

    // TODO, add tags automatically.
    public static final List<String> tagsCluster = Stream.of("type", "cluster").collect(Collectors.toList());
    public static final List<String> tagsNS = Stream.of("type", "cluster", "namespace_name").collect(Collectors.toList());
    public static final List<String> tagsNode = Stream.of("type", "cluster", "nodename", "label.beta.kubernetes.io/os", "label.kubernetes.io/hostname", "label.beta.kubernetes.io/arch").collect(Collectors.toList());
    public static final List<String> tagsPod = Stream.of("type", "cluster", "nodename", "namespace_name", "pod_name", "label.pod-template-hash", "label.k8s-app").collect(Collectors.toList());
    public static final List<String> tagsPodContainer = Stream.of("type", "cluster", "nodename", "namespace_name", "pod_name", "label.controller-revision-hash", "label.k8s-app", "container_name", "container_base_image").collect(Collectors.toList());
    public static final List<String> tagsSysContainer = Stream.of("type", "cluster", "nodename", "container_name").collect(Collectors.toList());
}