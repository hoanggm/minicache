package org.minicache.common;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadSheddingCfg {
    private final AtomicInteger activeRequests = new AtomicInteger(0);
    private final int maxConcurrentRequests;
    private final double maxMemoryThresholdRatio;
    private final double maxCpuThresholdRatio;
    private final OperatingSystemMXBean osBean;

    public LoadSheddingCfg(double maxMemoryThresholdRatio, double maxCpuThresholdRatio) {
        int cpuCores = Runtime.getRuntime().availableProcessors();
        long maxHeapBytes = Runtime.getRuntime().maxMemory();

        // 1. Tính theo CPU: Tối đa 100 concurrent reqs / core cho tác vụ hỗn hợp
        int cpuBoundLimit = cpuCores * 100;
        // 2. Tính theo RAM: Dành 10% RAM cho active requests, giả định 32KB / request
        long safeMemoryForActiveReqs = (long) (maxHeapBytes * 0.10);
        int memoryBoundLimit = (int) (safeMemoryForActiveReqs / (32 * 1024));
        // 3. Lấy giá trị an toàn nhỏ hơn giữa CPU Limit và Memory Limit
        this.maxConcurrentRequests = Math.min(cpuBoundLimit, memoryBoundLimit);

        this.maxMemoryThresholdRatio = maxMemoryThresholdRatio;
        this.maxCpuThresholdRatio = maxCpuThresholdRatio;
        this.osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    }

    public boolean shouldShed() {
        if (activeRequests.get() >= maxConcurrentRequests) {
            return true;
        }

        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        double memoryUsage = (double) usedMemory / maxMemory;

        if (memoryUsage >= maxMemoryThresholdRatio) {
            return true;
        }

        double systemCpuLoad = osBean.getCpuLoad();
        return (systemCpuLoad >= 0 && systemCpuLoad >= maxCpuThresholdRatio);
    }

    public void incrementActive() {
        activeRequests.incrementAndGet();
    }

    public void decrementActive() {
        activeRequests.decrementAndGet();
    }

    public int getActiveRequests() {
        return activeRequests.get();
    }

    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    public double getMaxMemoryThresholdRatio() {
        return maxMemoryThresholdRatio;
    }

    public double getMaxCpuThresholdRatio() {
        return maxCpuThresholdRatio;
    }
}
