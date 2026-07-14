package org.minicache.common;

public class Message {
    private Command command;
    private String key;
    private String value;
    private Long ttl;
    private Boolean notExists;
    private Integer bloomFilterExpectedElements;
    private Double bloomFilterFalsePositiveRate;
    private Double zsScore;
    private String zsMember;
    private Integer zsIdx;
    private Integer zsStartIdx;
    private Integer zsStopIdx;
    private Double zsStartScr;
    private Double zsStopScr;

    public Command getCommand() {
        return command;
    }

    public void setCommand(Command command) {
        this.command = command;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Long getTtl() {
        return this.ttl;
    }

    public void setTtl(Long ttl) {
        this.ttl = ttl;
    }

    public Boolean getNotExists() {
        return notExists;
    }

    public void setNotExists(Boolean notExists) {
        this.notExists = notExists;
    }

    public Integer getBloomFilterExpectedElements() {
        return bloomFilterExpectedElements;
    }

    public void setBloomFilterExpectedElements(Integer bloomFilterExpectedElements) {
        this.bloomFilterExpectedElements = bloomFilterExpectedElements;
    }

    public Double getBloomFilterFalsePositiveRate() {
        return bloomFilterFalsePositiveRate;
    }

    public void setBloomFilterFalsePositiveRate(Double bloomFilterFalsePositiveRate) {
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
    }

    public Double getZsScore() {
        return zsScore;
    }

    public void setZsScore(Double zsScore) {
        this.zsScore = zsScore;
    }

    public String getZsMember() {
        return zsMember;
    }

    public void setZsMember(String zsMember) {
        this.zsMember = zsMember;
    }

    public Integer getZsIdx() {
        return zsIdx;
    }

    public void setZsIdx(Integer zsIdx) {
        this.zsIdx = zsIdx;
    }

    public Integer getZsStartIdx() {
        return zsStartIdx;
    }

    public void setZsStartIdx(Integer zsStartIdx) {
        this.zsStartIdx = zsStartIdx;
    }

    public Integer getZsStopIdx() {
        return zsStopIdx;
    }

    public void setZsStopIdx(Integer zsStopIdx) {
        this.zsStopIdx = zsStopIdx;
    }

    public Double getZsStartScr() {
        return zsStartScr;
    }

    public void setZsStartScr(Double zsStartScr) {
        this.zsStartScr = zsStartScr;
    }

    public Double getZsStopScr() {
        return zsStopScr;
    }

    public void setZsStopScr(Double zsStopScr) {
        this.zsStopScr = zsStopScr;
    }
}
