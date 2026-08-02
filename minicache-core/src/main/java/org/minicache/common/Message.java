package org.minicache.common;

public class Message {
    private Command command;
    private String key;
    private String value;
    private Long ttl;
    private Integer limit;
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
    private Double geoLat;
    private Double geoLon;
    private String geoMem;
    private String geoMem2;
    private Double geoRadius;
    private String hsField;

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

    public Double getGeoLat() {
        return geoLat;
    }

    public void setGeoLat(Double geoLat) {
        this.geoLat = geoLat;
    }

    public Double getGeoLon() {
        return geoLon;
    }

    public void setGeoLon(Double geoLon) {
        this.geoLon = geoLon;
    }

    public String getGeoMem() {
        return geoMem;
    }

    public void setGeoMem(String geoMem) {
        this.geoMem = geoMem;
    }

    public String getGeoMem2() {
        return geoMem2;
    }

    public void setGeoMem2(String geoMem2) {
        this.geoMem2 = geoMem2;
    }

    public Double getGeoRadius() {
        return geoRadius;
    }

    public void setGeoRadius(Double geoRadius) {
        this.geoRadius = geoRadius;
    }

    public Integer getLimit() {
        return limit;
    }

    public void setLimit(Integer limit) {
        this.limit = limit;
    }

    public String getHsField() {
        return hsField;
    }

    public void setHsField(String hsField) {
        this.hsField = hsField;
    }
}
