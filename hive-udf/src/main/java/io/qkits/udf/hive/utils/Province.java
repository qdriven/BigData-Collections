package io.qkits.udf.hive.utils;

import java.util.List;

public class Province {
    private int code;
    private String region;
    private List<Province> regionEntitys;

    public Province(int code, String region) {
        this.code = code;
        this.region = region;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public List<Province> getRegionEntitys() {
        return regionEntitys;
    }

    public void setRegionEntitys(List<Province> regionEntitys) {
        this.regionEntitys = regionEntitys;
    }
}
