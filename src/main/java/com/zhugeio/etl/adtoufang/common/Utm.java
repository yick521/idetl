package com.zhugeio.etl.adtoufang.common;

public class Utm {
    /**
     * 活动名称
     */
    public String utm_campaign;
    /**
     * 广告来源
     */
    public String utm_source;
    /**
     * 广告媒介
     */
    public String utm_medium;
    /**
     * key word
     */
    public String utm_term;
    /**
     * 广告内容
     */
    public String utm_content;

    public String getUtm_source() {
        return utm_source;
    }

    public void setUtm_source(String utm_source) {
        this.utm_source = utm_source;
    }

    public String getUtm_medium() {
        return utm_medium;
    }

    public void setUtm_medium(String utm_medium) {
        this.utm_medium = utm_medium;
    }

    public String getUtm_term() {
        return utm_term;
    }

    public void setUtm_term(String utm_term) {
        this.utm_term = utm_term;
    }

    public String getUtm_content() {
        return utm_content;
    }

    public void setUtm_content(String utm_content) {
        this.utm_content = utm_content;
    }

    public String getUtm_campaign() {
        return utm_campaign;
    }

    public void setUtm_campaign(String utm_campaign) {
        this.utm_campaign = utm_campaign;
    }

    public String toJsonString(){
      return   "{\"utm_campaign\":\""+utm_campaign+"\",\"utm_source\":\""+utm_source+"\",\"utm_medium\":\""+utm_medium+"\",\"utm_term\":\""+utm_term+"\",\"utm_content\":\""+utm_content+"\"}";
    }

}
