package com.baidu.bce.es.engine;

/**
 * SearchType.
 *
 * @author Xiao Liu (liuxiao14@baidu.com)
 */
public enum SearchType {

    TERM("term"),
    MATCH("match");

    private String value;
    
    SearchType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static SearchType parse(String value) {
        for (SearchType modelType : SearchType.values()) {
            if (modelType.value.equals(value)) {
                return modelType;
            }
        }
        return null;
    }
}
