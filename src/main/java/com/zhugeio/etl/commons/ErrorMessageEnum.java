package com.zhugeio.etl.commons;


public enum ErrorMessageEnum {

    JSON_FORMAT_ERROR(1020001, "json解析异常", "校验pay_statisv2主题的数据是否符合json格式"),
    BASIC_SCHEMA_FORMAT_NOT_MATCH(1020002, "转换后的json与basicSchema标准不符", "json中的要字段不存在或json结构有误，详见basicSchema文档"),
    AK_NONE(1020003, "ak在应用管理中不存在或已被删除", "ak在company_app表不存在或已经被标记删除"),
    DID_NONE(1020004, "did获取异常", "did值为空或不存在"),
    EID_NONE(1020005, "$eid获取异常", "$eid值为空或不存在"),
    MKT_SEND_ZG_ID_NONE(1020006, "触达事件获取zg_id异常", "did按'-'切割出的第2个数据非数值型"),
    EVENT_NAME_LENGTH_LIMIT(1020007, "事件名称长度超过限制", "事件名称长度超过限制128个字符"),
    MKT_EVENT_NOT_SPECIFIED(1020008, "触达事件未被指定", "owner= 'mkt'表明是触达事件，该事件在mysql表etl_sdk_config中sdk_key='mktEvents'条件下不存在"),
    BUILTIN_EVENT_NOT_SPECIFIED(1020009, "内置事件未被指定", "owner= 'abp'表明是内置事件，该事件在mysql表etl_sdk_config中sdk_key='abpEvents'条件下不存在"),
    EVENT_NUMBER_LIMIT(1020010, "事件个数超限制或自定义事件已禁用自动创建", "该应用在用的事件id个数超过mysql表company_app设置的值event_sum或者auto_event字段被设置为0"),
    EVENT_ATTR_ID_ERROR(1020011, "事件属性id生成失败", "应用中事件的属性个数超过mysql表company_app表设置的值attr_sum 或者auto_event字段被设置为0"),
    EVENT_TYPE_ERROR(1020012, "事件类型dt错误", "事件类型dt错误"),
    EVENT_NAME_INVALID(1020013, "事件名不合法", "事件名包含了除数字、字母、下划线、中划线、汉字、$以外字符"),
    EVENT_ATTR_INVALID(1020014, "事件属性不合法", "事件属性包含了除数字、字母、下划线、中划线、汉字、$以外字符"),
    VIRTUAL_ATTR_FIELD(1020014, "虚拟属性处理失败", "虚拟属性处理失败"),
    NONE_ERROR(0000000, "无异常", "无异常，供代码处理使用");


    private final int errorCode;
    private final String errorMessage;
    private final String errorRuleDescribe;

    ErrorMessageEnum(int errorCode, String errorMessage, String errorRuleDescribe) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.errorRuleDescribe = errorRuleDescribe;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getErrorRuleDescribe() {
        return errorRuleDescribe;
    }

}
