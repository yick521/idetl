package com.zhugeio.etl.adtoufang.common;

/**
 * @author zhangdongbin
 * Attribute data type
 */
public enum PropType {
    /**
     * 字符类型
     */
    CONT_STRING("1", "cont-string", "{columnName}", "string"),
    /**
     * 数字类型
     */
    CONT_NUM("2", "cont-num", "cast( {columnName} as DOUBLE)", "number"),
    /**
     * 日期类型
     */
//    CONT_DATE("3", "cont-date", "from_unixtime(cast({columnName} as int))", "string"),
    CONT_DATE("3", "cont-date", "{columnName}", "string"),
    /**
     * 其他类型
     */
    UN_KNOWN("-1", "其他", null, null);

	private String type;
	private String strValue;
    private String columnCast;
    // Value stored in field attr_data_type in table event_attr
    private String dataTypeValue;

    private final static String TYPE_COLUMN_SQLFORMAT = "(case when {typeColumnName}='{typeColumnValue}' then {castSQL} else null end) ";

    PropType(String type, String value, String columnCast, String dataTypeValue) {
		this.type=type;
		this.strValue = value;
        this.columnCast = columnCast;
        this.dataTypeValue = dataTypeValue;
	}

	public String value() {
		return this.type;
	}

	public String strValue(){
		return this.strValue;
	}

	public int intValue() {
		return Integer.valueOf(this.type).intValue();
	}

    public String getColumnCast() {
        return columnCast;
    }

    public String dataTypeValue() {
        return dataTypeValue;
    }

    public static PropType getType(String type) {
		for(PropType propType:PropType.values()){
			if(propType.name().equals(type) || propType.name().equals(type.toUpperCase()) || propType.value().equals(type)){
				return propType;
			}else if(propType.strValue().equals(type) || propType.strValue().equals(type.toUpperCase()) ){
				return propType;
			}
		}
		return null;
	}

    public static PropType propTypeOf(int type) {
        for (PropType propType : PropType.values()) {
            if (propType.intValue() == type) {
                return propType;
            }
        }
        return UN_KNOWN;
    }

}
