package com.b5m.plugin.util;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class SerializerUtils {
	private final static String OBJECT_TYPE_FALG = "@type";

	public static boolean isObject(String value) {
		if (value.indexOf(OBJECT_TYPE_FALG) > 0) {
			return true;
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	public static <T extends Object> T Deserialization(String stringObj) {
		if (StringUtils.isBlank(stringObj)) {
			return null;
		}
		//！isObject(stringObj)，消息内容为Date、List的简单对象无法正常反序列化
		//须将Date、List等作为自定义bean的属性，这样方可正常反序列化
		if (!isObject(stringObj))
			return (T) stringObj;
		Object v = JSON.parse(stringObj);
		if (v == null)
			return null;
		return (T) v;
	}

	public static <T extends Object> String Serialization(T obj) {
		if (obj == null) {
			return null;
		}
		String v = null;

		if (obj instanceof String) {
			v = (String) obj;
		} else {
			v = JSON.toJSONString(obj, SerializerFeature.WriteClassName);
		}
		return v;
	}
}
