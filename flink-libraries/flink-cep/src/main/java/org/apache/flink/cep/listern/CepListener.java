package org.apache.flink.cep.listern;

import org.apache.flink.cep.pattern.Pattern;

import java.io.Serializable;

public interface CepListener<T> extends Serializable {

	/**
	 * @Description: 留给用户判断当接受到元素的时候，是否需要更新CEP逻辑
	 * @param: []
	 * @return: java.lang.Boolean
	 * @auther: greenday
	 * @date: 2019/9/7 11:22
	 */
	Boolean needChange(T element);
	/**
	 * @Description: 当needChange为true时会被调用，留给用户实现返回一个新逻辑生成的pattern对象
	 * @param: []
	 * @return: org.apache.flink.cep.pattern.Pattern
	 * @auther: greenday
	 * @date: 2019/9/7 10:46
	 */
	Pattern<T,?> returnPattern();
}
