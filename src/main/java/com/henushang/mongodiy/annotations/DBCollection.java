package com.henushang.mongodiy.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 可以通过在类上添加该注解来指定数据库中相对应的collection的名字，如果不指定，则默认使用
 * 类名的全部小写作为collection名。
 * @author henushang
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DBCollection {

	/**
	 * 数据库中的collection名字
	 */
	String name() default "";
}
