package com.henushang.mongodiy.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 对于那些不想被存进数据库的数据，可以使用该注解进行“忽略”。如果在类上使用此注解，则代表不进行存储
 * 该类；如果在方法上使用此注解，则表明不存储改方法所“对应”的字段。
 * 
 * @author ShangJianguo
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = {ElementType.TYPE, ElementType.METHOD})
public @interface Ignore {
}
