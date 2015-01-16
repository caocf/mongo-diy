

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import com.genius.core.base.constant.BaseMapperDict;
import com.genius.core.base.utils.MapsUtil;
import com.genius.core.base.utils.StrUtil;
import com.genius.xo.mongodb.Mongo;
import com.genius.xo.mongodb.dao.BaseMongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.QueryBuilder;

/**
 * MongoDB DAO 的基础实现类,基于MapsUtil来实现
 * 对象或map中的key或属性所有的uid或_id都会转成_id
 * 对应的实体类的属性类型不可为short，要与mongodb中相匹配
 * @param <T>
 * @author architect.bian
 * @createtime 2015-1-8 下午4:49:29
 */
public abstract class BaseMongoDaoImpl<T> extends BaseDaoImpl<T> implements BaseMongoDao<T>{
	
	private DBCollection coll = null;
	private String defaultStrategy = Mongo.defaultStrategy;
	
	/**
	 * 获取DBCollection
	 * @return
	 * @author Architect.bian
	 * @createtime 2015-1-9 下午6:14:00
	 */
	private DBCollection getColl(){
		if (coll == null) {
			Class<T> clazz = getTClass();
			Mongo mongoAnno = clazz.getAnnotation(Mongo.class);
			String collName = clazz.getSimpleName().toLowerCase();
			if (mongoAnno != null) {
				if (StrUtil.isNotEmpty(mongoAnno.collection())) {
					collName = mongoAnno.collection();
				}
//				if (mongoAnno.strategy().length == 1) {
//					defaultStrategy = mongoAnno.strategy()[0];// 设置strategy
//				}
			}
			coll = db.getCollection(collName);
		}
		return coll;
	}
	
	@Override
	public T get(String id) {
		return get(id, defaultStrategy);
	}
	
	@Override
	public T get(String id, String strategy) {
		BasicDBObject query = new BasicDBObject(DefaultCollectionID, id);
		DBObject dbObj = getColl().findOne(query);
		return result(dbObj, strategy);
	}

	@Override
	public T getOne(Map<String, Object> map) {
		return getOne(map, defaultStrategy);
	}
	
	@Override
	public T getOne(Map<String, Object> map, String strategy) {
		DBObject dbObj = getColl().findOne(getQuery(map));
		return result(dbObj, strategy);
	}

	@Override
	public int getCount(Map<String, Object> map) {
		return (int)getColl().getCount(getQuery(map));
	}

	@Override
	public List<T> getList(Map<String, Object> map) {
		return getList(map, defaultStrategy);
	}
	
	@Override
	public List<T> getList(Map<String, Object> map, String strategy) {
		int skip = 0;
		if (map.get(BaseMapperDict.startIndex) != null) {
			skip = Integer.parseInt(map.get(BaseMapperDict.startIndex).toString());
		}
		int limit = 10;
		if (map.get(BaseMapperDict.pageSize) != null) {
			limit = Integer.parseInt(map.get(BaseMapperDict.pageSize).toString());
		}
		DBObject orderby = getOrderDBObject(map);
		DBCursor cursor = getColl().find(getQuery(map)).skip(skip).limit(limit).sort(orderby);
		return result(cursor, strategy);
	}

	@Override
	public boolean insert(T t) {
		return insert(t, defaultStrategy);
	}
	
	@Override
	public boolean insert(T t, String strategy) {
		try {
			getColl().insert(new BasicDBObject(MapsUtil.toMap(t, strategy)));
			return true;
		} catch (MongoException e) {
			return false;
		}
	}

	@Override
	public boolean insertList(List<T> list) {
		return insertList(list, defaultStrategy);
	}
	
	@Override
	public boolean insertList(List<T> list, String strategy) {
		try {
			List<DBObject> all = new ArrayList<>();
			for (T t : list) {
				all.add(new BasicDBObject(MapsUtil.toMap(t, strategy)));
			}
			getColl().insert((DBObject[]) all.toArray());
			return true;
		} catch (MongoException e) {
			return false;
		}
	}

	@Override
	public boolean update(T t) {
		return update(t, defaultStrategy);
	}
	
	@Override
	public boolean update(T t, String strategy) {
		try {
			getColl().update(BasicDBObjectBuilder.start(DefaultCollectionID, getID(t)).get(), new BasicDBObject(MapsUtil.toMap(t, strategy)));
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	@Override
	public boolean updateFields(Map<String, Object> map) {
		Map<String, Object> m = filterMap(map);
		String id = (String) m.get(DefaultCollectionID);
		m.remove(DefaultCollectionID);
		BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
		for (String key : m.keySet()) {
			builder.add(key, getValue(m.get(key)));
		}
		try {
			DBObject dbObj = new BasicDBObject("$set", builder.get());
			getColl().update(BasicDBObjectBuilder.start(DefaultCollectionID, id).get(), dbObj);
			return true;
		} catch (MongoException e) {
			return false;
		}
	}

	@Override
	public boolean increase(Map<String, Object> map) {
		Map<String, Object> m = filterMap(map);
		String id = (String) m.get(DefaultCollectionID);
		m.remove(DefaultCollectionID);
		BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
		for (String key : m.keySet()) {
			builder.add(key, Integer.valueOf(m.get(key).toString()));
		}
		DBObject dbObj = new BasicDBObject("$inc", builder.get());
		getColl().update(BasicDBObjectBuilder.start(DefaultCollectionID, id).get(), dbObj);
		return true;
	}

	@Override
	public boolean delete(String id) {
		getColl().remove(new BasicDBObject(DefaultCollectionID, id));
		return true;
	}

	@Override
	public boolean deleteByMap(Map<String, Object> map) {
		getColl().remove(getQuery(map));
		return true;
	}

	/**
	 * 根据传来的map组装成可以在mongodb中查询的条件，具体的前后缀请参考 {@link com.genius.core.base.constant.BaseMapperDict}
	 * @param map
	 * @return
	 * @author Architect.bian
	 * @createtime 2015-1-9 下午7:09:50
	 */
	private DBObject getQuery(Map<String, Object> map){
		QueryBuilder query = QueryBuilder.start();
		map = filterMap(map);
		Iterator<String> iterator = map.keySet().iterator();
		while (iterator.hasNext()) {
			String key = (String) iterator.next();
			Object value = getValue(map.get(key));
			if (key.endsWith(BaseMapperDict.greaterOrEqual_key_suffix)) {
				String field = getRealField(key, false, BaseMapperDict.greaterOrEqual_key_suffix);
				query.put(field).greaterThanEquals(value);
				continue;
			}
			if (key.endsWith(BaseMapperDict.greater_key_suffix)) {
				String field = getRealField(key, false, BaseMapperDict.greater_key_suffix);
				query.put(field).greaterThan(value);
				continue;
			}
			if (key.endsWith(BaseMapperDict.lessOrEqual_key_suffix)) {
				String field = getRealField(key, false, BaseMapperDict.lessOrEqual_key_suffix);
				query.put(field).lessThanEquals(value);
				continue;
			}
			if (key.endsWith(BaseMapperDict.less_key_suffix)) {
				String field = getRealField(key, false, BaseMapperDict.less_key_suffix);
				query.put(field).lessThan(value);
				continue;
			}
			if (key.endsWith(BaseMapperDict.like_key_suffix)) {
				String field = getRealField(key, false, BaseMapperDict.like_key_suffix);
				query.put(field).regex(Pattern.compile((String) value));
				continue;
			}
			if (key.equals(BaseMapperDict.ids)) {
				query.put(DefaultCollectionID).in(value);
				continue;
			}
			if (key.endsWith(BaseMapperDict.in_key_suffix)) {
				query.put(getRealField(key, false, BaseMapperDict.in_key_suffix)).in(map.get(key));
				continue;
			}
			query.put(key).is(value);
		}
		return query.get();
	}

	/**
	 * 处理map中的特殊类型，转化为基本类型，比如枚举 jodatime等
	 * @param value
	 * @return
	 * @author Architect.bian
	 * @createtime 2015-1-12 下午12:43:15
	 */
	private Object getValue(Object value) {
		if (value != null) {
			if (value instanceof DateTime) {
				value = ((DateTime)value).getMillis();
			} else if (value instanceof LocalDate) {
				value = ((LocalDate) value).toDateTimeAtStartOfDay().toDate().getTime();
			} else if (value instanceof LocalTime) {
				value = ((LocalTime) value).getMillisOfDay();
			} else if (value.getClass().isEnum()) {
				value = value.toString();
			}
		}
		return value;
	}
	
	/**
	 * 将dbobject转成对象
	 * @param dbObj
	 * @return
	 * @author Architect.bian
	 * @createtime 2015-1-9 下午7:22:09
	 */
	@SuppressWarnings("unchecked")
	protected T result(DBObject dbObj, String strategy) {
		if (dbObj == null) {
			return null;
		}
		return (T) MapsUtil.fromMap(dbObj.toMap(), getTClass(), strategy);
//		return (T) MapsUtil.fromMap(toMap(dbObj), getTClass(), strategy);
	}
	/*
	@SuppressWarnings("unchecked")
	private Map<String, Object> toMap(DBObject dbObj) {
		Map<String, Object> map = dbObj.toMap();
		for (String key : map.keySet()) {
			Object val = map.get(key);
			if (val instanceof BasicDBList) {
				map.put(key, toList((BasicBSONList)val));
			} else if (val instanceof DBObject) {
				map.put(key, toMap((DBObject)val));
			}
		}
		return map;
	}

	private List<Object> toList(BasicBSONList val) {
		List<Object> list = new ArrayList<>();
		for (Object item : val) {
			if (item instanceof DBObject) {
				list.add(toMap((DBObject)item));
			} else {
				list.add(item);
			}
		}
		return list;
	}
*/
	/**
	 * 将dbcursor转成arraylist
	 * @param cursor
	 * @return
	 * @author Architect.bian
	 * @createtime 2015-1-9 下午7:22:27
	 */
	@SuppressWarnings("unchecked")
	private List<T> result(DBCursor cursor, String strategy) {
		if (cursor == null) {
			return null;
		}
		List<T> list = new ArrayList<>();
		if (cursor != null) {
			List<DBObject> tmpList = cursor.toArray();
			for (DBObject item : tmpList) {
				list.add((T) MapsUtil.fromMap(item.toMap(), getTClass(), strategy));
			}
		}
		return list;
	}

}
