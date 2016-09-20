/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.custom;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Primitive;

import org.elasticsearch.search.SearchHit;
import org.json.JSONObject;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Enumerator that reads from an Elasticsearch type.
 */
public class CustomSearchEnumerator implements Enumerator<Object> {
  private final Iterator<Object> cursor;
  private final Function1<JSONObject, Object> getter;
  private Object current;

  /**
   * Creates an ElasticsearchEnumerator.
   *
   * @param cursor Iterator over Elasticsearch {@link SearchHit} objects
   * @param getter2 Converts an object into a list of fields
   */
  public CustomSearchEnumerator(Iterator<Object> cursor, Function1<JSONObject, Object> getter2) {
    this.cursor = cursor;
    this.getter = getter2;
  }

  public Object current() {
    return current;
  }

  public boolean moveNext() {
    if (cursor.hasNext()) {
      Object map = cursor.next();
      current = getter.apply((JSONObject) map);
      return true;
    } else {
      current = null;
      return false;
    }
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  public void close() {
    // nothing to do
  }

  private static Function1<JSONObject, Set> mapGetter() {
    return new Function1<JSONObject, Set>() {
      public Set apply(JSONObject jsonObject) {
        return jsonObject.keySet();
      }
    };
  }

  private static Function1<JSONObject, Object> singletonGetter(final String fieldName,
      final Class fieldClass) {
    return new Function1<JSONObject, Object>() {
      public Object apply(JSONObject jsonObject) {
          return convert(jsonObject, fieldClass);

      }
    };
  }

  /**
   * Function that extracts a given set of fields from {@link SearchHit}
   * objects.
   *
   * @param fields List of fields to project
   */
  private static Function1<SearchHit, Object[]> listGetter(
      final List<Map.Entry<String, Class>> fields) {
    return new Function1<SearchHit, Object[]>() {
      public Object[] apply(SearchHit searchHitFields) {
        Object[] objects = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          final Map.Entry<String, Class> field = fields.get(i);
          final String name = field.getKey();
          if (searchHitFields.fields().isEmpty()) {
            objects[i] = convert(searchHitFields.getSource().get(name), field.getValue());
          } else {
            objects[i] = convert(searchHitFields.field(name).getValue(), field.getValue());
          }
        }
        return objects;
      }
    };
  }

  static Function1<JSONObject, Object> getter(List<Map.Entry<String, Class>> fields) {
    //noinspection unchecked
    return fields == null
      ? (Function1) mapGetter()
      : fields.size() == 1
      ? singletonGetter(fields.get(0).getKey(), fields.get(0).getValue())
      : (Function1) listGetter(fields);
  }

  private static Object convert(Object o, Class clazz) {
    if (o == null) {
      return null;
    }
    Primitive primitive = Primitive.of(clazz);
    if (primitive != null) {
      clazz = primitive.boxClass;
    } else {
      primitive = Primitive.ofBox(clazz);
    }
    if (clazz.isInstance(o)) {
      return o;
    }
    if (o instanceof Date && primitive != null) {
      o = ((Date) o).getTime() / DateTimeUtils.MILLIS_PER_DAY;
    }
    if (o instanceof Number && primitive != null) {
      return primitive.number((Number) o);
    }
    return o;
  }
}

// End ElasticsearchEnumerator.java
