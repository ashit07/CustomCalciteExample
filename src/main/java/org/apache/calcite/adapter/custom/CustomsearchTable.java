package org.apache.calcite.adapter.custom;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.calcite.adapter.custom.json.JsonHelper;
import org.apache.calcite.adapter.custom.rest.RestClient;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.json.JSONObject;

/**
 * Table based on an Elasticsearch type.
 */
public class CustomsearchTable extends AbstractQueryableTable implements TranslatableTable {
  private final String url;
  private final String table;
  /**
   * Creates an CustomSearchTable.
   */
  public CustomsearchTable(String url, String table) {
    super(Object[].class);
    this.url = url;
    this.table = table;
  }

  @Override public String toString() {
    return "CustomSearchTable { }";
  }

  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    final RelDataType mapType = relDataTypeFactory.createMapType(
        relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
        relDataTypeFactory.createTypeWithNullability(
            relDataTypeFactory.createSqlType(SqlTypeName.ANY),
            true));
    return relDataTypeFactory.builder().add("_MAP", mapType).build();
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema,
      String tableName) {
    return new ElasticsearchQueryable<>(queryProvider, schema, this, tableName);
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new ElasticsearchTableScan(cluster, cluster.traitSetOf(ElasticsearchRel.CONVENTION),
        relOptTable, this, null);
  }

  /** Executes a "find" operation on the underlying type.
   *
   * <p>For example,
   * <code>client.prepareSearch(index).setTypes(type)
   * .setSource("{\"fields\" : [\"state\"]}")</code></p>
   *
   * @param index Elasticsearch index
   * @param ops List of operations represented as Json strings.
   * @param fields List of fields to project; or null to return map
   * @return Enumerator of results
   */
  private Enumerable<Object> find(String index, List<String> ops,
      List<Map.Entry<String, Class>> fields) {
    final String dbName = index;

    final String queryString =  Util.toString(ops, "", "/", "/");

    final Function1<JSONObject, Object> getter = CustomSearchEnumerator.getter(fields);

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        final Iterator<Object> cursor = JsonHelper.getJsonArr(new RestClient().getJsonData(url), "");
        return new CustomSearchEnumerator(cursor, getter);
      }
    };
  }

  /**
   * Implementation of {@link org.apache.calcite.linq4j.Queryable} based on
   * a {@link org.apache.calcite.adapter.CustomsearchTable.ElasticsearchTable}.
   */
  public static class ElasticsearchQueryable<T> extends AbstractTableQueryable<T> {
    public ElasticsearchQueryable(QueryProvider queryProvider, SchemaPlus schema,
        CustomsearchTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      return null;
    }

    private String getIndex() {
      return schema.unwrap(CustomAdapterSchema.class).DAS_SCHEMA;
    }


    private CustomsearchTable getTable() {
      return (CustomsearchTable) table;
    }

    /** Called via code-generation.
     *
     * @see org.apache.calcite.adapter.elasticsearch.ElasticsearchMethod#ELASTICSEARCH_QUERYABLE_FIND
     */
//    @SuppressWarnings("UnusedDeclaration")
//    public Enumerable<Object> find(List<String> ops,
//        List<Map.Entry<String, Class>> fields) {
//      return getTable().find(getIndex(), ops, fields);
//    }
  }
}

// End ElasticsearchTable.java
