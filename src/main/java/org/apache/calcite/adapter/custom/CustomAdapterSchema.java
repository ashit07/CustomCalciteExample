package org.apache.calcite.adapter.custom;

import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.sun.jersey.api.client.Client;


public class CustomAdapterSchema extends AbstractSchema {
  public static final String DAS_TABLE = "das_table";
  public static final String DAS_SCHEMA = "das_schema";
  private final String url;
  private final String username;
  private final String password;

  private transient Client client;


  CustomAdapterSchema(String url, String username, String password) {
    super();
    this.url = url;
    this.username=username;
    this.password=password;
  }

  @Override protected Map<String, Table> getTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

    try {
      builder.put(DAS_TABLE, new CustomsearchTable(url, DAS_TABLE));

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return builder.build();
  }
}

// End CustomAdapterSchema.java
