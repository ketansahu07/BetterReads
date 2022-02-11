package com.betterreads.betterreadsdataloader.author;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Data
@NoArgsConstructor
@Table(value = "author_by_id")
public class Author {
  @Id
  @PrimaryKeyColumn(name = "author_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
  private String id;

  @Column("author_name")
  @CassandraType(type = CassandraType.Name.TEXT)
  private String name;

  @Column("personal_name")
  @CassandraType(type = CassandraType.Name.TEXT)
  private String personalName;
}
