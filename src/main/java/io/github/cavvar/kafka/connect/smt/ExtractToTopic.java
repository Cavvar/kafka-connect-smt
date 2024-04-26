/**
 * Copyright © 2024 @Cavvar
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.cavvar.kafka.connect.smt;

import java.util.Map;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;

public abstract class ExtractToTopic<R extends ConnectRecord<R>>
    implements Transformation<R>, Versioned {

  private static final int MAX_LRU_CACHE_SIZE = 16;

  public static final ConfigDef CONFIG_DEF = new ConfigDef();

  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public R apply(R connectRecord) {
    return connectRecord;
  }

  private R applySchemaless(R connectRecord) {
    return connectRecord;
  }

  private R applyWithSchema(R connectRecord) {
    return connectRecord;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(MAX_LRU_CACHE_SIZE));
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  protected abstract Schema operatingSchema(R connectRecord);

  protected abstract Object operatingValue(R connectRecord);

  protected abstract R newRecord(R connectRecord, Schema updatedSchema, String newDestinationTopic);

  @Override
  public String version() {
    return AppInfoParser.getVersion();
  }

  public static class Key<R extends ConnectRecord<R>> extends ExtractToTopic<R> {
    @Override
    protected Schema operatingSchema(R connectRecord) {
      return connectRecord.keySchema();
    }

    @Override
    protected Object operatingValue(R connectRecord) {
      return connectRecord.key();
    }

    @Override
    protected R newRecord(R connectRecord, Schema updatedSchema, String newDestinationTopic) {
      return connectRecord.newRecord(
          newDestinationTopic,
          connectRecord.kafkaPartition(),
          connectRecord.keySchema(),
          connectRecord.key(),
          connectRecord.valueSchema(),
          connectRecord.value(),
          connectRecord.timestamp());
    }
  }

  public static class Value<R extends ConnectRecord<R>> extends ExtractToTopic<R> {
    @Override
    protected Schema operatingSchema(R connectRecord) {
      return connectRecord.valueSchema();
    }

    @Override
    protected Object operatingValue(R connectRecord) {
      return connectRecord.value();
    }

    @Override
    protected R newRecord(R connectRecord, Schema updatedSchema, String newDestinationTopic) {
      return connectRecord.newRecord(
          newDestinationTopic,
          connectRecord.kafkaPartition(),
          connectRecord.keySchema(),
          connectRecord.key(),
          connectRecord.valueSchema(),
          connectRecord.value(),
          connectRecord.timestamp());
    }
  }
}
