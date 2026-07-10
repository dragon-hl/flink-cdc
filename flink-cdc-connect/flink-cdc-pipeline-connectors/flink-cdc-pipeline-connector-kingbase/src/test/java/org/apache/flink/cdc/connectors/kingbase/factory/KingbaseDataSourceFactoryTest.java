/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.kingbase.factory;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.connectors.kingbase.source.KingbaseDataSourceOptions;
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KingbaseDataSourceFactory}. */
public class KingbaseDataSourceFactoryTest {

    private static final String TEST_HOST = "127.0.0.1";
    private static final int TEST_PORT = 54321;
    private static final String TEST_USER = "testuser";
    private static final String TEST_PASSWORD = "testpassword";
    private static final String TEST_SLOT = "test_slot";

    @Test
    public void testIdentifier() {
        KingbaseDataSourceFactory factory = new KingbaseDataSourceFactory();
        assertThat(factory.identifier()).isEqualTo("kingbase");
    }

    @Test
    public void testRequiredOptions() {
        KingbaseDataSourceFactory factory = new KingbaseDataSourceFactory();
        assertThat(factory.requiredOptions()).isNotEmpty();
    }

    @Test
    public void testOptionalOptions() {
        KingbaseDataSourceFactory factory = new KingbaseDataSourceFactory();
        assertThat(factory.optionalOptions()).contains(KingbaseDataSourceOptions.PG_PORT);
        assertThat(factory.optionalOptions()).contains(KingbaseDataSourceOptions.DECODING_PLUGIN_NAME);
    }

    @Test
    public void testDecodingPluginDefaultIsDecoderbufs() {
        assertThat(KingbaseDataSourceOptions.DECODING_PLUGIN_NAME.defaultValue())
                .isEqualTo("decoderbufs");
    }

    @Test
    public void testPortDefault() {
        assertThat(KingbaseDataSourceOptions.PG_PORT.defaultValue()).isEqualTo(54321);
    }

    @Test
    public void testLackRequireOption() {
        Map<String, String> options = new HashMap<>();
        options.put(KingbaseDataSourceOptions.HOSTNAME.key(), TEST_HOST);
        options.put(KingbaseDataSourceOptions.PG_PORT.key(), String.valueOf(TEST_PORT));
        options.put(KingbaseDataSourceOptions.USERNAME.key(), TEST_USER);
        options.put(KingbaseDataSourceOptions.PASSWORD.key(), TEST_PASSWORD);
        options.put(KingbaseDataSourceOptions.TABLES.key(), "test_db.public.test_table");
        options.put(KingbaseDataSourceOptions.SLOT_NAME.key(), TEST_SLOT);

        KingbaseDataSourceFactory factory = new KingbaseDataSourceFactory();
        List<String> requireKeys =
                factory.requiredOptions().stream()
                        .map(ConfigOption::key)
                        .collect(Collectors.toList());
        for (String requireKey : requireKeys) {
            Map<String, String> remainingOptions = new HashMap<>(options);
            remainingOptions.remove(requireKey);
            Factory.Context context = new MockContext(Configuration.fromMap(remainingOptions));

            assertThatThrownBy(() -> factory.createDataSource(context))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining(
                            String.format(
                                    "One or more required options are missing.\n\n"
                                            + "Missing required options are:\n\n"
                                            + "%s",
                                    requireKey));
        }
    }

    @Test
    public void testUnsupportedOption() {
        Map<String, String> options = new HashMap<>();
        options.put(KingbaseDataSourceOptions.HOSTNAME.key(), TEST_HOST);
        options.put(KingbaseDataSourceOptions.PG_PORT.key(), String.valueOf(TEST_PORT));
        options.put(KingbaseDataSourceOptions.USERNAME.key(), TEST_USER);
        options.put(KingbaseDataSourceOptions.PASSWORD.key(), TEST_PASSWORD);
        options.put(KingbaseDataSourceOptions.TABLES.key(), "test_db.public.test_table");
        options.put(KingbaseDataSourceOptions.SLOT_NAME.key(), TEST_SLOT);
        options.put("unsupported_key", "unsupported_value");

        KingbaseDataSourceFactory factory = new KingbaseDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Unsupported options found for 'kingbase'.\n\n"
                                + "Unsupported options:\n\n"
                                + "unsupported_key");
    }

    @Test
    public void testTableValidationWithDifferentDatabases() {
        Map<String, String> options = new HashMap<>();
        options.put(KingbaseDataSourceOptions.HOSTNAME.key(), TEST_HOST);
        options.put(KingbaseDataSourceOptions.PG_PORT.key(), String.valueOf(TEST_PORT));
        options.put(KingbaseDataSourceOptions.USERNAME.key(), TEST_USER);
        options.put(KingbaseDataSourceOptions.PASSWORD.key(), TEST_PASSWORD);
        options.put(
                KingbaseDataSourceOptions.TABLES.key(),
                "db1.public.table1,db2.public.table2");
        options.put(KingbaseDataSourceOptions.SLOT_NAME.key(), TEST_SLOT);

        KingbaseDataSourceFactory factory = new KingbaseDataSourceFactory();
        Factory.Context context = new MockContext(Configuration.fromMap(options));

        assertThatThrownBy(() -> factory.createDataSource(context))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "not all table names have the same database name");
    }

    class MockContext implements Factory.Context {

        Configuration factoryConfiguration;

        public MockContext(Configuration factoryConfiguration) {
            this.factoryConfiguration = factoryConfiguration;
        }

        @Override
        public Configuration getFactoryConfiguration() {
            return factoryConfiguration;
        }

        @Override
        public Configuration getPipelineConfiguration() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return this.getClassLoader();
        }
    }
}
