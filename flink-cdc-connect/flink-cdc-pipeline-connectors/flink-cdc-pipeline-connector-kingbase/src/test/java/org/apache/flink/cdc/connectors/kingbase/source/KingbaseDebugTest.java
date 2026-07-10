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

package org.apache.flink.cdc.connectors.kingbase.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.FlinkSourceProvider;
import org.apache.flink.cdc.connectors.kingbase.factory.KingbaseDataSourceFactory;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Debug test for Kingbase pipeline connector incremental sync.
 *
 * <p>Prerequisites:
 * - Kingbase running at 192.168.8.127:54321
 * - Database "test_db" with "public" schema
 * - decoderbufs plugin configured in kingbase.conf (shared_preload_libraries = 'decoderbufs')
 * - wal_level = logical
 */
public class KingbaseDebugTest {
    private static final Logger LOG = LoggerFactory.getLogger(KingbaseDebugTest.class);

    private static final String HOST = "192.168.8.127";
    private static final int PORT = 54321;
    private static final String USER = "system";
    private static final String PASSWORD = "12345678ab";
    private static final String DATABASE = "test_db";
    private static final String SCHEMA = "public";
    private static final String SLOT_NAME = "flink_cdc_debug_slot";
    private static final String TABLE = "cdc_debug_test";

    @Test
    @Disabled("Requires Kingbase database at 192.168.8.127:54321. Run manually for debugging.")
    public void testKingbaseIncrementalSync() throws Exception {
        LOG.info("=== Kingbase Debug Test: Starting incremental sync test ===");

        // Step 1: Prepare test table
        LOG.info("Step 1: Creating test table {}", TABLE);
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(
                    "DROP TABLE IF EXISTS %s.%s", SCHEMA, TABLE));
            stmt.execute(String.format(
                    "CREATE TABLE %s.%s (id INT PRIMARY KEY, name VARCHAR(100), val INT)",
                    SCHEMA, TABLE));
            stmt.execute(String.format(
                    "INSERT INTO %s.%s VALUES (1, 'init_data', 100)", SCHEMA, TABLE));
            LOG.info("Table created and seeded.");
        }

        // Step 2: Drop any existing replication slot
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format("SELECT pg_drop_replication_slot('%s')", SLOT_NAME));
            LOG.info("Dropped old slot if existed.");
        } catch (Exception e) {
            LOG.info("No old slot to drop: {}", e.getMessage());
        }

        // Step 3: Configure Kingbase source
        LOG.info("Step 2: Configuring Kingbase source...");
        Configuration sourceConfig = new Configuration();
        sourceConfig.set(KingbaseDataSourceOptions.HOSTNAME, HOST);
        sourceConfig.set(KingbaseDataSourceOptions.PG_PORT, PORT);
        sourceConfig.set(KingbaseDataSourceOptions.USERNAME, USER);
        sourceConfig.set(KingbaseDataSourceOptions.PASSWORD, PASSWORD);
        sourceConfig.set(KingbaseDataSourceOptions.TABLES, DATABASE + "." + SCHEMA + "." + TABLE);
        sourceConfig.set(KingbaseDataSourceOptions.SLOT_NAME, SLOT_NAME);
        sourceConfig.set(KingbaseDataSourceOptions.SERVER_TIME_ZONE, "Asia/Shanghai");
        sourceConfig.set(KingbaseDataSourceOptions.DECODING_PLUGIN_NAME, "decoderbufs");
        sourceConfig.set(KingbaseDataSourceOptions.SCAN_STARTUP_MODE, "initial");
        LOG.info("Kingbase source configured.");

        // Step 4: Create DataSource
        LOG.info("Step 3: Creating Kingbase DataSource...");
        FactoryHelper.DefaultContext context =
                new FactoryHelper.DefaultContext(
                        sourceConfig, new Configuration(), this.getClass().getClassLoader());
        KingbaseDataSourceFactory factory = new KingbaseDataSourceFactory();
        KingbaseDataSource dataSource = (KingbaseDataSource) factory.createDataSource(context);
        LOG.info("Kingbase DataSource created.");

        // Step 5: Start Flink local env
        LOG.info("Step 4: Starting Flink local environment...");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        env.setRestartStrategy(RestartStrategies.noRestart());

        FlinkSourceProvider sourceProvider =
                (FlinkSourceProvider) dataSource.getEventSourceProvider();

        List<Event> capturedEvents = new ArrayList<>();
        Thread captureThread = new Thread(() -> {
            try {
                env.fromSource(
                        sourceProvider.getSource(),
                        WatermarkStrategy.noWatermarks(),
                        "kingbase-debug",
                        new EventTypeInfo())
                    .executeAndCollect()
                    .forEachRemaining(event -> {
                        synchronized (capturedEvents) {
                            capturedEvents.add(event);
                            LOG.info("Captured event: type={}, tableId={}",
                                    event.getClass().getSimpleName(),
                                    extractTableId(event));
                        }
                    });
            } catch (Exception e) {
                LOG.error("Pipeline error", e);
            }
        });
        captureThread.setDaemon(true);
        captureThread.start();

        // Step 6: Wait for snapshot and then insert data
        LOG.info("Step 5: Waiting for snapshot phase to complete (15s)...");
        Thread.sleep(15000);

        LOG.info("Step 6: Inserting incremental data...");
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(String.format(
                    "INSERT INTO %s.%s VALUES (2, 'incremental_1', 200)", SCHEMA, TABLE));
            LOG.info("INSERT executed: (2, incremental_1, 200)");
            Thread.sleep(3000);

            stmt.execute(String.format(
                    "UPDATE %s.%s SET name = 'updated_1', val = 201 WHERE id = 2", SCHEMA, TABLE));
            LOG.info("UPDATE executed: id=2 name->updated_1");
            Thread.sleep(3000);

            stmt.execute(String.format(
                    "DELETE FROM %s.%s WHERE id = 2", SCHEMA, TABLE));
            LOG.info("DELETE executed: id=2");
            Thread.sleep(3000);
        }

        // Step 7: Print summary
        Thread.sleep(5000);
        synchronized (capturedEvents) {
            LOG.info("Step 7: Total events captured: {}", capturedEvents.size());
            for (Event event : capturedEvents) {
                if (event instanceof CreateTableEvent) {
                    CreateTableEvent ct = (CreateTableEvent) event;
                    LOG.info("  CREATE_TABLE: {}", ct.tableId());
                } else if (event instanceof DataChangeEvent) {
                    DataChangeEvent dc = (DataChangeEvent) event;
                    LOG.info("  DATA_CHANGE: op={}, tableId={}",
                            dc.op(), dc.tableId());
                }
            }
        }

        LOG.info("=== Kingbase debug test completed ===");
    }

    private Connection getConnection() throws Exception {
        String url = String.format("jdbc:postgresql://%s:%d/%s", HOST, PORT, DATABASE);
        return DriverManager.getConnection(url, USER, PASSWORD);
    }

    private String extractTableId(Event event) {
        if (event instanceof DataChangeEvent) {
            return ((DataChangeEvent) event).tableId().toString();
        } else if (event instanceof CreateTableEvent) {
            return ((CreateTableEvent) event).tableId().toString();
        }
        return "unknown";
    }
}
