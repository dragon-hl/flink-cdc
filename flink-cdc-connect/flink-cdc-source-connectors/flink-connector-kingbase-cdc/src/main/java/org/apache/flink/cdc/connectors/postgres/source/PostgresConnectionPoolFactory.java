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

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.relational.connection.ConnectionPoolId;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;

import com.zaxxer.hikari.HikariDataSource;
import io.debezium.jdbc.JdbcConfiguration;

/** A connection pool factory to create pooled Kingbase {@link HikariDataSource}. */
public class PostgresConnectionPoolFactory extends JdbcConnectionPoolFactory {
    public static final String JDBC_URL_PATTERN = "jdbc:kingbase8://%s:%s/%s";
    private static final String KINGBASE_DRIVER_CLASS = "com.kingbase8.Driver";

    @Override
    public String getJdbcUrl(JdbcSourceConfig sourceConfig) {

        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();
        String database = sourceConfig.getDatabaseList().get(0);
        return String.format(JDBC_URL_PATTERN, hostName, port, database);
    }

    @Override
    public HikariDataSource createPooledDataSource(JdbcSourceConfig sourceConfig) {
        final com.zaxxer.hikari.HikariConfig config = new com.zaxxer.hikari.HikariConfig();

        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();

        config.setPoolName(CONNECTION_POOL_PREFIX + hostName + ":" + port);
        config.setJdbcUrl(getJdbcUrl(sourceConfig));
        config.setUsername(sourceConfig.getUsername());
        config.setPassword(sourceConfig.getPassword());
        config.setMinimumIdle(MINIMUM_POOL_SIZE);
        config.setMaximumPoolSize(sourceConfig.getConnectionPoolSize());
        config.setConnectionTimeout(sourceConfig.getConnectTimeout().toMillis());
        config.setDriverClassName(KINGBASE_DRIVER_CLASS);

        // optional optimization configurations for pooled DataSource
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        return new HikariDataSource(config);
    }

    /**
     * The reuses of connection pools are based on databases in postgresql. Different databases in
     * same instance cannot reuse same connection pool to connect.
     */
    @Override
    public ConnectionPoolId getPoolId(
            JdbcConfiguration config, String dataSourcePoolFactoryIdentifier) {
        return new ConnectionPoolId(
                config.getHostname(),
                config.getPort(),
                config.getHostname(),
                config.getDatabase(),
                dataSourcePoolFactoryIdentifier);
    }
}
