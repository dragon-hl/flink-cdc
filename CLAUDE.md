# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Flink CDC is a distributed data integration tool for real-time and batch data. Users describe data movement and transformation in YAML pipeline definitions. The project is built with Maven (Java 8+), depends on Apache Flink 1.20.x, and uses Debezium 1.9.x for change data capture.

## Build Commands

```bash
# Full build (all modules, skip tests)
mvn clean install -DskipTests

# Fast build (skip checkstyle, spotless, RAT, enforcer — useful during development)
mvn clean install -DskipTests -Pfast

# Compile and run tests for all modules
mvn clean verify

# Compile and run tests for a specific module (must compile dependencies first)
mvn install -DskipTests -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql -am
mvn verify -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql

# Run a single test class
mvn verify -pl flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc \
  -Dtest=MySqlSourceExampleTest

# Run a single test method
mvn verify -pl <module-path> -Dtest=TestClass#testMethod

# Code formatting (Google Java Format, AOSP style)
mvn com.diffplug.spotless:spotless-maven-plugin:apply
mvn com.diffplug.spotless:spotless-maven-plugin:check   # check only

# License check (requires Ruby; run after compiling)
gem install rubyzip -v 2.3.0 && ./tools/ci/license_check.rb
```

**Important**: Tests use `mvn verify` (not `mvn test`) because they are integration tests bound to the `verify` phase. The POM enforces `forkCount=1` and `reuseForks=true` due to heavy mini cluster usage.

**Test infrastructure**: Tests use Testcontainers (`org.testcontainers`) to spin up real databases (MySQL, Postgres, Kafka, etc.). Docker must be available to run most connector tests.

## Architecture

### Module Map

```
flink-cdc-cli/             CLI entry point (CliFrontend); parses YAML and submits jobs
flink-cdc-common/          Core APIs: DataSource, DataSink, Event types, PipelineOptions,
                           SchemaChangeBehavior, MetadataAccessor, MetadataApplier
flink-cdc-composer/        Assembles pipeline definitions into executable Flink jobs
                           (definition/ has PipelineDef, SourceDef, SinkDef, RouteDef, TransformDef)
flink-cdc-pipeline-model/  Model classes for YAML pipeline definitions (uses LangChain4j for AI features)
flink-cdc-runtime/         Runtime operators that execute in the Flink job:
                           operators/schema/  — schema evolution tracking
                           operators/sink/    — data sink writers (regular + batch)
                           operators/transform/ — pre/post transform, projection, filtering
                           parser/            — JaninoCompiler for transform expressions
flink-cdc-connect/
  flink-cdc-source-connectors/
    flink-cdc-base/          Base classes for all source connectors
    flink-connector-<db>-cdc/  Runtime source connector implementations
    flink-sql-connector-<db>-cdc/  SQL packaging (shaded JAR for Flink SQL)
  flink-cdc-pipeline-connectors/
    flink-cdc-pipeline-connector-<db>  End-to-end pipeline connectors (source + sink combined)
flink-cdc-e2e-tests/       End-to-end tests (pipeline and source variants)
flink-cdc-dist/            Distribution assembly (tarball with bin/flink-cdc.sh)
flink-cdc-flink1-compat/   Compatibility layer for Flink 1.x
flink-cdc-flink2-compat/   Compatibility layer for Flink 2.x
tools/                     cdcup (playground), CI scripts, Maven checkstyle config, release scripts
```

### Event Model (the core abstraction)

All data flows through **Events** — special Flink records that describe captured changes:

- **`DataChangeEvent`**: carries table ID, before/after images, operation type (INSERT/DELETE/UPDATE/REPLACE), and metadata. Does NOT embed its own schema to reduce serialization overhead.
- **`SchemaChangeEvent`**: describes DDL changes (AddColumn, DropColumn, RenameColumn, AlterColumnType, CreateTable).

**Schema contract**: A `CreateTableEvent` must be emitted before any `DataChangeEvent` for a newly-seen table; `SchemaChangeEvent` must precede `DataChangeEvent` after a schema change. The runtime's schema operators enforce this ordering.

### Data Flow (Pipeline)

```
External DB → EventSource → [PreTransform → SchemaOperator → PostTransform] → Route → DataSink → External DB
```

1. **Source** reads changes from external systems, emits Events
2. **PreTransform** applies user-defined transformations (projection, filtering) before schema tracking
3. **SchemaOperator** tracks schema state per table ID
4. **PostTransform** applies transformations after schema resolution
5. **Route** maps source tables to sink tables
6. **Sink** writes/applies changes to the target system

### Connector Pattern

Each connector implements two key interfaces from `flink-cdc-common`:

- **DataSource** — produces `EventSourceProvider` + `MetadataAccessor`
- **DataSink** — produces `EventSinkProvider` + `MetadataApplier`

Source connectors (e.g., `flink-connector-mysql-cdc`) use Debezium under the hood to read binlogs/WAL. Pipeline connectors bundle both source and sink for end-to-end integration.

### Commit Message Convention

Commit messages follow the format: `[FLINK-xxxxx][component] Description`. The FLINK issue number references the Apache Flink JIRA with the `Flink CDC` component tag.

## CI

CI runs on GitHub Actions. Key jobs:
- **License check**: compiles JARs, runs `tools/ci/license_check.rb`
- **Common unit tests**: core modules (`flink-cdc-cli`, `flink-cdc-common`, `flink-cdc-composer`, `flink-cdc-runtime`, `flink-cdc-base`, `flink-cdc-pipeline-connector-values`)
- **Pipeline/source unit tests**: per-connector (MySQL, Postgres, Oracle, MongoDB, etc.)
- **E2E tests**: run against Flink 1.19.2 and 1.20.1 with parallelism 1 and 4
- Tests use a random JVM timezone (set via `-Duser.timezone`) to catch timezone-dependent bugs
- MongoDB tests have version-specific profiles (`-DspecifiedMongoVersion=6.0.16` or `7.0.12`)

CI test command pattern:
```bash
# Compile everything needed, then run verify on the module
mvn --no-snapshot-updates -B -DskipTests -pl <all-needed-modules> -am install && \
mvn --no-snapshot-updates -B -pl <test-module> -DspecifiedFlinkVersion=<version> -DspecifiedParallelism=<1|4> -Duser.timezone=<random> verify
```
Flink 官方不支持kingbase连接器，现在我已基本完成自定义连接器的开发：
[pipeline模块](flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-kingbase) 和 [flink sql模块](flink-cdc-connect/flink-cdc-source-connectors/flink-connector-kingbase-cdc)
现在处于使用调试阶段，请帮我排查并解决问题

编译使用jdk8进行编译：/d/myserver/jdk-1.8.251