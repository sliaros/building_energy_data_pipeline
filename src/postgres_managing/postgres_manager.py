import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import List, Dict, Any, Optional, Union
import logging
from datetime import datetime
import time
from dataclasses import dataclass
from contextlib import contextmanager
import json
import threading
import backoff
import os
from logs.logging_config import setup_logging
from functools import lru_cache, wraps

@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    logger: Optional[logging.Logger] = None
    application_name: str = "DBManager"
    min_connections: int = 5
    max_connections: int = 20
    connection_timeout: int = 30
    query_timeout: int = 30000  # milliseconds
    enable_ssl: bool = True
    ssl_mode: str = "prefer"
    work_mem: str = "16MB"
    maintenance_work_mem: str = "128MB"
    replication_slot: Optional[str] = None
    standby_servers: List[str] = None
    postgresql_conf: Optional[str] = None
    pg_hba_conf: Optional[str] = None

def configure_connection(func):
    """Decorator to apply session-level settings to database connections."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        conn = func(self, *args, **kwargs)
        try:
            with conn.cursor() as cur:
                cur.execute(f"SET work_mem = '{self.config.work_mem}'")
                cur.execute(f"SET maintenance_work_mem = '{self.config.maintenance_work_mem}'")
                cur.execute("SET client_encoding = 'UTF8'")
            return conn
        except Exception as e:
            self._logger.error(f"Failed to configure connection: {e}")
            conn.close()
            raise
    return wrapper

class PostgresManager:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._logger = config.logger or logging.getLogger(self.__class__.__name__)

        if not self._logger.hasHandlers():
            setup_logging(
                log_file='C:\\slPrivateData\\00_portfolio\\building_energy_data_pipeline\\logs\\application.log')

        self._connection_pool = []
        self._pool_lock = threading.Lock()
        self._initialize_connection_pool()
        self.query_history = []
        self.max_query_history = 1000

    @backoff.on_exception(
        backoff.expo,
        (psycopg2.OperationalError, psycopg2.InterfaceError),
        max_tries=5,
        on_backoff=lambda details: logging.warning(
            f"Retrying database connection pool initialization (attempt {details['tries']} after {details['wait']:.2f}s)..."
        ),
    )
    def _initialize_connection_pool(self) -> None:
        """Initialize the PostgreSQL connection pool."""
        self._pool = psycopg2.pool.SimpleConnectionPool(
            minconn=self.config.min_connections,
            maxconn=self.config.max_connections,
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password,
            sslmode=self.config.ssl_mode if self.config.enable_ssl else 'disable'
        )

    @configure_connection
    @backoff.on_exception(
        backoff.expo,
        (psycopg2.OperationalError, psycopg2.InterfaceError),
        max_tries=5,
        on_backoff=lambda details: logging.warning(
            f"Retrying database connection retrieval (attempt {details['tries']} after {details['wait']:.2f}s)..."
        ),
    )
    def get_connection(self):
        """Get a connection from the pool instead of creating a new one."""
        try:
            _connection = self._pool.getconn()
            if _connection.closed:
                self._pool.putconn(_connection, close=True)  # Remove bad connection
                _connection = self._pool.getconn()
            self._logger.info(f"Total: {self._pool.maxconn}, In Use: {len(self._pool._used)}, Available: {len(self._pool._pool)}")
            return _connection
        except Exception as e:
            self._logger.error(f"Failed to get a connection from the pool: {e}")
            raise

    def release_connection(self, conn):
        """Return the connection back to the pool."""
        try:
            if conn:
                self._pool.putconn(conn)
        except Exception as e:
            self._logger.error(f"Failed to return connection to pool: {e}")

    @contextmanager
    def connection_context(self):
        """Provide a transactional connection context."""
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.release_connection(conn)

    def close_all_connections(self):
        """Close all connections in the pool."""
        if self._pool:
            self._pool.closeall()

    def verify_connection(self) -> bool:
        """
        Verify and create database if it doesn't exist.

        Returns:
            bool: Connection status
        """
        try:
            # Try to create a connection using the established connection method
            with self.connection_context() as conn:
                self._logger.info("Successfully connected to database")
                return True
        except psycopg2.OperationalError as e:
            if "does not exist" in str(e):
                return self._create_database()
            self._logger.error(f"Connection verification failed: {str(e)}")
            raise

    def _create_database(self) -> bool:
        """
        Create a new database if it doesn't exist.

        Returns:
            bool: Database creation status
        """
        # Create a modified configuration for connecting to 'postgres' database
        temp_config = DatabaseConfig(
            host=self.config.host,
            port=self.config.port,
            database='postgres',  # Connect to default postgres database
            user=self.config.user,
            password=self.config.password,
            logger=self.config.logger,
            application_name=self.config.application_name,
            connection_timeout=self.config.connection_timeout,
            enable_ssl=self.config.enable_ssl,
            ssl_mode=self.config.ssl_mode
        )

        try:
            # Create connection to postgres database with autocommit mode
            conn = psycopg2.connect(
                host=temp_config.host,
                port=temp_config.port,
                database=temp_config.database,
                user=temp_config.user,
                password=temp_config.password,
                application_name=temp_config.application_name,
                connect_timeout=temp_config.connection_timeout,
                sslmode=temp_config.ssl_mode if temp_config.enable_ssl else 'disable'
            )
            conn.autocommit = True  # Enable autocommit

            try:
                with conn.cursor() as cur:
                    cur.execute(f"CREATE DATABASE {self.config.database}")
                self._logger.info(f"Created database {self.config.database}")
                return True
            finally:
                if conn and not conn.closed:
                    conn.close()

        except Exception as e:
            self._logger.error(f"Database creation failed: {str(e)}")
            return False

    # Query Management Methods
    def execute_query(self, query: str, params: Optional[tuple] = None, fetch_all: bool = True) -> Union[
        List[Dict], Dict, None]:
        """Execute a query with proper error handling and logging."""
        start_time = datetime.now()
        try:
            with self.connection_context() as conn:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(query, params)
                    if cur.description:  # Select query
                        result = cur.fetchall() if fetch_all else cur.fetchone()
                        result = [dict(row) for row in result] if fetch_all else dict(result)
                    else:  # DML query
                        result = cur.rowcount
                        conn.commit()
                    execution_time = (datetime.now() - start_time).total_seconds()
                    self._log_query(query, params, execution_time)
                    return result
        except Exception as e:
            self._logger.error(f"Query execution failed: {str(e)}")
            raise

    def _log_query(self, query: str, params: Optional[tuple], execution_time: float) -> None:
        """Log query execution details."""
        query_info = {
            'timestamp': datetime.now().isoformat(),
            'query': query,
            'parameters': params,
            'execution_time': execution_time
        }
        self.query_history.append(query_info)
        if len(self.query_history) > self.max_query_history:
            self.query_history.pop(0)

    # Table Management Methods
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database using execute_query."""
        query = """
            SELECT EXISTS (
                SELECT 1
                FROM pg_tables
                WHERE tablename = %s
            );
        """
        result = self.execute_query(query, (table_name,), fetch_all=False)
        return result["exists"] if result else False

    def create_table(self, table_name: str, columns: List[Dict[str, str]],
                     primary_key: Optional[str] = None) -> bool:
        """Create a new table with specified columns and constraints."""
        column_definitions = []
        for col in columns:
            definition = f"{col['name']} {col['type']}"
            if col.get('nullable') is False:
                definition += " NOT NULL"
            if col.get('default'):
                definition += f" DEFAULT {col['default']}"
            column_definitions.append(definition)

        if primary_key:
            column_definitions.append(f"PRIMARY KEY ({primary_key})")

        create_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(column_definitions)}
            )
        """

        try:
            self.execute_query(create_query)
            self._logger.info(f"Table {table_name} created successfully")
            return True
        except Exception as e:
            self._logger.error(f"Failed to create table {table_name}: {str(e)}")
            return False

    def drop_table(self, table_name: str, cascade: bool = False) -> bool:
        """Drop a table with safety checks."""
        try:
            exists_query = "SELECT to_regclass(%s) IS NOT NULL"
            exists = self.execute_query(exists_query, (table_name,), fetch_all=False)

            if exists and exists.get("to_regclass"):
                cascade_str = "CASCADE" if cascade else ""
                self.execute_query(f"DROP TABLE {table_name} {cascade_str}")
                self._logger.info(f"Table {table_name} dropped successfully")
                return True
            return False
        except Exception as e:
            self._logger.error(f"Failed to drop table {table_name}: {str(e)}")
            return False

    # Database Health Methods
    def get_database_size(self) -> Dict[str, Any]:
        """Get detailed database size information."""
        query = """
            SELECT
                pg_database.datname,
                pg_size_pretty(pg_database_size(pg_database.datname)) as size,
                pg_database_size(pg_database.datname) as size_bytes
            FROM pg_database
            WHERE datname = current_database()
        """
        return self.execute_query(query, fetch_all=False)

    def get_table_sizes(self) -> List[Dict[str, Any]]:
        """Get size information for all tables."""
        query = """
            SELECT
                schemaname,
                tablename,
                pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as total_size,
                pg_size_pretty(pg_relation_size(schemaname || '.' || tablename)) as table_size,
                pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename) - 
                             pg_relation_size(schemaname || '.' || tablename)) as index_size
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC
        """
        return self.execute_query(query)

    def analyze_query_performance(self, query: str) -> Dict[str, Any]:
        """Analyze query performance using EXPLAIN ANALYZE."""
        explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"
        return self.execute_query(explain_query, fetch_all=False)

    def get_slow_queries(self, threshold_seconds: int = 1) -> List[Dict[str, Any]]:
        """Get currently running slow queries."""
        query = f"""
            SELECT
                pid,
                usename,
                client_addr,
                application_name,
                query_start,
                state,
                query
            FROM pg_stat_activity
            WHERE state = 'active'
                AND query NOT LIKE '%pg_stat_activity%'
                AND query_start < NOW() - interval '{threshold_seconds} seconds'
            ORDER BY query_start
        """
        return self.execute_query(query)  # No need for parameters

    def get_index_usage_stats(self) -> List[Dict[str, Any]]:
        """Get detailed index usage statistics."""
        query = """
            SELECT
                schemaname,
                tablename,
                indexname,
                idx_scan as number_of_scans,
                idx_tup_read as tuples_read,
                idx_tup_fetch as tuples_fetched,
                pg_size_pretty(pg_relation_size(schemaname || '.' || indexname::text)) as index_size
            FROM pg_stat_user_indexes
            ORDER BY idx_scan DESC
        """
        return self.execute_query(query)

    def get_table_bloat_estimation(self) -> List[Dict[str, Any]]:
        """Estimate table bloat."""
        query = """
            SELECT
                schemaname,
                tablename,
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples,
                CASE WHEN n_live_tup > 0
                    THEN round(100 * n_dead_tup::float / n_live_tup::float, 2)
                    ELSE 0
                END as bloat_ratio
            FROM pg_stat_user_tables
            ORDER BY bloat_ratio DESC
        """
        return self.execute_query(query)

    @lru_cache(maxsize=100)
    def _cached_query(self, query_hash, fetch_all=True):
        """Internal method to handle cached queries."""
        # Extract query and params from hash
        query, params_str = query_hash.split('|')
        params = eval(params_str) if params_str else None

        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query, params)
                if cur.description:  # Select query
                    result = cur.fetchall() if fetch_all else cur.fetchone()
                    return [dict(row) for row in result] if fetch_all else (dict(result) if result else None)
                else:  # DML query
                    conn.commit()
                    return cur.rowcount

    def execute_cached_query(self, query: str, params: Optional[tuple] = None,
                             fetch_all: bool = True, ttl: int = 60) -> Union[List[Dict], Dict, None]:
        """Execute a cached query with specified TTL (in seconds)."""
        # Only cache SELECT queries
        if not query.strip().upper().startswith('SELECT'):
            return self.execute_query(query, params, fetch_all)

        # Create a unique hash for this query and params
        query_hash = f"{query}|{str(params)}"

        # Get result from cache or execute query
        start_time = time.time()
        result = self._cached_query(query_hash, fetch_all)
        execution_time = time.time() - start_time

        self._log_query(query, params, execution_time, cached=True)
        return result

    def clear_query_cache(self):
        """Clear the query cache."""
        self._cached_query.cache_clear()

    # Database Maintenance Methods
    def vacuum_analyze_table(self, table_name: str, full: bool = False) -> bool:
        """Perform VACUUM ANALYZE on a table."""
        try:
            with self.get_connection() as conn:
                old_isolation_level = conn.isolation_level
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                with conn.cursor() as cur:
                    vacuum_type = "FULL" if full else ""
                    cur.execute(f"VACUUM {vacuum_type} ANALYZE {table_name}")
                conn.set_isolation_level(old_isolation_level)
            return True
        except Exception as e:
            self._logger.error(f"Vacuum analyze failed for {table_name}: {str(e)}")
            return False

    def reindex_table(self, table_name: str, concurrent: bool = True) -> bool:
        """Reindex a table and its indexes."""
        try:
            concurrent_str = "CONCURRENTLY" if concurrent else ""
            query = f"REINDEX TABLE {concurrent_str} {table_name}"
            self.execute_query(query)
            return True
        except Exception as e:
            self._logger.error(f"Reindex failed for {table_name}: {str(e)}")
            return False

    # Backup and Restore Methods
    def create_backup(self, backup_path: str) -> bool:
        """Create a database backup."""
        try:
            import subprocess
            cmd = [
                'pg_dump',
                f'-h{self.config.host}',
                f'-p{self.config.port}',
                f'-U{self.config.user}',
                '-Fc',
                f'-f{backup_path}',
                self.config.database
            ]
            env = dict(os.environ, PGPASSWORD=self.config.password)
            subprocess.run(cmd, env=env, check=True)
            return True
        except Exception as e:
            self._logger.error(f"Backup failed: {str(e)}")
            return False

    # Utility Methods
    def get_table_definition(self, table_name: str) -> Dict[str, Any]:
        """Get complete table definition including constraints and indexes."""
        column_query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """

        constraint_query = """
            SELECT
                conname as constraint_name,
                pg_get_constraintdef(c.oid) as definition
            FROM pg_constraint c
            JOIN pg_namespace n ON n.oid = c.connamespace
            WHERE conrelid = %s::regclass
        """

        index_query = """
            SELECT
                indexname,
                indexdef
            FROM pg_indexes
            WHERE tablename = %s
        """

        return {
            'columns': self.execute_query(column_query, (table_name,)),
            'constraints': self.execute_query(constraint_query, (table_name,)),
            'indexes': self.execute_query(index_query, (table_name,))
        }

    def get_table_dependencies(self, table_name: str) -> List[Dict[str, Any]]:
        """Get all dependencies for a table."""
        query = """
            WITH RECURSIVE deps AS (
                SELECT DISTINCT
                    dependent_ns.nspname as dependent_schema,
                    dependent_view.relname as dependent_view,
                    source_ns.nspname as source_schema,
                    source_table.relname as source_table
                FROM pg_depend
                JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
                JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
                JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
                JOIN pg_namespace dependent_ns ON dependent_view.relnamespace = dependent_ns.oid
                JOIN pg_namespace source_ns ON source_table.relnamespace = source_ns.oid
                WHERE source_table.relname = %s
                    AND source_ns.nspname = 'public'
                    AND pg_depend.deptype = 'n'
            )
            SELECT * FROM deps
        """
        return self.execute_query(query, (table_name,))

    # Advanced Partitioning Methods
    def create_partitioned_table(self, table_name: str, columns: List[Dict[str, str]],
                                 partition_key: str, partition_type: str = 'RANGE') -> bool:
        """Create a partitioned table with specified strategy."""
        try:
            column_definitions = []
            for col in columns:
                definition = f"{col['name']} {col['type']}"
                if col.get('nullable') is False:
                    definition += " NOT NULL"
                if col.get('default'):
                    definition += f" DEFAULT {col['default']}"
                column_definitions.append(definition)

            create_query = f"""
                CREATE TABLE {table_name} (
                    {', '.join(column_definitions)}
                ) PARTITION BY {partition_type} ({partition_key})
            """
            self.execute_query(create_query)
            return True
        except Exception as e:
            self._logger.error(f"Failed to create partitioned table: {str(e)}")
            return False

    def create_table_partition(self, table_name: str, partition_name: str,
                               start_value: Any, end_value: Any) -> bool:
        """Create a new partition for a partitioned table."""
        try:
            create_query = f"""
                CREATE TABLE {partition_name}
                PARTITION OF {table_name}
                FOR VALUES FROM ('{start_value}') TO ('{end_value}')
            """
            self.execute_query(create_query)
            return True
        except Exception as e:
            self._logger.error(f"Failed to create partition: {str(e)}")
            return False

    def detach_partition(self, table_name: str, partition_name: str) -> bool:
        """Detach a partition from a partitioned table."""
        try:
            query = f"ALTER TABLE {table_name} DETACH PARTITION {partition_name}"
            self.execute_query(query)
            return True
        except Exception as e:
            self._logger.error(f"Failed to detach partition: {str(e)}")
            return False

    # Advanced Query Optimization Methods
    def analyze_table_statistics(self, table_name: str) -> Dict[str, Any]:
        """Analyze and return detailed table statistics."""
        query = """
            SELECT
                schemaname,
                tablename,
                n_live_tup,
                n_dead_tup,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze,
                vacuum_count,
                autovacuum_count,
                analyze_count,
                autoanalyze_count
            FROM pg_stat_user_tables
            WHERE tablename = %s
        """
        return self.execute_query(query, (table_name,), fetch_all=False)

    def get_missing_indexes(self, min_scans: int = 1000) -> List[Dict[str, Any]]:
        """Identify potentially missing indexes based on sequential scans."""
        query = """
            SELECT
                schemaname,
                tablename,
                seq_scan,
                seq_tup_read,
                idx_scan,
                n_live_tup,
                n_dead_tup
            FROM pg_stat_user_tables
            WHERE seq_scan > %s
                AND (idx_scan / GREATEST(seq_scan, 1))::float < 0.1
            ORDER BY seq_tup_read DESC
        """
        return self.execute_query(query, (min_scans,))

    def get_unused_indexes(self, min_size_bytes: int = 1024 * 1024) -> List[Dict[str, Any]]:
        """Identify unused indexes that are taking up space."""
        query = """
            SELECT
                schemaname,
                tablename,
                indexname,
                idx_scan,
                pg_size_pretty(pg_relation_size(schemaname || '.' || indexname::text)) as index_size,
                pg_relation_size(schemaname || '.' || indexname::text) as index_size_bytes
            FROM pg_stat_user_indexes
            WHERE idx_scan = 0
                AND pg_relation_size(schemaname || '.' || indexname::text) > %s
                AND indexname NOT LIKE '%%_pkey'
                AND indexname NOT LIKE '%%_unique'
            ORDER BY pg_relation_size(schemaname || '.' || indexname::text) DESC
        """
        return self.execute_query(query, (min_size_bytes,))

    # Advanced Monitoring Methods
    def get_lock_information(self) -> List[Dict[str, Any]]:
        """Get detailed information about current locks."""
        query = """
            SELECT
                blocked_locks.pid AS blocked_pid,
                blocked_activity.usename AS blocked_user,
                blocking_locks.pid AS blocking_pid,
                blocking_activity.usename AS blocking_user,
                blocked_activity.query AS blocked_statement,
                blocking_activity.query AS blocking_statement,
                blocked_activity.application_name AS blocked_application,
                blocking_activity.application_name AS blocking_application
            FROM pg_catalog.pg_locks blocked_locks
            JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
            JOIN pg_catalog.pg_locks blocking_locks 
                ON blocking_locks.locktype = blocked_locks.locktype
                AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
                AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                AND blocking_locks.pid != blocked_locks.pid
            JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
            WHERE NOT blocked_locks.GRANTED
        """
        return self.execute_query(query)

    def get_replication_status(self) -> List[Dict[str, Any]]:
        """Get detailed replication status information."""
        query = """
            SELECT
                client_addr,
                usename,
                application_name,
                state,
                sync_state,
                pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)) as send_lag,
                pg_size_pretty(pg_wal_lsn_diff(sent_lsn, write_lsn)) as write_lag,
                pg_size_pretty(pg_wal_lsn_diff(write_lsn, flush_lsn)) as flush_lag,
                pg_size_pretty(pg_wal_lsn_diff(flush_lsn, replay_lsn)) as replay_lag
            FROM pg_stat_replication
        """
        return self.execute_query(query)

    # Advanced Performance Monitoring
    def get_cache_hit_ratios(self) -> Dict[str, float]:
        """Get cache hit ratios for various PostgreSQL caches."""
        queries = {
            'heap_read_ratio': """
                SELECT
                    sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0) as ratio
                FROM pg_statio_user_tables
            """,
            'index_read_ratio': """
                SELECT
                    sum(idx_blks_hit) / NULLIF(sum(idx_blks_hit) + sum(idx_blks_read), 0) as ratio
                FROM pg_statio_user_indexes
            """,
            'toast_read_ratio': """
                SELECT
                    sum(toast_blks_hit) / NULLIF(sum(toast_blks_hit) + sum(toast_blks_read), 0) as ratio
                FROM pg_statio_user_tables
            """
        }

        results = {}
        for name, query in queries.items():
            result = self.execute_query(query, fetch_all=False)
            results[name] = result['ratio'] if result and result['ratio'] is not None else 0.0
        return results

    # Advanced Table Management
    def estimate_row_count(self, table_name: str) -> int:
        """Estimate the number of rows in a table using statistics."""
        query = """
            SELECT reltuples::bigint AS estimate
            FROM pg_class
            WHERE relname = %s
        """
        result = self.execute_query(query, (table_name,), fetch_all=False)
        return result['estimate'] if result else 0

    def get_bloat_analysis(self) -> List[Dict[str, Any]]:
        """Get detailed bloat analysis for tables and indexes."""
        query = """
            WITH constants AS (
                SELECT current_setting('block_size')::numeric AS bs,
                       23 AS hdr,
                       8 AS ma
            ),
            no_stats AS (
                SELECT table_schema, table_name, 
                       n_live_tup::numeric as est_rows,
                       pg_table_size(relid)::numeric as table_size
                FROM information_schema.columns
                JOIN pg_stat_user_tables as psut
                     ON table_schema = psut.schemaname
                     AND table_name = psut.relname
                LEFT OUTER JOIN pg_stats
                ON table_schema = pg_stats.schemaname
                    AND table_name = pg_stats.tablename
                    AND column_name = attname
                WHERE attname IS NULL
                    AND table_schema NOT IN ('pg_catalog', 'information_schema')
                GROUP BY table_schema, table_name, relid, n_live_tup
            ),
            null_headers AS (
                SELECT
                    hdr+1+(sum(case when null_frac <> 0 THEN 1 else 0 END)/8) as nullhdr,
                    SUM((1-null_frac)*avg_width) as datawidth,
                    MAX(null_frac) as maxfracsum,
                    schemaname,
                    tablename,
                    hdr
                FROM pg_stats CROSS JOIN constants
                LEFT OUTER JOIN no_stats
                    ON schemaname = no_stats.table_schema
                    AND tablename = no_stats.table_name
                WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                GROUP BY schemaname, tablename, hdr
            )
            SELECT
                schemaname as schema_name,
                tablename as table_name,
                ROUND(CASE WHEN bs > 0
                    THEN bs*CEIL((datawidth + nullhdr)::numeric/bs)
                    ELSE NULL
                END) as expected_bytes,
                ROUND(CASE WHEN bs > 0
                    THEN bs*CEIL(datawidth::numeric/bs)
                    ELSE NULL
                END) as expected_bytes_packed,
                pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)) as actual_bytes,
                pg_size_pretty(pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename))) as actual_size,
                CASE WHEN bs > 0
                    THEN ROUND(100 * (pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)) - 
                                    bs*CEIL((datawidth + nullhdr)::numeric/bs))::numeric / 
                              pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))
                    ELSE NULL
                END as bloat_percentage
            FROM null_headers CROSS JOIN constants
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                AND pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)) > 0
            ORDER BY bloat_percentage DESC NULLS LAST
        """
        return self.execute_query(query)

    # Data Export/Import Methods
    def get_columns(self, table_name: str) -> List[str]:
        query = """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        return [col["column_name"] for col in self.execute_query(query, (table_name,))]

    def export_table_to_csv(self, table_name: str, output_path: str) -> bool:
        try:
            columns = self.get_columns(table_name)
            query = f"COPY (SELECT {', '.join(columns)} FROM {table_name}) TO STDOUT WITH CSV HEADER"

            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    with open(output_path, "w") as f:
                        cur.copy_expert(query, f)
            return True
        except Exception as e:
            self._logger.error(f"Failed to export table to CSV: {str(e)}")
            return False

    def import_csv_to_table(self, table_name: str, file_path: str,
                            delimiter: str = ',', header: bool = True) -> bool:
        """Import data from CSV file to table."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    with open(file_path, 'r') as f:
                        if header:
                            next(f)  # Skip header row
                        cur.copy_from(f, table_name, sep=delimiter)
                conn.commit()
            return True
        except Exception as e:
            self._logger.error(f"Failed to import CSV to table: {str(e)}")
            return False

    # Schema Management Methods
    def compare_table_schemas(self, table1: str, table2: str) -> Dict[str, Any]:
        """Compare schemas of two tables and return differences."""
        query = """
            WITH table1_cols AS (
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = %s
            ),
            table2_cols AS (
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = %s
            )
            SELECT
                'only_in_table1' as difference_type,
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM table1_cols
            WHERE column_name NOT IN (SELECT column_name FROM table2_cols)
            UNION ALL
            SELECT
                'only_in_table2' as difference_type,
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM table2_cols
            WHERE column_name NOT IN (SELECT column_name FROM table1_cols)
            UNION ALL
            SELECT
                'type_mismatch' as difference_type,
                t1.column_name,
                t1.data_type as table1_type,
                t2.data_type as table2_type,
                NULL as column_default
            FROM table1_cols t1
            JOIN table2_cols t2 USING (column_name)
            WHERE t1.data_type != t2.data_type
        """
        return self.execute_query(query, (table1, table2))

    # Advanced Security Methods
    def audit_user_permissions(self) -> List[Dict[str, Any]]:
        """Audit database user permissions."""
        query = """
            SELECT
                r.rolname,
                r.rolsuper,
                r.rolinherit,
                r.rolcreaterole,
                r.rolcreatedb,
                r.rolcanlogin,
                r.rolreplication,
                r.rolconnlimit,
                r.rolvaliduntil,
                ARRAY(
                    SELECT b.rolname
                    FROM pg_catalog.pg_auth_members m
                    JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
                    WHERE m.member = r.oid
                ) as memberof,
                array_agg(distinct privilege_type) as privileges
            FROM pg_catalog.pg_roles r
            LEFT JOIN information_schema.role_table_grants g
                ON r.rolname = g.grantee
            WHERE r.rolname NOT LIKE 'pg_%'
            GROUP BY r.rolname, r.rolsuper, r.rolinherit, r.rolcreaterole,
                     r.rolcreatedb, r.rolcanlogin, r.rolreplication,
                     r.rolconnlimit, r.rolvaliduntil
        """
        return self.execute_query(query)

    def get_table_access_privileges(self, table_name: str) -> List[Dict[str, Any]]:
        """Get detailed access privileges for a specific table."""
        query = """
            SELECT
                grantee,
                string_agg(privilege_type, ', ') as privileges,
                is_grantable
            FROM information_schema.role_table_grants
            WHERE table_name = %s
            GROUP BY grantee, is_grantable
        """
        return self.execute_query(query, (table_name,))

    # Query Analysis and Optimization
    def analyze_query_plan(self, query: str) -> Dict[str, Any]:
        """Analyze query execution plan with detailed statistics."""
        explain_query = f"""
            EXPLAIN (
                ANALYZE,
                VERBOSE,
                BUFFERS,
                WAL,
                SETTINGS,
                FORMAT JSON
            ) {query}
        """
        try:
            plan = self.execute_query(explain_query, fetch_all=False)
            return json.loads(plan[0])
        except Exception as e:
            self._logger.error(f"Failed to analyze query plan: {str(e)}")
            return None

    # Advanced Maintenance Methods
    def rebuild_table_indexes(self, table_name: str, concurrent: bool = True) -> bool:
        """Rebuild all indexes for a table."""
        try:
            # Get all indexes for the table
            index_query = """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = %s
                AND indexname NOT LIKE '%_pkey'
            """
            indexes = self.execute_query(index_query, (table_name,))

            for index in indexes:
                concurrent_str = "CONCURRENTLY" if concurrent else ""
                self.execute_query(f"REINDEX INDEX {concurrent_str} {index['indexname']}")

            return True
        except Exception as e:
            self._logger.error(f"Failed to rebuild indexes: {str(e)}")
            return False

    def optimize_table_storage(self, table_name: str) -> bool:
        """Optimize table storage through various maintenance operations."""
        try:
            operations = [
                f"VACUUM FULL {table_name}",
                f"ANALYZE {table_name}",
                f"CLUSTER {table_name}",
                f"ALTER TABLE {table_name} SET FILLFACTOR = 90"
            ]

            for operation in operations:
                self.execute_query(operation)

            return True
        except Exception as e:
            self._logger.error(f"Failed to optimize table storage: {str(e)}")
            return False


# Example usage:
if __name__=="__main__":
    # Configuration and initialization
    config = DatabaseConfig(
        host="localhost",
        port=5432,
        database="building_energy_staging_db_v2",
        user="postgres",
        password="postgres"
    )

    db = PostgresManager(config)

    # Create a table
    columns = [
        {'name': 'id', 'type': 'SERIAL', 'nullable': False},
        {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': False},
        {'name': 'email', 'type': 'VARCHAR(255)', 'nullable': False},
        {'name': 'created_at', 'type': 'TIMESTAMP', 'default': 'CURRENT_TIMESTAMP'}
    ]
    db.create_table('users', columns, primary_key='id')

    # Get database health metrics
    db_size = db.get_database_size()
    table_sizes = db.get_table_sizes()
    slow_queries = db.get_slow_queries()

    # Perform maintenance
    db.vacuum_analyze_table('users')

    # Get table information
    table_info = db.get_table_definition('users')
    dependencies = db.get_table_dependencies('users')

    print("Database size:", db_size)
    print("Table sizes:", json.dumps(table_sizes, indent=2))

    # Example usage of advanced features
    # Get database health metrics
    db_size = db.get_database_size()
    table_sizes = db.get_table_sizes()
    cache_stats = db.get_cache_hit_ratios()
    bloat_analysis = db.get_bloat_analysis()

    # Monitor replication
    replication_status = db.get_replication_status()

    # Analyze performance
    missing_indexes = db.get_missing_indexes()
    unused_indexes = db.get_unused_indexes()

    # Security audit
    user_permissions = db.audit_user_permissions()

    print("Database size:", db_size)
    print("Cache hit ratios:", json.dumps(cache_stats, indent=2))
    print("Bloat analysis:", json.dumps(bloat_analysis, indent=2))
