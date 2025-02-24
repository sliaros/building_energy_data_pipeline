import pytest
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from unittest.mock import Mock, patch, MagicMock
import logging
from datetime import datetime
import os
from pathlib import Path
import json

from src.postgres_managing.postgres_manager import PostgresManager, DatabaseConfig

# Test configurations
TEST_CONFIG = DatabaseConfig(
    host="localhost",
    port=5432,
    database="test_db",
    user="test_user",
    password="test_password",
    logger=logging.getLogger("test_logger"),
    application_name="TestDBManager"
)

@pytest.fixture
def mock_connection():
    """Fixture for mocked database connection"""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__exit__.return_value = None
    return mock_conn

@pytest.fixture
def mock_pool():
    """Fixture for mocked connection pool"""
    mock = MagicMock()
    mock._used = []
    mock._pool = []
    mock.maxconn = 20
    return mock

@pytest.fixture
def db_manager(mock_pool):
    """Fixture for PostgresManager instance with mocked pool"""
    with patch('psycopg2.pool.SimpleConnectionPool', return_value=mock_pool):
        manager = PostgresManager(TEST_CONFIG)
        manager._pool = mock_pool
        yield manager

class TestPostgresManagerInitialization:
    """Test PostgresManager initialization and configuration"""

    def test_singleton_pattern(self):
        """Test that PostgresManager maintains singleton pattern"""
        manager1 = PostgresManager(TEST_CONFIG)
        manager2 = PostgresManager(TEST_CONFIG)
        assert manager1 is manager2

    def test_temporary_instance(self):
        """Test creation of temporary PostgresManager instance"""
        manager1 = PostgresManager(TEST_CONFIG)
        temp_manager = PostgresManager.create_temporary_instance(TEST_CONFIG)
        assert manager1 is not temp_manager

    def test_config_validation(self):
        """Test configuration validation"""
        with pytest.raises(ValueError):
            DatabaseConfig(
                host="",
                port=5432,
                database="test_db",
                user="test_user",
                password="test_password"
            )

class TestConnectionManagement:
    """Test connection management functionality"""

    def test_get_connection(self, db_manager, mock_pool):
        """Test getting connection from pool"""
        mock_conn = MagicMock()
        mock_pool.getconn.return_value = mock_conn
        mock_conn.closed = False

        connection = db_manager.get_connection()
        assert connection==mock_conn
        mock_pool.getconn.assert_called()
        assert mock_pool.getconn.call_count >= 1

    def test_release_connection(self, db_manager, mock_pool):
        """Test releasing connection back to pool"""
        mock_conn = MagicMock()
        db_manager.release_connection(mock_conn)
        mock_pool.putconn.assert_called()
        assert mock_pool.putconn.call_count >= 1

    def test_connection_context(self, db_manager):
        """Test connection context manager"""
        with patch.object(db_manager, 'get_connection') as mock_get:
            with patch.object(db_manager, 'release_connection') as mock_release:
                mock_conn = MagicMock()
                mock_get.return_value = mock_conn

                with db_manager.connection_context() as conn:
                    assert conn == mock_conn

                mock_get.assert_called_once()
                mock_release.assert_called_once_with(mock_conn)

class TestQueryExecution:
    """Test query execution functionality"""

    def test_execute_query_select(self, db_manager, mock_connection):
        """Test executing SELECT query"""
        mock_cursor = mock_connection.cursor().__enter__()
        mock_cursor.description = True
        mock_cursor.fetchall.return_value = [{'id': 1, 'name': 'test'}]

        with patch.object(db_manager, 'get_connection', return_value=mock_connection):
            result = db_manager.execute_query("SELECT * FROM test")
            assert isinstance(result, list)
            assert len(result) == 1
            assert result[0]['id'] == 1

    def test_execute_query_insert(self, db_manager, mock_connection):
        """Test executing INSERT query"""
        mock_cursor = mock_connection.cursor().__enter__()
        mock_cursor.description = None
        mock_cursor.rowcount = 1

        with patch.object(db_manager, 'get_connection', return_value=mock_connection):
            result = db_manager.execute_query("INSERT INTO test VALUES (1, 'test')")
            assert result == 1

    def test_execute_cached_query(self, db_manager):
        """Test cached query execution"""
        test_query = "SELECT * FROM test_table"
        test_params = ('param1', 'param2')

        with patch.object(db_manager, '_cached_query') as mock_cached:
            mock_cached.return_value = [{'id': 1}]
            result = db_manager.execute_cached_query(test_query, test_params)
            assert result == [{'id': 1}]

class TestTableManagement:
    """Test table management functionality"""

    def test_table_exists(self, db_manager):
        """Test checking if table exists"""
        with patch.object(db_manager, 'execute_query') as mock_execute:
            mock_execute.return_value = {'exists': True}
            assert db_manager.table_exists('test_table') is True

    def test_create_table_from_schema(self, db_manager):
        """Test creating table from schema"""
        schema = """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        );
        """
        with patch.object(db_manager, 'execute_query') as mock_execute:
            mock_execute.return_value = None
            assert db_manager.create_table_from_schema(schema, 'test_table') is True

    def test_drop_table(self, db_manager):
        """Test dropping table"""
        with patch.object(db_manager, 'execute_query') as mock_execute:
            mock_execute.side_effect = [{'to_regclass': True}, None]
            assert db_manager.drop_table('test_table') is True

class TestDatabaseMaintenance:
    """Test database maintenance functionality"""

    def test_vacuum_analyze_table(self, db_manager, mock_connection):
        """Test vacuum analyze operation"""
        with patch.object(db_manager, 'get_connection', return_value=mock_connection):
            db_manager.vacuum_analyze_table('test_table')
            mock_connection.set_isolation_level.assert_called_with(ISOLATION_LEVEL_AUTOCOMMIT)

    def test_reindex_table(self, db_manager):
        """Test reindex table operation"""
        with patch.object(db_manager, 'execute_query') as mock_execute:
            mock_execute.return_value = None
            assert db_manager.reindex_table('test_table') is True

class TestDatabaseMonitoring:
    """Test database monitoring functionality"""

    def test_get_database_size(self, db_manager):
        """Test getting database size"""
        expected_result = {'datname': 'test_db', 'size': '100 MB', 'size_bytes': 104857600}
        with patch.object(db_manager, 'execute_query') as mock_execute:
            mock_execute.return_value = expected_result
            result = db_manager.get_database_size()
            assert result == expected_result

    def test_get_slow_queries(self, db_manager):
        """Test getting slow queries"""
        expected_result = [{'pid': 1, 'query': 'SELECT * FROM test', 'duration': '5 seconds'}]
        with patch.object(db_manager, 'execute_query') as mock_execute:
            mock_execute.return_value = expected_result
            result = db_manager.get_slow_queries()
            assert result == expected_result

class TestSecurityFeatures:
    """Test security-related functionality"""

    def test_audit_user_permissions(self, db_manager):
        """Test auditing user permissions"""
        expected_result = [{'rolname': 'test_user', 'privileges': ['SELECT', 'INSERT']}]
        with patch.object(db_manager, 'execute_query') as mock_execute:
            mock_execute.return_value = expected_result
            result = db_manager.audit_user_permissions()
            assert result == expected_result

    def test_get_table_access_privileges(self, db_manager):
        """Test getting table access privileges"""
        expected_result = [{'grantee': 'test_user', 'privileges': 'SELECT, INSERT'}]
        with patch.object(db_manager, 'execute_query') as mock_execute:
            mock_execute.return_value = expected_result
            result = db_manager.get_table_access_privileges('test_table')
            assert result == expected_result

class TestBackupRestore:
    """Test backup and restore functionality"""

    def test_create_backup(self, db_manager):
        """Test creating database backup"""
        with patch('subprocess.run') as mock_run:
            mock_run.return_value.returncode = 0
            assert db_manager.create_backup('/path/to/backup.sql') is True

class TestUtilityMethods:
    """Test utility methods"""

    def test_get_table_definition(self, db_manager):
        """Test getting table definition"""
        expected_result = {
            'columns': [{'column_name': 'id', 'data_type': 'integer'}],
            'constraints': [{'constraint_name': 'pk_test', 'definition': 'PRIMARY KEY (id)'}],
            'indexes': [{'indexname': 'idx_test', 'indexdef': 'CREATE INDEX idx_test ON test_table(id)'}]
        }
        with patch.object(db_manager, 'execute_query') as mock_execute:
            mock_execute.side_effect = [
                expected_result['columns'],
                expected_result['constraints'],
                expected_result['indexes']
            ]
            result = db_manager.get_table_definition('test_table')
            assert result == expected_result

def test_integration(db_manager):
    """Integration test combining multiple operations"""
    # This would be marked with pytest.mark.integration in a real test suite
    with patch.object(db_manager, 'execute_query') as mock_execute:
        # Create table
        mock_execute.return_value = None
        assert db_manager.create_table_from_schema(
            "CREATE TABLE test_table (id SERIAL PRIMARY KEY);",
            "test_table"
        ) is True

        # Insert data
        mock_execute.return_value = 1
        result = db_manager.execute_query(
            "INSERT INTO test_table (id) VALUES (1)"
        )
        assert result == 1

        # Query data
        mock_execute.return_value = [{'id': 1}]
        result = db_manager.execute_query(
            "SELECT * FROM test_table"
        )
        assert len(result) == 1
        assert result[0]['id'] == 1

        # Drop table
        mock_execute.side_effect = [{'to_regclass': True}, None]
        assert db_manager.drop_table('test_table') is True

if __name__ == '__main__':
    pytest.main(['-v'])