from src.pipeline.orchestrator import Orchestrator

if __name__ == "__main__":
    # Orchestrator().retrieve_data()
    # Orchestrator().transform_data()
    # Orchestrator().read_parquet_info()
    Orchestrator().load_data()
    # Orchestrator().return_active_sessions({'datname': 'postgres', 'state': 'idle'})
    # Orchestrator().terminate_sessions('building_energy_staging_db_v2')
    # Orchestrator().delete_database('building_energy_staging_db_v2')