from src.pipeline.orchestrator import Orchestrator

if __name__ == "__main__":
    orch = Orchestrator("demo_database")
    # Orchestrator().retrieve_data()
    # Orchestrator().transform_data()
    # Orchestrator().read_parquet_info()
    # Orchestrator().load_data()
    # print(orch.return_active_sessions({'state': 'active'}))
    # orch.terminate_sessions('postgres')
    # orch.delete_database('demo_db')
    pass