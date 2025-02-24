from src.orchestrator.orchestrator import Orchestrator

if __name__ == "__main__":
    orch = Orchestrator("demo_database")
    orch.retrieve_data()
    orch.transform_data()
    orch.read_parquet_info()
    orch.load_data()
    print(orch.return_active_sessions({'state': 'active'}))
    orch.terminate_sessions('postgres')
    orch.delete_database('demo_db')
    pass