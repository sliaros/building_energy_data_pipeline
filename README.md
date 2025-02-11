from pathlib import Path
from typing import Optional
import argparse
from logging_config import CustomLogger
from smart_sampler import ProgressiveSmartSampler, SamplingConfig
from schema_analyzer import EnhancedSchemaAnalyzer
from sql_generator import EnhancedSQLGenerator

class SchemaGenerationOrchestrator:
    """Orchestrates the entire schema generation process"""
    
    def __init__(
        self,
        input_file: Path,
        output_file: Optional[Path] = None,
        table_name: Optional[str] = None,
        schema_name: str = "public",
        sampling_config: Optional[SamplingConfig] = None
    ):
        self.input_file = Path(input_file)
        self.output_file = output_file or self.input_file.with_suffix('.sql')
        self.table_name = table_name or self.input_file.stem
        self.schema_name = schema_name
        self.sampling_config = sampling_config or SamplingConfig()
        self.logger = CustomLogger(__name__).get_logger()
    
    def generate(self) -> str:
        """Execute the schema generation process"""
        self.logger.info(f"Starting schema generation for {self.input_file}")
        
        # Sample data
        sampler = ProgressiveSmartSampler(self.sampling_config)
        sample_result = sampler.sample(self.input_file)
        
        # Analyze schema
        analyzer = EnhancedSchemaAnalyzer(
            sample_result.sample_data,
            sample_result.schema
        )
        columns = analyzer.analyze()
        
        # Generate SQL
        generator = EnhancedSQLGenerator(
            self.table_name,
            self.schema_name
        )
        sql = generator.generate(columns)
        
        # Save to file if specified
        if self.output_file:
            self.output_file.write_text(sql)
            self.logger.info(f"SQL schema saved to {self.output_file}")
        
        return sql

def main():
    """Command line interface for schema generation"""
    parser = argparse.ArgumentParser(
        description="Generate SQL schema from CSV or Parquet