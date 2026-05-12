class PipelineError(Exception):
    """Base exception for pipeline errors."""
    pass

class IngestionError(PipelineError):
    """Error raised during data ingestion."""
    pass

class TransformError(PipelineError):
    """Error raised during data transformation."""
    pass
