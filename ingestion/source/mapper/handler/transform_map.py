def transform_data(raw_data: dict, mapping_config: dict) -> dict:
    """Pre-processing logic based on mapping config."""
    transformed = {}
    for src, tgt in zip(mapping_config.get("source_fields", []), mapping_config.get("target_fields", [])):
        transformed[tgt] = raw_data.get(src)
    return transformed
