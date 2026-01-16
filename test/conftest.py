def pytest_addoption(parser):
    parser.addoption("--adapters-config", action="store", default="config/hsa/hsa_adapters_config_sample.yaml", help="Path to the adapter config file")
    parser.addoption("--dbsnp-rsids", action="store", help="Path to the dbsnp rsids file")
    parser.addoption("--dbsnp-pos", action="store", help="Path to the dbsnp pos file")
    parser.addoption("--schema-config", action="store", default="config/hsa/hsa_schema_config.yaml", help="Path to the schema config file")