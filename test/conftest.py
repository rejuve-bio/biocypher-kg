def pytest_addoption(parser):
    parser.addoption("--adapters-config", action="store", default="config/hsa/hsa_adapters_config_sample.yaml", help="Path to the adapter config file")