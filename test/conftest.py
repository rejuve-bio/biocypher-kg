def pytest_addoption(parser):
    parser.addoption(
        "--adapters-config",
        action="store",
        default="config/hsa/hsa_adapters_config_sample.yaml",
        help="Path to the adapters config YAML file"
    )
    parser.addoption(
        "--primer-schema-config",
        action="store",
        default="config/primer_schema_config.yaml",
        help="Path to the primer (base) schema config YAML file"
    )
    parser.addoption(
        "--species-schema-config",
        action="store",
        default="config/hsa/hsa_schema_config.yaml",
        help="Path to the species-specific schema config YAML file"
    )
    parser.addoption(
        "--adapter-test-mode",
        action="store",
        choices=["smoke", "full"],
        default="full",
        help="Adapter test depth: 'smoke' skips heavy adapters and limits samples; 'full' runs everything."
    )
    parser.addoption(
        "--adapter-max-adapters",
        action="store",
        type=int,
        default=25,
        help="Maximum number of adapters to sample per node/edge test in smoke mode."
    )
    parser.addoption(
        "--adapter-profile",
        action="store_const",
        const=True,
        dest="adapter_profile",
        default=True,
        help="Enable per-adapter runtime profiling output."
    )
    parser.addoption(
        "--no-adapter-profile",
        action="store_const",
        const=False,
        dest="adapter_profile",
        help="Disable per-adapter runtime profiling output."
    )
