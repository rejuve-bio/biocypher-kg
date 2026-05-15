## Type of change

- [ ] New adapter
- [ ] Adapter fix / update
- [ ] New processor / Processor fix
- [ ] Schema change
- [ ] Adapters config change
- [ ] Writer fix / update
- [ ] CI / workflow change
- [ ] Bug fix / Refactor

---

## Summary

<!-- What changed, why it changed, and any important context for reviewers -->

---

## Checklist

### General
- [ ] PR targets `main` and branch is up to date
- [ ] No hardcoded absolute paths; paths are repo-relative or under `aux_files/` when persistent generated assets are expected
- [ ] No credentials, tokens, or real patient data committed

### New adapter
- [ ] Entry added to `config/hsa/hsa_adapters_config_sample.yaml` with correct `module`, `cls`, and `args`
- [ ] Entry added to `config/hsa/hsa_data_source_config.yaml` with the correct `name` and `url`
- [ ] Sample inputs are committed under `samples/` when tests must run offline; generated/cache assets are under `aux_files/` only when appropriate
- [ ] `nodes` / `edges` flags are correct; dbSNP-related config uses the current mapping-based inputs if needed
- [ ] If this is a heavy ontology adapter, the relevant skip/detection lists used by CI and tests are updated consistently

### Schema changes
- [ ] New labels are validated against BioCypher / Biolink expectations
- [ ] `input_label`, `output_label`, `source`, and `target` match what the adapter actually yields
- [ ] Breaking label, schema, or adapter-arg changes are called out below

### Testing
- [ ] `uv run pytest test/test.py --adapter-test-mode=smoke` passes
- [ ] Ran additional focused checks relevant to the change
- [ ] `uv run pytest test/test.py` passes if heavy ontology, schema-wide, or cache/update behavior changed
- [ ] Spot-checked output node / edge labels and counts where relevant


