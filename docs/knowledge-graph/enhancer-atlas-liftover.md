# EnhancerAtlas 2.0 offline liftover: step by step

Human hg19 → GRCh38 and *Drosophila melanogaster* dm3 → dm6, with
[`scripts/lift_enhancer_atlas.py`](../../scripts/lift_enhancer_atlas.py) v1.7 (standard-library Python 3).
This is a preprocessing step outside the KG build: the adapter reads its output (see
[enhancer-atlas.md](enhancer-atlas.md)). The commands below reproduce the lifted data the adapter was validated on.

In the commands, `IN` is the species' input folder, the `input_dir` of
`config/<species>/<species>_adapters_config.yaml`, and commands run from the repo root.

## 0. Why

EnhancerAtlas 2.0 coordinates are hg19 and dm3 (Gao and Qian, NAR 2020;48:D58-D64). Against the annotation the graph
uses, the TSS inside the enhancer-gene (EP) keys falls within 10 kb of a gene TSS for 5.5% of the human pairs as
distributed and 96.1% after the lift (GENCODE v50); fly 42.8% and 99.0% (Ensembl BDGP6.54).

## 1. What you need

- `python3` and the UCSC `liftOver` binary on `PATH`, or `LIFTOVER=/path/to/liftOver`
  (`wget https://hgdownload.soe.ucsc.edu/admin/exe/linux.x86_64/liftOver`, or `conda install -c bioconda ucsc-liftover`).
- **The EnhancerAtlas download**, made by the download manager into `$IN/enhancer_atlas/non_lifted/`:
  - `bed/<tissue>.bed`: 197 human and 21 fly per-tissue BEDs, from `enhancer.tar.tgz`;
  - `enhancer_gene/<tissue>_EP.txt`: 110 human and 7 fly EP files, with CRLF line ends;
  - `hs.bed.gz` / `dm.bed.gz`: the merged BED. The lift processes it too, but the adapter does not read it.

  ```bash
  make download-direct OUTPUT_DIR=$IN SOURCE=enhancer_atlas                     # human
  make download-direct OUTPUT_DIR=$IN SOURCE=enhancer_atlas \
                       CONFIG_FILE=./config/dmel/dmel_data_source_config.yaml   # fly
  ```
- **Chains**, in `$IN/liftover_chains/` (the default) or given with `CHAIN=`:
  - `hg19ToHg38.over.chain.gz`: the downloader fetches it for dbSUPER. The validated run used md5 `35887f73fe5e2231656504d1f6430900`.
  - `dm3ToDm6.over.chain.gz`: not downloaded by the pipeline; get it from `https://hgdownload.soe.ucsc.edu/goldenPath/dm3/liftOver/`. The validated run used md5 `582f6f4067e41542a4b77a7401dadd45`.
- **Gene annotation** for the current gene ids: the GENCODE main GTF (`$IN/gencode/`) and the Ensembl fly GTF (`$IN/ensembl/`).
- **Fly only:**
  - the FlyBase `fbgn_annotation_ID` table;
  - UCSC `dm6.chromAlias.txt`;
  - [`scripts/dmel/enhancer_atlas_inputs/dm6_contig_name_map.tsv`](../../scripts/dmel/enhancer_atlas_inputs/dm6_contig_name_map.tsv) (contig names);
  - the FlyBase GFF (`$IN/flybase/dmel-all-r6.*.gff.gz`). Release r6.69 is a tar archive with a `.gz` name; the scripts ignore the junk.

## 2. Checks before you start

```bash
which liftOver
md5sum $IN/liftover_chains/*.over.chain.gz
grep -m1 '^VERSION' scripts/lift_enhancer_atlas.py      # must print VERSION = "1.7"
```

The script ignores, without an error, any environment variable it does not know. Always read the first line of
`summary.txt` (the version) and the printed summary.

## 3. Human

```bash
IN=/path/to/input/hsa
mkdir -p gene_ids
zcat $IN/gencode/gencode.v50.annotation.gtf.gz \
  | awk -F'\t' '$3=="gene"{if(match($9,/gene_id "[^"]+"/)){id=substr($9,RSTART+9,RLENGTH-10); sub(/\..*$/,"",id); print id}}' \
  | LC_ALL=C sort -u > gene_ids/hsa_gencode_v50_ids.txt
wc -l gene_ids/hsa_gencode_v50_ids.txt          # 78733
SPECIES=hsa INPUT_DIR=$IN GENE_IDS=$PWD/gene_ids/hsa_gencode_v50_ids.txt python3 scripts/lift_enhancer_atlas.py 2>&1 | tee lift_hsa.log
```

The output goes to `$IN/enhancer_atlas/`. Expected in the summary:

| | Source | Lifted | Not lifted |
|---|---|---|---|
| BED records | 4,035,437 | 4,029,881 | 5,556 dropped |
| Unique intervals | 3,596,268 (69,535 only in EP files) | 3,591,160 | 5,108 unmapped |
| EP pairs | 7,578,822 | 7,224,299 written | 6,104 dropped (the enhancer did not lift) |

`nodes_union.bed` has 3,590,710 rows. Another 348,419 EP pairs, covering 3,635 gene ids, are held out. No map is
applied for human: the ids that GENCODE v50 does not have stay in `enhancer_gene_held/` and `gene_id_report/`.

## 4. Fly

```bash
IN=/path/to/input/dmel
D=scripts/dmel/enhancer_atlas_inputs
```

### 4.1 Contig names (once)

The FlyBase GFF names the dm6 contigs differently from UCSC. `NAME_MODE=flybase` uses `chromAlias`; the 48 contigs that
chromAlias cannot name are mapped by identical sequence. The map is committed in `$D/dm6_contig_name_map.tsv`; to rebuild it:

```bash
mkdir -p dm6_names
curl -s -o dm6_names/dm6.chromAlias.txt https://hgdownload.soe.ucsc.edu/goldenPath/dm6/bigZips/dm6.chromAlias.txt
curl -s -o dm6_names/dm6.fa.gz https://hgdownload.soe.ucsc.edu/goldenPath/dm6/bigZips/dm6.fa.gz
CHROM_ALIAS=$PWD/dm6_names/dm6.chromAlias.txt GFF=$IN/flybase/dmel-all-r6.69.gff.gz DM6_FASTA=$PWD/dm6_names/dm6.fa.gz \
  MAP_OUT=$PWD/dm6_names/dm6_contig_name_map.tsv python3 scripts/dmel/build_dm6_contig_name_map.py
```

Expect 48 contigs with status `verified`. The lift uses only `verified` entries.

### 4.2 Current gene ids and the FlyBase table

```bash
zcat $IN/ensembl/Drosophila_melanogaster.BDGP6.54.63.gtf.gz \
  | awk -F'\t' '$3=="gene"{if(match($9,/gene_id "[^"]+"/)) print substr($9,RSTART+9,RLENGTH-10)}' \
  | LC_ALL=C sort -u > gene_ids/dm_gtf_ids.txt
wc -l gene_ids/dm_gtf_ids.txt                    # 24254
U=https://s3ftp.flybase.org/releases/current/precomputed_files/genes/fbgn_annotation_ID.tsv.gz
for i in $(seq 1 20); do code=$(curl -sL -o gene_ids/fbgn_annotation_ID.tsv.gz -w '%{http_code}' "$U"); echo "try $i: http $code"; [ "$code" = "200" ] && [ -s gene_ids/fbgn_annotation_ID.tsv.gz ] && break; sleep 30; done
```

The server answers HTTP 202 with an empty body while it prepares the file; repeat until 200.

The validated run used `fbgn_annotation_ID_fb_2026_03`. `current` changes over time, so keep the file and its md5.

### 4.3 Pass 1: find the ids that are not current (no map)

```bash
SPECIES=dmel INPUT_DIR=$IN NAME_MODE=flybase CHROM_ALIAS=$PWD/dm6_names/dm6.chromAlias.txt NAME_MAP=$D/dm6_contig_name_map.tsv \
  GFF=$IN/flybase/dmel-all-r6.69.gff.gz GENE_IDS=$PWD/gene_ids/dm_gtf_ids.txt OUT=$PWD/dmel_pass1 \
  python3 scripts/lift_enhancer_atlas.py 2>&1 | tee lift_dmel_pass1.log
```

`dmel_pass1/gene_id_report/gene_id_held.tsv` lists every old id (530 expected).

### 4.4 Build the map

Rule: an old id is mapped when it is a secondary id of exactly one primary id, and that primary is in the current ids.

```bash
HELD=$PWD/dmel_pass1/gene_id_report/gene_id_held.tsv TABLE=$PWD/gene_ids/fbgn_annotation_ID.tsv.gz IDS=$PWD/gene_ids/dm_gtf_ids.txt \
  MAP_OUT=$PWD/gene_ids/dmel_gene_id_map.tsv NOTES_OUT=$PWD/gene_ids/dmel_gene_id_notes.tsv python3 scripts/dmel/build_dmel_gene_id_map.py
```

Expected: 530 ids read, 412 mapped and 118 still held:
- 97 are not in the table;
- 19 are split genes with two candidate primaries;
- 2 have a primary that is not in the GTF.

The committed `$D/dmel_gene_id_map.tsv` and `$D/dmel_gene_id_notes.tsv` hold the same mappings and unresolved ids. The
notes also carry what was checked by hand (e.g. FBgn0085777 is "Withdrawn" in FlyBase). With them, 4.3 and 4.4 can be
skipped.

### 4.5 Final run

```bash
SPECIES=dmel INPUT_DIR=$IN NAME_MODE=flybase CHROM_ALIAS=$PWD/dm6_names/dm6.chromAlias.txt NAME_MAP=$D/dm6_contig_name_map.tsv \
  GFF=$IN/flybase/dmel-all-r6.69.gff.gz \
  GENE_IDS=$PWD/gene_ids/dm_gtf_ids.txt GENE_ID_MAP=$D/dmel_gene_id_map.tsv GENE_ID_NOTES=$D/dmel_gene_id_notes.tsv \
  GENE_ID_COLLISION=dedupe_identical python3 scripts/lift_enhancer_atlas.py 2>&1 | tee lift_dmel.log
```

Expected:
- **BED records:** 294,158 → 272,149 (22,009 dropped).
- **EP pairs (317,588):**
  - 311,176 written;
  - 1,854 dropped (the enhancer did not lift);
  - 4,076 held (118 ids);
  - 482 identical-score duplicates moved to `enhancer_gene_duplicates/`.
- **Gene ids:** 300,903 pairs already current; 10,755 remapped (412 ids).
- **Collisions:** 760 groups, of which 371 still have several lines (with different scores).

`diff -rq dmel_pass1/bed $IN/enhancer_atlas/bed` must print nothing: the gene step does not touch the BEDs.

## 5. Validate (both species)

```bash
# smoke test: nodes, edges, dangling edges, ontology targets
LIFTED=$IN/enhancer_atlas PICKLE=aux_files/hsa/enhancer_atlas_tissues_to_ontology.pkl TAXON=9606 ASSEMBLY=GRCh38 \
  TARGETS_OUT=hsa_targets.tsv uv run python scripts/enhancer_atlas_smoke_test.py
LIFTED=$IN/enhancer_atlas PICKLE=aux_files/dmel/enhancer_atlas_tissues_to_ontology.pkl TAXON=7227 ASSEMBLY=dm6 \
  TARGETS_OUT=dmel_targets.tsv uv run python scripts/enhancer_atlas_smoke_test.py
# ontology terms exist and are not obsolete (repeat for UBERON, CL, CLO, BTO, FBbt, FBdv)
OWL=cl.owl PREFIX=CL TARGETS=hsa_targets.tsv python3 scripts/enhancer_atlas_check_ontology_terms.py
# content fingerprint of a tissue-to-ontology pickle
python3 scripts/enhancer_atlas_pickle_fingerprint.py aux_files/*/enhancer_atlas_tissues_to_ontology.pkl
```

Expected:

| | Human | Fly |
|---|---|---|
| Nodes | 4,546,586 | 272,149 |
| `enhancer_gene` edges | 7,224,299 | 311,176 |
| `active_in` edges | 2,690,577: anatomy 648,775, cell type 364,403, cell line 1,089,661, tissue 587,738 | 272,149: anatomy 195,138, developmental stage 77,011 |

Both species should have 0 duplicated ids and 0 edges from a missing node. The pickles should give
`110 entries 502d0791e1b1` (human) and `21 entries 0ba43d34c135` (fly).

## 6. Record

Keep the printed summary and each `summary.txt`. They record the chain md5, the `liftOver` path, `minMatch`, and the md5
of `GENE_IDS`, `GENE_ID_MAP` and `GENE_ID_NOTES`.

`MINMATCH` (default 0.90) is the lever: a lower value keeps more of the enhancers that map only in part.

## 7. Problems met

- **Unknown variables:** an environment variable the script does not know is ignored silently (an old copy of the script ran a policy it did not have). Check the version line.
- **Placeholders:** in bash, `<file>` is an input redirection. Never paste placeholders in angle brackets.
- **Locale:** with a non-C locale (e.g. `pt_BR`) and `mawk`, use `LC_ALL=C` for `sort` and `join`.
- **FlyBase download:** FlyBase's S3 mirror may answer 202 (empty file) at first; retry.
