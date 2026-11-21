import csv
import math
import os
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Any

from biocypher._logger import logger
from biocypher_metta.adapters import Adapter


class JasparMotifAdapter(Adapter):
    """
    BioCypher adapter for Drosophila melanogaster transcription factor binding motifs
    from JASPAR 2026 (CORE insects collection).

    Reads the JASPAR MEME file (letter-probability matrices / PPM) and the Drosophila
    motif mapping file, computes log-odds Position Weight Matrices (PWM), and yields
    'motif' node records:
        (_id, label, properties)

    Key Biological and Technical Guarantees:
      1. Node Identifiers:
         Every generated motif node uses the corresponding Drosophila transcription factor
         gene identifier as its bare node ID (e.g. 'FBgn0000541').
         No prefixes (no 'ENSEMBL:', 'JASPAR:', 'MOTIF:', etc.) are added.
      2. Heterodimer Processing:
         Heterodimer motifs with multiple partner transcription factor genes (e.g. EcR::usp)
         produce separate motif nodes for each partner gene (e.g. one for EcR FBgn0000546
         and one for usp FBgn0003964). No combined node is ever emitted.
      3. Deterministic Deduplication:
         When multiple JASPAR matrices in the redundant collection map to the same FBgn gene,
         conflicting duplicate node IDs are deterministically resolved by ranking:
           - Monomer motifs preferred over heterodimers (n_partners == 1 > n_partners > 1)
           - Higher matrix version (e.g. MA0010.2 > MA0010.1)
           - Matrix ID string (lexicographic tie-breaker)
      4. PWM Calculation:
         Converts letter probabilities P_ij into natural log-odds Position Weight Matrices:
           PWM_ij = ln(max(P_ij, floor) / background_j)
         Matching the conventions expected by BioCypher motif nodes and HoCoMoCoMotifAdapter.
    """

    def __init__(
        self,
        filepath: str,
        motif_map_path: str,
        label: str = "motif",
        write_properties: bool = True,
        add_provenance: bool = True,
        taxon_id: int = 7227,
        floor: float = 1e-4,
        deduplicate: bool = True,
    ):
        """
        Initialize the JasparMotifAdapter.

        Args:
            filepath: Path to the JASPAR MEME file (e.g. jaspar/dmel_jaspar_v4.meme).
            motif_map_path: Path to the motif mapping TSV (e.g. jaspar/dmel_motif_map_v4.tsv).
            label: Node label, default 'motif'.
            write_properties: Whether to include node properties.
            add_provenance: Whether to include 'source' and 'source_url' in properties.
            taxon_id: Taxon ID, default 7227 for Drosophila melanogaster.
            floor: Probability floor for zero/near-zero values to avoid log(0), default 1e-4.
            deduplicate: Whether to deduplicate multiple motifs mapping to the same gene, default True.
        """
        if write_properties is None:
            write_properties = True
        if add_provenance is None:
            add_provenance = True

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"JASPAR MEME file not found: {filepath}")
        if not os.path.exists(motif_map_path):
            raise FileNotFoundError(f"Motif map file not found: {motif_map_path}")
        if floor <= 0:
            raise ValueError(f"Probability floor must be positive, got {floor}")

        self.filepath = filepath
        self.motif_map_path = motif_map_path
        self.label = label
        self.taxon_id = int(taxon_id)
        self.floor = float(floor)
        self.deduplicate = deduplicate

        self.source = "JASPAR2026"
        self.version = "2026"
        self.source_url = "https://jaspar.elixir.no"

        self.motif_map: Dict[str, Dict[str, Any]] = {}
        self.header_to_matrix_id: Dict[str, str] = {}
        self._load_motif_mapping()

        super(JasparMotifAdapter, self).__init__(write_properties, add_provenance)

    def _load_motif_mapping(self) -> None:
        """
        Loads and validates the Drosophila motif mapping TSV.
        Required columns:
          - motif_name_in_diff_header
          - matrix_id
          - symbol
          - uniprot_ids
          - fbgns
          - n_partners
          - status
        """
        required_cols = {
            "motif_name_in_diff_header",
            "matrix_id",
            "symbol",
            "uniprot_ids",
            "fbgns",
            "n_partners",
            "status",
        }

        with open(self.motif_map_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            if not reader.fieldnames or not required_cols.issubset(reader.fieldnames):
                missing = sorted(required_cols - set(reader.fieldnames or []))
                raise ValueError(
                    f"Malformed motif map file '{self.motif_map_path}': missing required column(s): {missing}"
                )

            for row_num, row in enumerate(reader, start=2):
                matrix_id = (row.get("matrix_id") or "").strip()
                header = (row.get("motif_name_in_diff_header") or "").strip()
                if not matrix_id:
                    continue

                status = (row.get("status") or "").strip()
                fbgns_raw = (row.get("fbgns") or "").strip()
                fbgns = [g.strip() for g in fbgns_raw.split(";") if g.strip()]

                # Validate FBgn format if fbgns present
                for fbgn in fbgns:
                    if not fbgn.startswith("FBgn"):
                        logger.warning(
                            f"Row {row_num}: gene identifier '{fbgn}' does not follow expected 'FBgn...' prefix format"
                        )

                try:
                    n_partners = int(row.get("n_partners", len(fbgns)))
                except (ValueError, TypeError):
                    n_partners = len(fbgns)

                if fbgns and len(fbgns) != n_partners:
                    logger.warning(
                        f"Row {row_num} (motif {matrix_id}): declared n_partners={n_partners} but found {len(fbgns)} fbgns: {fbgns}"
                    )

                # Partner symbols extraction
                symbol_raw = (row.get("symbol") or "").strip()
                if "::" in symbol_raw and len(fbgns) > 1:
                    symbols = [s.strip() for s in symbol_raw.split("::")]
                    if len(symbols) == len(fbgns):
                        partner_symbols = symbols
                    else:
                        partner_symbols = [symbol_raw] * len(fbgns)
                else:
                    partner_symbols = [symbol_raw] * len(fbgns)

                uniprot_ids = [u.strip() for u in (row.get("uniprot_ids") or "").split(";") if u.strip()]

                entry = {
                    "motif_name_in_diff_header": header,
                    "matrix_id": matrix_id,
                    "symbol": symbol_raw,
                    "uniprot_ids": uniprot_ids,
                    "fbgns": fbgns,
                    "partner_symbols": partner_symbols,
                    "n_partners": n_partners,
                    "status": status,
                }
                self.motif_map[matrix_id] = entry
                if header:
                    self.header_to_matrix_id[header] = matrix_id

    def parse_meme_file(self) -> Dict[str, Dict[str, Any]]:
        """
        Parses the JASPAR MEME file.
        Extracts background nucleotide frequencies, motif headers, matrix dimensions,
        and converts letter-probability matrices (PPM) into log-odds Position Weight Matrices (PWM).

        Returns:
            Dict[matrix_id, {
                'matrix_id': str,
                'symbol': str,
                'pwm': {'pwm_A': List[float], 'pwm_C': List[float], 'pwm_G': List[float], 'pwm_T': List[float]},
                'length': int
            }]
        """
        bg_freqs = {"A": 0.25, "C": 0.25, "G": 0.25, "T": 0.25}
        motifs: Dict[str, Dict[str, Any]] = {}

        with open(self.filepath, "r", encoding="utf-8") as f:
            lines = f.readlines()

        if not lines or not any(l.strip() for l in lines):
            logger.warning(f"MEME file '{self.filepath}' is empty.")
            return motifs

        # 1. Parse background frequencies if present in header
        in_bg = False
        for line in lines:
            stripped = line.strip()
            if "Background letter frequencies" in line:
                in_bg = True
                continue
            if in_bg:
                if stripped.startswith("MOTIF") or not stripped:
                    if stripped.startswith("MOTIF"):
                        in_bg = False
                    continue
                parts = stripped.split()
                for k in range(0, len(parts) - 1, 2):
                    letter, freq_str = parts[k], parts[k + 1]
                    if letter in bg_freqs:
                        try:
                            val = float(freq_str)
                            if val > 0:
                                bg_freqs[letter] = val
                        except ValueError:
                            pass
                in_bg = False

        # 2. Parse MOTIF blocks and letter-probability matrices
        i = 0
        n_lines = len(lines)
        while i < n_lines:
            line = lines[i].strip()
            if line.startswith("MOTIF"):
                tokens = line.split()
                if len(tokens) < 2:
                    i += 1
                    continue
                matrix_id = tokens[1]
                motif_symbol = tokens[2] if len(tokens) >= 3 else ""

                i += 1
                w: Optional[int] = None
                while i < n_lines and not lines[i].strip().startswith("MOTIF"):
                    curr = lines[i].strip()
                    if "letter-probability matrix:" in curr:
                        w_match = re.search(r"\bw=\s*(\d+)", curr)
                        if not w_match:
                            raise ValueError(
                                f"Malformed letter-probability matrix header in motif {matrix_id}: missing 'w='"
                            )
                        w = int(w_match.group(1))

                        alength_match = re.search(r"\balength=\s*(\d+)", curr)
                        if alength_match and int(alength_match.group(1)) != 4:
                            raise ValueError(
                                f"Unsupported alphabet length {alength_match.group(1)} in motif {matrix_id}; expected 4"
                            )

                        i += 1
                        break
                    i += 1

                if w is None:
                    continue

                rows: List[List[float]] = []
                while i < n_lines and len(rows) < w:
                    row_str = lines[i].strip()
                    i += 1
                    if not row_str or row_str.startswith("URL") or row_str.startswith("#"):
                        continue
                    vals = row_str.split()
                    if len(vals) != 4:
                        raise ValueError(
                            f"Malformed PWM row in motif {matrix_id}: expected 4 values, got {len(vals)}: '{row_str}'"
                        )
                    try:
                        prob_vals = [float(v) for v in vals]
                    except ValueError as exc:
                        raise ValueError(
                            f"Invalid float in PWM row for motif {matrix_id}: {exc}"
                        ) from exc

                    for v in prob_vals:
                        if math.isnan(v) or v < 0.0 or v > 1.05:
                            raise ValueError(
                                f"Invalid nucleotide probability {v} in motif {matrix_id}"
                            )

                    rows.append(prob_vals)

                if len(rows) != w:
                    raise ValueError(
                        f"Motif {matrix_id} declared length w={w} but contained {len(rows)} matrix rows"
                    )

                # Convert probabilities to log-odds PWM
                # Column ordering: A, C, G, T
                bases = ["A", "C", "G", "T"]
                pwm = {"pwm_A": [], "pwm_C": [], "pwm_G": [], "pwm_T": []}
                for row_vals in rows:
                    for base_idx, base in enumerate(bases):
                        p = row_vals[base_idx]
                        p_eff = max(p, self.floor)
                        bg = bg_freqs.get(base, 0.25)
                        if bg <= 0:
                            raise ValueError(
                                f"Background frequency for {base} must be positive, got {bg}"
                            )
                        log_odds = math.log(p_eff / bg)
                        pwm[f"pwm_{base}"].append(float(log_odds))

                motifs[matrix_id] = {
                    "matrix_id": matrix_id,
                    "symbol": motif_symbol,
                    "pwm": pwm,
                    "length": w,
                }
            else:
                i += 1

        return motifs

    def get_nodes(self):
        """
        Yields BioCypher motif node records:
            (_id, label, properties)

        Where _id is the bare FBgn identifier (e.g. 'FBgn0000541').
        Heterodimers produce separate node records for each partner gene.
        """
        motifs = self.parse_meme_file()
        if not motifs:
            return

        # Group candidate records by gene FBgn to resolve multiple matrix versions / duplicates
        candidates_by_gene: Dict[str, List[Tuple[Tuple[int, int, str], Tuple[str, str, Dict[str, Any]], str]]] = (
            defaultdict(list)
        )

        for matrix_id, motif_data in motifs.items():
            map_entry = self.motif_map.get(matrix_id)
            if not map_entry:
                header_key = f"{matrix_id} {motif_data['symbol']}".strip()
                if header_key in self.header_to_matrix_id:
                    map_entry = self.motif_map.get(self.header_to_matrix_id[header_key])

            if not map_entry:
                logger.warning(f"JASPAR motif '{matrix_id}' not found in motif map; skipping.")
                continue

            if map_entry.get("status") != "ok":
                logger.debug(
                    f"Motif '{matrix_id}' has status '{map_entry.get('status')}'; skipping."
                )
                continue

            fbgns = map_entry.get("fbgns", [])
            if not fbgns:
                logger.debug(f"Motif '{matrix_id}' has no mapped FBgn gene IDs; skipping.")
                continue

            partner_symbols = map_entry.get("partner_symbols", [])
            n_partners = map_entry.get("n_partners", len(fbgns))

            # Parse matrix version for ranking (e.g. MA0010.2 -> 2)
            version = 0
            if "." in matrix_id:
                ver_part = matrix_id.split(".")[-1]
                if ver_part.isdigit():
                    version = int(ver_part)

            # Deterministic priority key for multiple matrices of the same gene:
            # 1. Monomer profile (n_partners == 1) preferred over heterodimer (1 > 0)
            # 2. Newer matrix version (e.g. version 2 > version 1)
            # 3. Lexicographical matrix ID tie-breaker
            priority = (1 if n_partners == 1 else 0, version, matrix_id)

            # Emit separate node per partner gene
            for idx, fbgn in enumerate(fbgns):
                tf_name = (
                    partner_symbols[idx]
                    if idx < len(partner_symbols)
                    else map_entry.get("symbol", "")
                )

                props = {}
                if self.write_properties:
                    props = {
                        "tf_name": tf_name,
                        "pwm_A": motif_data["pwm"]["pwm_A"],
                        "pwm_C": motif_data["pwm"]["pwm_C"],
                        "pwm_G": motif_data["pwm"]["pwm_G"],
                        "pwm_T": motif_data["pwm"]["pwm_T"],
                        "length": motif_data["length"],
                        "taxon_id": self.taxon_id,
                    }
                    if self.add_provenance:
                        props["source"] = self.source
                        props["source_url"] = self.source_url

                node_record = (fbgn, self.label, props)
                candidates_by_gene[fbgn].append((priority, node_record, matrix_id))

        if not self.deduplicate:
            for fbgn, candidates in candidates_by_gene.items():
                for _, node_record, _ in candidates:
                    yield node_record
            return

        # Deduplicate deterministically: yield highest-priority node per FBgn
        for fbgn, candidates in sorted(candidates_by_gene.items()):
            if len(candidates) == 1:
                yield candidates[0][1]
            else:
                candidates.sort(key=lambda x: x[0], reverse=True)
                _, chosen_node, chosen_matrix_id = candidates[0]
                other_matrix_ids = [c[2] for c in candidates[1:]]
                logger.info(
                    f"Gene {fbgn} mapped to multiple JASPAR motifs; selected {chosen_matrix_id} over {other_matrix_ids}"
                )
                yield chosen_node

    def get_edges(self):
        """Motif adapter generates nodes only; yields no edges."""
        return []

