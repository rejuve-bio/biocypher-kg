import csv
import gzip
from biocypher_metta.adapters import Adapter
from biocypher_metta.adapters.helpers import build_regulatory_region_id

"""
- Sample data from CATLAS cCRE dataset:

#Chromosome     hg38_Start      hg38_End        Class   Present in fetal tissues        Present in adult tissues        CRE module
chr1    9955    10355   Promoter Proximal       yes     yes     146
chr1    29163   29563   Promoter        yes     yes     37
chr1    79215   79615   Distal  no      yes     75
chr1    102755  103155  Distal  no      yes     51
chr1    115530  115930  Distal  yes     no      36
chr1    180580  180980  Promoter Proximal       no      yes     146
chr1    181273  181673  Promoter Proximal       no      yes     146
"""


class CAtlasCCREAdapter(Adapter):
    """
    - Class mapping:
      * Distal -> enhancer
      * Promoter / Promoter Proximal -> promoter
    """

    _PROMOTER_CLASSES = {"promoter", "promoter proximal"}
    _ENHANCER_CLASSES = {"distal"}

    def __init__(
        self,
        filepath,
        write_properties,
        add_provenance,
        label=None,
        taxon_id=9606,
        class_filter=None,
        input_coordinate_system="bed"
    ):
        self.filepath = filepath
        self.label = label
        self.taxon_id = str(taxon_id) if taxon_id is not None else None
        self.class_filter = class_filter.lower().strip() if isinstance(class_filter, str) else None
        self.input_coordinate_system = input_coordinate_system
        self.source = "CATLAS"
        self.source_url = "https://catlas.org/"

        super(CAtlasCCREAdapter, self).__init__(write_properties, add_provenance)

    def _passes_class_filter(self, cls_value):
        if not self.class_filter:
            return True

        cls_lc = cls_value.lower().strip()

        if self.class_filter in {"promoter", "proximal", "promoter_proximal"}:
            return cls_lc in self._PROMOTER_CLASSES

        if self.class_filter in {"enhancer", "distal"}:
            return cls_lc in self._ENHANCER_CLASSES

        return cls_lc == self.class_filter

    def _open_file(self):
        if self.filepath.endswith(".gz"):
            return gzip.open(self.filepath, "rt")
        return open(self.filepath, "rt")

    @staticmethod
    def _to_bool(value):
        return str(value).strip().lower() in {"yes", "true", "1", "y"}

    def _normalize_coordinates(self, start, end):
        if self.input_coordinate_system == "bed":
            # 0-based start, 1-based end
            return start + 1, end
        if self.input_coordinate_system == "zero_based_closed":
            return start + 1, end + 1
        # one_based_closed
        return start, end

    def _class_to_label(self, cls_value):
        cls = cls_value.lower().strip()
        if self.label:
            return self.label
        if cls in self._ENHANCER_CLASSES:
            return "enhancer"
        if cls in self._PROMOTER_CLASSES:
            return "promoter"
        return None

    def _iter_records(self):
        with self._open_file() as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                if not row:
                    continue

                chrom = row.get("#Chromosome") or row.get("Chromosome")
                cls = (row.get("Class") or "").strip()
                start_raw = row.get("hg38_Start")
                end_raw = row.get("hg38_End")

                if not chrom or not cls or start_raw is None or end_raw is None:
                    continue

                if not self._passes_class_filter(cls):
                    continue

                try:
                    start = int(start_raw)
                    end = int(end_raw)
                except ValueError:
                    continue

                start, end = self._normalize_coordinates(start, end)
                node_label = self._class_to_label(cls)

                if node_label is None:
                    continue

                module_value = row.get("CRE module")
                try:
                    cre_module = int(module_value) if module_value not in (None, "") else None
                except ValueError:
                    cre_module = None

                yield {
                    "chr": chrom,
                    "start": start,
                    "end": end,
                    "class": cls,
                    "present_in_fetal_tissues": self._to_bool(row.get("Present in fetal tissues", "")),
                    "present_in_adult_tissues": self._to_bool(row.get("Present in adult tissues", "")),
                    "cre_module": cre_module,
                    "label": node_label,
                }

    def get_nodes(self):
        for record in self._iter_records():

            node_id = build_regulatory_region_id(record["chr"], record["start"], record["end"])
            props = {}
            if self.write_properties:
                props = {
                    "chr": record["chr"],
                    "start": record["start"],
                    "end": record["end"],
                    "class": record["class"],
                    "present_in_fetal_tissues": record["present_in_fetal_tissues"],
                    "present_in_adult_tissues": record["present_in_adult_tissues"],
                    "cre_module": record["cre_module"],
                }

                if self.add_provenance:
                    props["source"] = self.source
                    props["source_url"] = self.source_url

            yield node_id, record["label"], props

    def get_edges(self):
        # CATLAS sample file provides region annotations only (no gene links).
        if False:
            yield
