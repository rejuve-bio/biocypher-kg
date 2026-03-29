import os
import threading
from pathlib import Path
from urllib.parse import unquote

class WalMORK:
    """
    Durability layer for MORK. Tees every write to a local MeTTa file.
    Merged into the main client to provide unified persistence support.
    """
    def __init__(self, base_mork, wal_path=None, sync_writes=True, host_data_dir=None, lock=None):
        self._mork = base_mork
        if wal_path is None:
            snapshot_dir = os.environ.get("SNAPSHOT_DIR", os.path.join(os.path.dirname(__file__), "mork_persist"))
            wal_path = os.path.join(snapshot_dir, "wal.metta")
        self.wal_path = Path(wal_path).resolve()
        self.wal_path.parent.mkdir(parents=True, exist_ok=True)
        self.sync_writes = sync_writes
        self.host_data_dir = host_data_dir
        self._wal_lock = lock or threading.RLock()
        
    def __getattr__(self, name):
        return getattr(self._mork, name)

    def __enter__(self):
        self._mork.__enter__()
        return self

    def __exit__(self, *args):
        return self._mork.__exit__(*args)

    def _wal_append(self, text):
        if not text: return
        # Ensure entry ends with a newline so entries don't merge
        if not text.endswith("\n"):
            text += "\n"
        with self._wal_lock:
            with open(self.wal_path, "a", encoding="utf-8") as f:
                f.write(text)
                f.flush()
                if self.sync_writes:
                    os.fsync(f.fileno())

    def _wal_append_file(self, file_uri):
        if not file_uri.startswith("file://"): return
        # Decode URL-encoded characters (e.g., %3A -> :)
        local_path = unquote(file_uri[len("file://"):])
        # ALWAYS translate /app/data to host_data_dir if we are on the host
        clean_path = local_path.replace("//", "/") # Handle file:///app/data -> //app/data
        if clean_path.startswith("/app/data") and self.host_data_dir:
            suffix = clean_path[len("/app/data"):].lstrip("/")
            local_path = str(Path(self.host_data_dir) / suffix)

        try:
            with open(local_path, "r", encoding="utf-8") as f:
                self._wal_append(f.read())
        except Exception as e:
            print(f"[WalMORK] WARNING: could not tee {file_uri} to WAL: {e}")

    def upload_(self, data):
        return self.upload("$x", "$x", data)

    def upload(self, pattern, template, data):
        self._wal_append(data)
        return self._mork.upload(pattern, template, data)

    def sexpr_import_(self, file_uri):
        return self.sexpr_import("$x", "$x", file_uri)

    def sexpr_import(self, pattern, template, file_uri):
        self._wal_append_file(file_uri)
        return self._mork.sexpr_import(pattern, template, file_uri)

    def paths_import_(self, file_uri):
        return self.paths_import("$x", "$x", file_uri)

    def paths_import(self, pattern, template, file_uri):
        # tee .metta sibling if it exists
        if file_uri.startswith("file://"):
            m_path = unquote(file_uri.replace(".paths", ".metta")[len("file://"):])
            # Handle host/container path translation
            host_m_path = m_path
            clean_path = m_path.replace("//", "/")
            if clean_path.startswith("/app/data") and self.host_data_dir:
                suffix = clean_path[len("/app/data"):].lstrip("/")
                host_m_path = str(Path(self.host_data_dir) / suffix)
            
            if os.path.exists(host_m_path):
                self._wal_append_file("file://" + m_path)
            else:
                self._wal_append_file(file_uri)
        return self._mork.paths_import(pattern, template, file_uri)

    def clear(self):
        # Truncate the WAL so that operations prior to this clear()
        # are not replayed during crash recovery.
        with self._wal_lock:
            try:
                with open(self.wal_path, "w", encoding="utf-8") as f:
                    f.truncate(0)
                    f.flush()
                    if self.sync_writes:
                        os.fsync(f.fileno())
              
                # Also truncate snapshot file if it exists to prevent old data recovery on restart
                snapshot_path = self.wal_path.parent / "snapshot.paths"
                if snapshot_path.exists():
                    with open(snapshot_path, "wb") as f:
                        f.truncate(0)
                    print(f"[WalMORK] Snapshot file truncated to ensure persistent clear.")
                return self._mork.clear()
            except Exception as e:
                print(f"[WalMORK] Warning: Failed to truncate persistence files on clear: {e}")
                return self._mork.clear()

    def work_at(self, *args, **kwargs):
        child_mork = self._mork.work_at(*args, **kwargs)
        return WalMORK(child_mork, wal_path=self.wal_path, sync_writes=self.sync_writes, host_data_dir=self.host_data_dir, lock=self._wal_lock)
