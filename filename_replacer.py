import os
import tempfile
from typing import Optional, Tuple, Dict


def _is_probably_binary(file_path: str, sniff_bytes: int = 2048) -> bool:
    """
    Quick heuristic to detect binary files by scanning the first bytes.
    Returns True if the file likely contains binary data (e.g., NUL bytes).
    """
    try:
        with open(file_path, "rb") as f:
            chunk = f.read(sniff_bytes)
        if b"\x00" in chunk:
            return True
        # Heuristic: if too many non-text bytes, treat as binary
        text_chars = bytes(range(32, 127)) + b"\n\r\t\b\f"
        non_text = sum(byte not in text_chars for byte in chunk)
        return (len(chunk) > 0) and (non_text / max(1, len(chunk)) > 0.30)
    except Exception:
        # If we can't read the file safely, assume binary to avoid corruption
        return True


def _try_read_text(file_path: str, encodings: Tuple[str, ...]) -> Optional[Tuple[str, str]]:
    """
    Attempt to read a text file using a list of encodings.
    Returns (content, encoding) on success, or None on failure.
    """
    for enc in encodings:
        try:
            with open(file_path, "r", encoding=enc) as f:
                content = f.read()
            return content, enc
        except UnicodeDecodeError:
            continue
        except Exception:
            # Any other error (permissions, etc.) -> give up
            return None
    return None


def _detect_newline(content: str) -> str:
    """
    Infer the newline style from the content to preserve original line endings.
    """
    if "\r\n" in content:
        return "\r\n"
    if "\r" in content and "\n" not in content:
        return "\r"
    return "\n"


def replace_all(directory: str, to_be_replaced: str, surrogate: str) -> Dict[str, int]:
    """
    Recursively replace all occurrences of `to_be_replaced` with `surrogate`
    across all files within `directory` and its subdirectories.

    - Skips symbolic links and files likely to be binary.
    - Tries multiple common encodings and preserves the one used for reading.
    - Preserves the original newline style (LF, CRLF, or CR).
    - Writes changes atomically to avoid partial writes.

    Returns a summary dict containing:
      {
        "files_scanned": int,
        "files_modified": int,
        "total_replacements": int
      }
    """
    if not os.path.isdir(directory):
        raise NotADirectoryError(f"Not a directory: {directory}")

    # Encodings to try in order when reading text files
    candidate_encodings: Tuple[str, ...] = ("utf-8", "utf-8-sig", "latin-1", "cp1252")

    files_scanned = 0
    files_modified = 0
    total_replacements = 0

    for root, _, files in os.walk(directory):
        for name in files:
            file_path = os.path.join(root, name)
            files_scanned += 1

            # Skip symlinks
            if os.path.islink(file_path):
                continue

            # Skip likely binary files
            if _is_probably_binary(file_path):
                continue

            # Read with best-effort encoding detection
            read_result = _try_read_text(file_path, candidate_encodings)
            if read_result is None:
                # Unable to read as text; skip
                continue

            content, used_encoding = read_result

            if not content or to_be_replaced not in content:
                continue

            # Count occurrences before replacement
            occurrences = content.count(to_be_replaced)
            if occurrences == 0:
                continue

            new_content = content.replace(to_be_replaced, surrogate)
            newline_style = _detect_newline(content)

            # Atomic write: write to a temp file, then replace
            dir_for_temp = os.path.dirname(file_path) or "."
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding=used_encoding,
                    newline=newline_style,
                    delete=False,
                    dir=dir_for_temp,
                    suffix=".tmp",
                ) as tmp:
                    tmp.write(new_content)
                    temp_path = tmp.name

                os.replace(temp_path, file_path)
            except Exception:
                # On any failure, ensure temp file is cleaned up
                try:
                    if temp_path and os.path.exists(temp_path):
                        os.remove(temp_path)
                except Exception:
                    pass
                # Re-raise to make failures visible to the caller if desired
                raise

            files_modified += 1
            total_replacements += occurrences

    summary = {
        "files_scanned": files_scanned,
        "files_modified": files_modified,
        "total_replacements": total_replacements,
    }
    return summary


if __name__ == "__main__":
    # Example usage (edit the values below for your scenario)
    target_directory = "../test/config/"
    old_string = "hdd_1"
    new_string = "hdd_2"
    
    result = replace_all(target_directory, old_string, new_string)
    print(
        f"Scanned: {result['files_scanned']} | "
        f"Modified: {result['files_modified']} | "
        f"Replacements: {result['total_replacements']}"
    )