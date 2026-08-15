import tempfile
import unittest
from pathlib import Path

from src.m4b.path_resolver import M4BPathResolver


class TestM4BPathResolver(unittest.TestCase):
    def test_resolves_direct_existing_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            file_path = Path(tmp) / "part1.mp3"
            file_path.write_bytes(b"a")

            resolver = M4BPathResolver()
            resolved = resolver.resolve(str(file_path), "part1.mp3")
            self.assertEqual(resolved, file_path)

    def test_resolves_via_mapping_and_filename_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "audio"
            root.mkdir(parents=True, exist_ok=True)
            nested = root / "book" / "track01.mp3"
            nested.parent.mkdir(parents=True, exist_ok=True)
            nested.write_bytes(b"a")

            resolver = M4BPathResolver(
                audio_root=root,
                mappings="D:/Media/Audiobooks=>" + str(root).replace("\\", "/"),
            )

            mapped = resolver.resolve("D:/Media/Audiobooks/book/track01.mp3", "track01.mp3")
            self.assertTrue(mapped and mapped.exists())


if __name__ == "__main__":
    unittest.main()

