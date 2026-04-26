# core/id_allocator.py

import os


class IDAllocator:
    """
    Simple persistent ID generator
    """

    def __init__(self, name: str, base_path="data/id_state"):
        self.name = name
        self.base_path = base_path

        os.makedirs(base_path, exist_ok=True)

        self.file_path = os.path.join(base_path, f"{name}.txt")

        self.current = self._load()

    def _load(self):
        if os.path.exists(self.file_path):
            with open(self.file_path, "r") as f:
                return int(f.read().strip())
        return 0

    def _save(self):
        with open(self.file_path, "w") as f:
            f.write(str(self.current))

    def next_batch(self, n: int):
        start = self.current + 1
        end = self.current + n

        self.current = end
        self._save()

        return range(start, end + 1)