"""Disk-resident B+-tree for 1-D range and point queries.

Pages are 4 KB to match the OS page size (so mmap reads hit exactly one page).
Leaves hold 254 key/row-id pairs: (4096 - 20-byte header) / 16-byte entry. Internal
nodes hold up to 339 separator keys. Build is single-pass bulk-load from a sorted
array -- no per-key insertion path, since this is read-mostly at query time.
"""

import mmap
import struct
from dataclasses import dataclass, field

import numpy as np

PAGE_SIZE = 4096
# Header layout: page_type(1B) + n_keys(2B) + prev_page(4B) + next_page(4B) + padding.
HEADER_SIZE = 20
# Leaf entry layout: key (float64, 8B) + row_id (int64, 8B).
ENTRY_SIZE = 16
MAX_LEAF_KEYS = (PAGE_SIZE - HEADER_SIZE) // ENTRY_SIZE  # 254

LEAF_PAGE = 1
INTERNAL_PAGE = 2

# Internal node packs K keys and K+1 child pointers: 8K + 4(K+1) = 12K + 4 bytes.
MAX_INTERNAL_KEYS = (PAGE_SIZE - HEADER_SIZE - 4) // 12  # 339


@dataclass
class BPlusTree:
    """One-dimensional B+-tree. Backed by a single page-aligned file, read via mmap."""

    path: str
    order: int = MAX_LEAF_KEYS
    _fp: object = field(default=None, repr=False)
    _mm: mmap.mmap | None = field(default=None, repr=False)
    _root_page: int = field(default=0, repr=False)
    _num_pages: int = field(default=0, repr=False)

    def build_from_sorted(self, keys: np.ndarray, row_ids: np.ndarray) -> None:
        """Bulk-load the tree from a sorted (keys, row_ids) pair.

        This is the only supported build path -- inserting keys one at a time
        would be orders of magnitude slower and is unnecessary for read-mostly
        analytical workloads. Caller must sort the arrays by key first.
        """
        n = len(keys)
        if n == 0:
            # Empty dataset still needs a valid root leaf so range_query returns [] cleanly.
            with open(self.path, "wb") as f:
                f.write(self._make_leaf_page([], [], -1, -1))
            self._num_pages = 1
            self._root_page = 0
            self._open()
            return

        keys = keys.astype(np.float64)
        row_ids = row_ids.astype(np.int64)

        # Pack leaves first, then build internal levels bottom-up.
        leaf_pages: list[bytes] = []
        leaf_first_keys: list[float] = []
        for i in range(0, n, MAX_LEAF_KEYS):
            chunk_keys = keys[i : i + MAX_LEAF_KEYS]
            chunk_ids = row_ids[i : i + MAX_LEAF_KEYS]
            page_idx = len(leaf_pages)
            prev_page = page_idx - 1 if page_idx > 0 else -1
            # next_page pointer gets patched in once we know the final index below.
            leaf_pages.append(
                self._make_leaf_page(chunk_keys, chunk_ids, prev_page, -1)
            )
            leaf_first_keys.append(float(chunk_keys[0]))

        # Link leaves forward so range_query can scan sibling-to-sibling without
        # walking back up to internal nodes.
        for i in range(len(leaf_pages) - 1):
            page = bytearray(leaf_pages[i])
            struct.pack_into("<i", page, 7, i + 1)
            leaf_pages[i] = bytes(page)

        all_pages = list(leaf_pages)
        child_indices = list(range(len(leaf_pages)))
        separator_keys = leaf_first_keys[1:]

        while len(child_indices) > 1:
            new_children: list[int] = []
            new_separators: list[float] = []
            i = 0
            while i < len(child_indices):
                end = min(i + MAX_INTERNAL_KEYS + 1, len(child_indices))
                node_children = child_indices[i:end]
                node_keys = separator_keys[i : end - 1]

                page_idx = len(all_pages)
                all_pages.append(
                    self._make_internal_page(node_keys, node_children)
                )
                new_children.append(page_idx)
                if end < len(child_indices):
                    new_separators.append(separator_keys[end - 1])
                i = end

            child_indices = new_children
            separator_keys = new_separators

        self._root_page = child_indices[0]
        self._num_pages = len(all_pages)

        with open(self.path, "wb") as f:
            for page in all_pages:
                f.write(page)

        self._open()

    def _make_leaf_page(
        self,
        keys: np.ndarray | list,
        row_ids: np.ndarray | list,
        prev_page: int,
        next_page: int,
    ) -> bytes:
        buf = bytearray(PAGE_SIZE)
        n_keys = len(keys)
        struct.pack_into("<B", buf, 0, LEAF_PAGE)
        struct.pack_into("<H", buf, 1, n_keys)
        struct.pack_into("<i", buf, 3, prev_page)
        struct.pack_into("<i", buf, 7, next_page)
        for i in range(n_keys):
            offset = HEADER_SIZE + i * ENTRY_SIZE
            struct.pack_into("<d", buf, offset, float(keys[i]))
            struct.pack_into("<q", buf, offset + 8, int(row_ids[i]))
        return bytes(buf)

    def _make_internal_page(
        self, keys: list[float], children: list[int]
    ) -> bytes:
        buf = bytearray(PAGE_SIZE)
        n_keys = len(keys)
        struct.pack_into("<B", buf, 0, INTERNAL_PAGE)
        struct.pack_into("<H", buf, 1, n_keys)
        # Child pointers come before keys in the page -- n_keys + 1 children,
        # then n_keys separator values. Read/write order must stay consistent.
        offset = HEADER_SIZE
        for i in range(len(children)):
            struct.pack_into("<I", buf, offset, children[i])
            offset += 4
        for i in range(n_keys):
            struct.pack_into("<d", buf, offset, keys[i])
            offset += 8
        return bytes(buf)

    def _open(self) -> None:
        if self._mm is not None:
            return
        self._fp = open(self.path, "rb")
        self._mm = mmap.mmap(self._fp.fileno(), 0, access=mmap.ACCESS_READ)

    def _read_page(self, page_idx: int) -> bytes:
        self._open()
        offset = page_idx * PAGE_SIZE
        return self._mm[offset : offset + PAGE_SIZE]

    def _find_leaf(self, key: float) -> int:
        """Walk root->leaf for the given key. One page read per level (~3 reads at 50K rows)."""
        page_idx = self._root_page
        while True:
            page = self._read_page(page_idx)
            page_type = struct.unpack_from("<B", page, 0)[0]
            if page_type == LEAF_PAGE:
                return page_idx

            n_keys = struct.unpack_from("<H", page, 1)[0]
            n_children = n_keys + 1
            offset = HEADER_SIZE
            children = []
            for i in range(n_children):
                children.append(struct.unpack_from("<I", page, offset)[0])
                offset += 4
            keys = []
            for i in range(n_keys):
                keys.append(struct.unpack_from("<d", page, offset)[0])
                offset += 8

            # Linear scan is fine here -- at most 339 keys, and the level count is
            # tiny (3 for 50K rows), so a binary-search optimization would not
            # noticeably change overall query cost.
            child_idx = n_children - 1
            for i in range(n_keys):
                if key < keys[i]:
                    child_idx = i
                    break
            page_idx = children[child_idx]

    def range_query(self, low: float, high: float) -> list[int]:
        """Row ids where low <= key <= high. Uses leaf-sibling links to avoid
        re-descending the tree between consecutive leaves."""
        if self._num_pages == 0:
            return []
        self._open()
        leaf_idx = self._find_leaf(low)
        result: list[int] = []

        while leaf_idx != -1:
            page = self._read_page(leaf_idx)
            n_keys = struct.unpack_from("<H", page, 1)[0]
            next_page = struct.unpack_from("<i", page, 7)[0]

            done = False
            for i in range(n_keys):
                offset = HEADER_SIZE + i * ENTRY_SIZE
                k = struct.unpack_from("<d", page, offset)[0]
                if k > high:
                    done = True
                    break
                if k >= low:
                    row_id = struct.unpack_from("<q", page, offset + 8)[0]
                    result.append(row_id)

            if done:
                break
            leaf_idx = next_page

        return result

    def point_query(self, key: float) -> list[int]:
        """Row ids with exactly this key. Implemented as a degenerate range query."""
        return self.range_query(key, key)

    def close(self) -> None:
        """Release the mmap and file handle. Idempotent."""
        if self._mm is not None:
            self._mm.close()
            self._mm = None
        if self._fp is not None:
            self._fp.close()
            self._fp = None
