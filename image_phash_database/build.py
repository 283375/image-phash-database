import sqlite3
import time
from pathlib import Path

import imagehash
from PIL import Image


def build_image_phash_database(
    images: list[Path],
    labels: list[str],
    database_file_path: str,
    *,
    hash_size: int = 16,
    highfreq_factor: int = 4,
):
    assert len(images) == len(labels)

    conn = sqlite3.connect(database_file_path)

    with conn:
        cursor = conn.cursor()

        cursor.execute("CREATE TABLE properties (key TEXT, value TEXT)")
        cursor.executemany(
            "INSERT INTO properties VALUES (?, ?)",
            [
                ("hash_size", hash_size),
                ("highfreq_factor", highfreq_factor),
            ],
        )

        id_hashes = []
        for label, image_path in zip(labels, images):
            image_hash = imagehash.phash(
                Image.open(image_path.resolve()),
                hash_size=hash_size,
                highfreq_factor=highfreq_factor,
            )
            image_hash_bytes = image_hash.hash.flatten().tobytes()

            id_hashes.append([label, image_hash_bytes])

        hash_length = len(id_hashes[0][1])
        cursor.execute(f"CREATE TABLE hashes (id TEXT, hash BLOB({hash_length}))")

        cursor.executemany(
            "INSERT INTO hashes VALUES (?, ?)",
            id_hashes,
        )
        cursor.executemany(
            "INSERT INTO properties VALUES (?, ?)",
            [
                ("built_timestamp", int(time.time())),
            ],
        )
        conn.commit()
