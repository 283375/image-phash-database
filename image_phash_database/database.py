import sqlite3

import imagehash
import numpy as np
from PIL import Image


def hamming_distance_sql_function(user_input, db_entry) -> int:
    return np.count_nonzero(
        np.frombuffer(user_input, bool) ^ np.frombuffer(db_entry, bool)
    )


class ImagePHashDatabase:
    def __init__(self, db_path: str):
        self.__conn = sqlite3.connect(db_path)
        with self.conn as conn:
            self.hash_size = int(
                conn.execute(
                    "SELECT value FROM properties WHERE key = 'hash_size'"
                ).fetchone()[0]
            )
            self.highfreq_factor = int(
                conn.execute(
                    "SELECT value FROM properties WHERE key = 'highfreq_factor'"
                ).fetchone()[0]
            )
            self.built_timestamp = int(
                conn.execute(
                    "SELECT value FROM properties WHERE key = 'built_timestamp'"
                ).fetchone()[0]
            )

            self.conn.create_function(
                "HAMMING_DISTANCE",
                2,
                hamming_distance_sql_function,
                deterministic=True,
            )

    @property
    def conn(self):
        return self.__conn

    def lookup_hash(self, image_hash: imagehash.ImageHash, *, limit: int = 5):
        with self.conn as conn:
            image_hash_bytes = image_hash.hash.flatten().tobytes()
            return conn.execute(
                """SELECT id, HAMMING_DISTANCE(?, hash) AS distance
                FROM hashes
                ORDER BY distance ASC LIMIT ?""",
                [image_hash_bytes, limit],
            ).fetchall()

    def lookup_image(self, pil_image: Image.Image):
        image_hash = imagehash.phash(
            pil_image, hash_size=self.hash_size, highfreq_factor=self.highfreq_factor
        )
        return self.lookup_hash(image_hash)[0]
