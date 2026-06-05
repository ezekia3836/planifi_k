import os
import redis
import json
import gzip

from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv("app.env")


r = redis.Redis(
    host="localhost",
    port=6379,
    db=0,
    decode_responses=False
)

TTL = 60 * 60 * 24

print(
    f"[STATUS] Cache: "
    f"read={'enabled' if os.getenv('CACHE_READ_ENABLED', 'true').strip().lower() == 'true' else 'disabled'} "
    f"and "
    f"write={'enabled' if os.getenv('CACHE_WRITE_ENABLED', 'true').strip().lower() == 'true' else 'disabled'}"
)


class CacheManager:

    COMPRESS_THRESHOLD = 1_000_000

    MAX_CACHE_SIZE = 30_000_000

    @staticmethod
    def enabled_read():
        """
        Active/désactive uniquement la lecture du cache
        """
        return (
            os.getenv("CACHE_READ_ENABLED", "true")
            .strip()
            .lower() == "true"
        )

    @staticmethod
    def enabled_write():
        """
        Active/désactive uniquement l'écriture du cache
        """
        return (
            os.getenv("CACHE_WRITE_ENABLED", "true")
            .strip()
            .lower() == "true"
        )

    # -----------------------------------
    # HELPERS
    # -----------------------------------

    @staticmethod
    def today():
        return date.today().strftime("%Y-%m-%d")

    @staticmethod
    def key(name, *args):
        return (
            f"{name}:"
            + ":".join(map(str, args))
            + f":{CacheManager.today()}"
        )

    @staticmethod
    def _json_serializer(obj):

        if isinstance(obj, set):
            return list(obj)

        if isinstance(obj, (date, datetime)):
            return obj.isoformat()

        raise TypeError(f"Type not serializable: {type(obj)}")

    # -----------------------------------
    # GET
    # -----------------------------------

    @staticmethod
    def get(key):

        if not CacheManager.enabled_read():
            return None

        try:

            data = r.get(key)

            if not data:
                return None

            payload = json.loads(data)

            raw = bytes.fromhex(payload["data"])

            if payload.get("compressed"):
                raw = gzip.decompress(raw)

            return json.loads(raw.decode())

        except Exception as e:
            print(f"[CACHE GET ERROR] {key}: {e}")
            return None

    # -----------------------------------
    # SET
    # -----------------------------------

    @staticmethod
    def set(key, value, ttl=TTL):

        if not CacheManager.enabled_write():
            return

        try:

            raw = json.dumps(
                value,
                default=CacheManager._json_serializer
            ).encode()

            raw_size = len(raw)

            # -----------------------------------
            # SKIP HUGE PAYLOAD
            # -----------------------------------

            if raw_size > CacheManager.MAX_CACHE_SIZE:

                print(
                    f"[CACHE SKIP] huge payload | "
                    f"{key} | "
                    f"{raw_size / 1024 / 1024:.2f} MB"
                )

                return

            compressed = False

            # -----------------------------------
            # AUTO GZIP
            # -----------------------------------

            if raw_size > CacheManager.COMPRESS_THRESHOLD:

                raw = gzip.compress(raw)
                compressed = True

            payload = {
                "compressed": compressed,
                "data": raw.hex()
            }

            final = json.dumps(payload)

            r.setex(
                key,
                ttl,
                final
            )

            print(
                f"[CACHE SET] "
                f"{key} | "
                f"size={raw_size / 1024 / 1024:.2f}MB | "
                f"compressed={compressed}"
            )

        except Exception as e:
            print(f"[CACHE SET ERROR] {key}: {e}")

    # -----------------------------------
    # DELETE
    # -----------------------------------

    @staticmethod
    def delete(key):
        r.delete(key)

