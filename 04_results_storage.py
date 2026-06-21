import hashlib
import time
from pathlib import Path

from prefect.cache_policies import INPUTS
from prefect_aws import AwsClientParameters, MinIOCredentials, S3Bucket

from prefect import flow, task

# --- Object storage: a local MinIO instance (S3-compatible) ------------------
# Start it with `docker compose up -d` (see docker-compose.yml). MinIO stands in
# for Tencent COS / AWS S3 — the only difference in production is the credentials
# and `endpoint_url`. The bucket `prefect-artifacts` is created by compose.
BLOCK_NAME = "minio-artifacts"


def get_bucket() -> S3Bucket:
    """Load the saved storage block, or build + save it on first run."""
    try:
        return S3Bucket.load(BLOCK_NAME)
    except Exception:
        bucket = S3Bucket(
            bucket_name="prefect-artifacts",
            credentials=MinIOCredentials(
                minio_root_user="minioadmin",
                minio_root_password="minioadmin",
                aws_client_parameters=AwsClientParameters(
                    endpoint_url="http://localhost:9000",
                ),
            ),
        )
        bucket.save(BLOCK_NAME, overwrite=True)
        return bucket


cos = get_bucket()


def content_hash(path: Path) -> str:
    """Hash the *bytes* of the source, so a changed file busts the cache.

    Keying on a filename would re-use a stale encode when the file changes;
    keying on content is what you actually want.
    """
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


@task(cache_policy=INPUTS, result_storage=cos)
def encode_video(src_hash: str, preset: str = "h264-1080p") -> str:
    """Encode -> upload the mp4 to MinIO -> return the object key (a small string).

    `cache_policy=INPUTS` keys on (src_hash, preset): same bytes + same preset
    => Cached, no re-encode and no re-upload. What gets cached is the returned
    KEY, never the multi-megabyte mp4. The video lives in object storage because
    we put it there with `upload_from_path`.
    """
    print(f"  encoding {src_hash} @ {preset} (slow)...")
    time.sleep(3)  # stand-in for an ffmpeg transcode
    out_key = f"encoded/{src_hash}/{preset}.mp4"
    tmp = Path(f"/tmp/{src_hash}-{preset}.mp4")
    tmp.write_bytes(b"\x00\x00\x00\x18ftypmp42")  # pretend encoded mp4 bytes
    cos.upload_from_path(str(tmp), out_key)  # the artifact upload
    return out_key


@flow
def transcode_pipeline(source: str = "/tmp/source.mov") -> str:
    src = Path(source)
    if not src.exists():  # seed a dummy source on first run
        src.write_bytes(b"RIFF....fake source video bytes....")

    h = content_hash(src)
    key = encode_video(h, preset="h264-1080p")
    print(f"  encoded mp4 at object key: {key}")
    return key


if __name__ == "__main__":
    print(transcode_pipeline())
