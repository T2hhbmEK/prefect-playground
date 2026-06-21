# Results & object storage: caching big artifacts to Tencent COS (lesson 0004)

Fourth session. **Learner-driven detour** — they asked directly "how to cache
encoded mp4 files in COS with Prefect" (Tencent COS). This pulled *results &
storage* forward, ahead of the planned `flow.serve()` deployment lesson. The
mission ([[MISSION.md]]) is unchanged; only the course-arc order in [[NOTES.md]]
moved. Grounded in their real use case (video transcoding), not a toy.

Builds directly on [[0003-caching]]: same `cache_policy=INPUTS` + cross-run
persistence, now with the *result store* pointed at object storage.

**What was covered (lesson 0004 + `reference/results-storage-cheatsheet.html`):**

- **The core decision:** cache the *pointer*, store the *payload*. For a big
  artifact (mp4), the task uploads the file to COS itself and returns a small KEY
  string; Prefect caches the key, never the bytes. `result_storage` holds the
  return value — NOT the large file. This is the one idea the whole lesson hangs
  on; it's the `.why` callout and quiz Q1.
- **Key on content, not filename:** pass a content hash + preset as the task args
  so `INPUTS` busts the cache when bytes or encoding settings change (quiz Q2).
- **Tencent COS block** via `prefect_aws.S3Bucket` (COS is S3-compatible). Verified
  the API against the *installed* package (dev build), not memory:
  - `AwsClientParameters.endpoint_url` (confirmed field) →
    `https://cos.<region>.myqcloud.com`.
  - `AwsCredentials`: `aws_access_key_id`/`aws_secret_access_key` = Tencent
    SecretId/SecretKey; `region_name`; `aws_client_parameters`.
  - `S3Bucket`: `bucket_name` MUST include the APPID (`videos-1250000000`);
    methods `upload_from_path`, `download_object_to_path`, `read_path`/`write_path`
    (+ `a`-prefixed async).
- **The stale-pointer gotcha (wisdom):** a cache *hit* comes from Prefect's result
  record, not from whether the COS object exists. Deleted artifact + surviving
  cache record = dangling key. Defend with an existence check (`read_path`/HEAD →
  re-encode) or align `cache_expiration` with the COS lifecycle rule. Quiz Q3.

**Local object store — MinIO (learner's call, mid-session):** instead of real
Tencent COS, we stood up a local **MinIO** instance (S3-compatible) so everything
stays on the laptop — a better mission fit than a cloud account. Added
`docker-compose.yml` (MinIO on :9000, console :9001, auto-creates bucket
`prefect-artifacts`, creds `minioadmin`/`minioadmin`). `prefect-aws` ships a
purpose-built `MinIOCredentials` (verified in the installed package). The whole
point taught: **MinIO ⇄ COS is a credentials swap** — task code identical, only the
`S3Bucket` block changes.

**Verified end-to-end against real MinIO** (not a dir stand-in): `S3Bucket`
`write_path`/`read_path` round-trip + `upload_from_path` + `.save()` all work.
`04_results_storage.py` now uses the real block: Run 1 encodes (3s, Completed) and
the mp4 lands in MinIO (`mc ls` confirms `encoded/<hash>/h264-1080p.mp4`); Run 2 =
`Cached`, no encode, no re-upload, same key. Lesson 4 shows the exact swap to
Tencent COS (`AwsCredentials` + COS endpoint).

**Implication for next session:** ZPD returns to the back half of the mission —
**lesson 5: script → deployment** (`flow.serve()`), then **schedules**. The storage
block built here is reusable when deployments need remote result/flow storage. The
glossary now defines Result storage + Storage block.

**Open threads (ask-me, not drilled):** the existence-check guard implementation;
storing the result as a real `.mp4` content-type vs. the default serializer;
confirming the COS endpoint for the learner's *actual* region (flagged to the
Prefect Slack); pointing Prefect's own result store at COS for multi-machine
shared cache records.
