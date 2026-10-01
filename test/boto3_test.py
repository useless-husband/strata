"""boto3 against strata.

Starts the strata binary (STRATA_BIN, or ./strata) on an ephemeral port
with 4 data and 2 parity disks and exercises it with boto3, the AWS SDK
for Python, including its s3transfer multipart uploads and downloads.

    python3 -m venv .venv && .venv/bin/pip install boto3
    STRATA_BIN=./strata .venv/bin/python test/boto3_test.py
"""

import hashlib
import io
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

ACCESS, SECRET = "boto3-access", "boto3-secret-key"
BIN = os.environ.get("STRATA_BIN", os.path.join(os.path.dirname(__file__), "..", "strata"))


class StrataBoto3Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="strata-boto3-")
        addr_file = os.path.join(cls.dir, "addr")
        disks = [os.path.join(cls.dir, f"disk{i}") for i in range(6)]
        cls.log = open(os.path.join(cls.dir, "server.log"), "w")
        cls.proc = subprocess.Popen(
            [BIN, "server", "--address", "127.0.0.1:0", "--address-file", addr_file,
             "--access-key", ACCESS, "--secret-key", SECRET, "--sync", "none",
             "--data", "4", "--parity", "2", *disks],
            stdout=cls.log, stderr=cls.log)
        for _ in range(200):
            if os.path.exists(addr_file) and os.path.getsize(addr_file) > 0:
                break
            time.sleep(0.025)
        with open(addr_file) as f:
            cls.endpoint = "http://" + f.read().strip()
        for var in ("AWS_PROFILE", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE"):
            os.environ.pop(var, None)
        os.environ["AWS_CONFIG_FILE"] = os.devnull
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = os.devnull
        cls.s3 = boto3.client(
            "s3", endpoint_url=cls.endpoint, region_name="us-east-1",
            aws_access_key_id=ACCESS, aws_secret_access_key=SECRET,
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}))
        cls.s3.create_bucket(Bucket="boto")

    @classmethod
    def tearDownClass(cls):
        cls.proc.send_signal(signal.SIGTERM)
        cls.proc.wait(timeout=30)
        cls.log.close()
        shutil.rmtree(cls.dir, ignore_errors=True)

    def code(self, ctx):
        return ctx.exception.response["Error"]["Code"]

    def test_put_get_head_metadata(self):
        body = os.urandom(200_000)
        self.s3.put_object(Bucket="boto", Key="a/b.bin", Body=body, ContentType="application/x-boto",
                           Metadata={"mixed-Case": "Value"}, ChecksumAlgorithm="SHA256")
        got = self.s3.get_object(Bucket="boto", Key="a/b.bin", ChecksumMode="ENABLED")
        self.assertEqual(got["Body"].read(), body)
        self.assertEqual(got["ContentType"], "application/x-boto")
        self.assertEqual(got["Metadata"], {"mixed-case": "Value"})
        self.assertEqual(got["ETag"], '"%s"' % hashlib.md5(body).hexdigest())
        head = self.s3.head_object(Bucket="boto", Key="a/b.bin")
        self.assertEqual(head["ContentLength"], len(body))
        part = self.s3.get_object(Bucket="boto", Key="a/b.bin", Range="bytes=100-199")
        self.assertEqual(part["Body"].read(), body[100:200])
        self.assertEqual(part["ContentRange"], "bytes 100-199/200000")

    def test_errors(self):
        with self.assertRaises(ClientError) as ctx:
            self.s3.get_object(Bucket="boto", Key="missing")
        self.assertEqual(self.code(ctx), "NoSuchKey")
        with self.assertRaises(ClientError) as ctx:
            self.s3.list_objects_v2(Bucket="no-such-bucket")
        self.assertEqual(self.code(ctx), "NoSuchBucket")
        with self.assertRaises(ClientError) as ctx:
            self.s3.create_bucket(Bucket="boto")
        self.assertEqual(self.code(ctx), "BucketAlreadyOwnedByYou")
        self.s3.put_object(Bucket="boto", Key="cond", Body=b"x")
        with self.assertRaises(ClientError) as ctx:
            self.s3.get_object(Bucket="boto", Key="cond", IfMatch='"nope"')
        self.assertEqual(self.code(ctx), "PreconditionFailed")
        etag = self.s3.head_object(Bucket="boto", Key="cond")["ETag"]
        with self.assertRaises(ClientError) as ctx:
            self.s3.get_object(Bucket="boto", Key="cond", IfNoneMatch=etag)
        self.assertEqual(ctx.exception.response["ResponseMetadata"]["HTTPStatusCode"], 304)
        with self.assertRaises(ClientError) as ctx:
            self.s3.put_object(Bucket="boto", Key="cond", Body=b"y", IfNoneMatch="*")
        self.assertEqual(self.code(ctx), "PreconditionFailed")

    def test_transfer_manager_multipart(self):
        data = os.urandom(21 * 1024 * 1024)
        cfg = TransferConfig(multipart_threshold=8 * 1024 * 1024, multipart_chunksize=5 * 1024 * 1024,
                             max_concurrency=4)
        self.s3.upload_fileobj(io.BytesIO(data), "boto", "big/file", Config=cfg)
        head = self.s3.head_object(Bucket="boto", Key="big/file")
        self.assertTrue(head["ETag"].endswith('-5"'), head["ETag"])
        out = io.BytesIO()
        self.s3.download_fileobj("boto", "big/file", out, Config=cfg)
        self.assertEqual(out.getvalue(), data)
        self.assertEqual(self.s3.list_multipart_uploads(Bucket="boto").get("Uploads", []), [])

    def test_manual_multipart_and_abort(self):
        up = self.s3.create_multipart_upload(Bucket="boto", Key="manual")
        uid = up["UploadId"]
        p1 = self.s3.upload_part(Bucket="boto", Key="manual", UploadId=uid, PartNumber=1, Body=b"a" * (5 << 20))
        p2 = self.s3.upload_part(Bucket="boto", Key="manual", UploadId=uid, PartNumber=2, Body=b"end")
        parts = self.s3.list_parts(Bucket="boto", Key="manual", UploadId=uid)["Parts"]
        self.assertEqual([p["PartNumber"] for p in parts], [1, 2])
        with self.assertRaises(ClientError) as ctx:
            self.s3.complete_multipart_upload(
                Bucket="boto", Key="manual", UploadId=uid,
                MultipartUpload={"Parts": [{"PartNumber": 2, "ETag": p2["ETag"]},
                                           {"PartNumber": 1, "ETag": p1["ETag"]}]})
        self.assertEqual(self.code(ctx), "InvalidPartOrder")
        self.s3.complete_multipart_upload(
            Bucket="boto", Key="manual", UploadId=uid,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": p1["ETag"]},
                                       {"PartNumber": 2, "ETag": p2["ETag"]}]})
        body = self.s3.get_object(Bucket="boto", Key="manual")["Body"].read()
        self.assertEqual(len(body), (5 << 20) + 3)
        up = self.s3.create_multipart_upload(Bucket="boto", Key="aborted")
        self.s3.abort_multipart_upload(Bucket="boto", Key="aborted", UploadId=up["UploadId"])
        with self.assertRaises(ClientError) as ctx:
            self.s3.list_parts(Bucket="boto", Key="aborted", UploadId=up["UploadId"])
        self.assertEqual(self.code(ctx), "NoSuchUpload")

    def test_listing_paginator(self):
        keys = [f"list/{d}/{i:03d}" for d in ("x", "y") for i in range(7)] + ["list/top"]
        for k in keys:
            self.s3.put_object(Bucket="boto", Key=k, Body=k.encode())
        seen = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(
                Bucket="boto", Prefix="list/", PaginationConfig={"PageSize": 4}):
            seen += [o["Key"] for o in page.get("Contents", [])]
        self.assertEqual(seen, sorted(keys))
        page = self.s3.list_objects_v2(Bucket="boto", Prefix="list/", Delimiter="/")
        self.assertEqual([p["Prefix"] for p in page["CommonPrefixes"]], ["list/x/", "list/y/"])
        self.assertEqual([o["Key"] for o in page["Contents"]], ["list/top"])
        old = []
        for page in self.s3.get_paginator("list_objects").paginate(
                Bucket="boto", Prefix="list/", PaginationConfig={"PageSize": 5}):
            old += [o["Key"] for o in page.get("Contents", [])]
        self.assertEqual(old, sorted(keys))

    def test_copy_delete_presign(self):
        self.s3.put_object(Bucket="boto", Key="src", Body=b"copy source", Metadata={"k": "v"})
        self.s3.copy_object(Bucket="boto", Key="dst", CopySource={"Bucket": "boto", "Key": "src"})
        self.assertEqual(self.s3.head_object(Bucket="boto", Key="dst")["Metadata"], {"k": "v"})
        # boto3's default for presigned URLs here is Signature Version 2;
        # check that and SigV4.
        url = self.s3.generate_presigned_url("get_object", Params={"Bucket": "boto", "Key": "dst"}, ExpiresIn=60)
        self.assertIn("AWSAccessKeyId=", url)
        with urllib.request.urlopen(url) as r:
            self.assertEqual(r.read(), b"copy source")
        v4 = boto3.client(
            "s3", endpoint_url=self.endpoint, region_name="us-east-1",
            aws_access_key_id=ACCESS, aws_secret_access_key=SECRET,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))
        url = v4.generate_presigned_url("get_object", Params={"Bucket": "boto", "Key": "dst"}, ExpiresIn=60)
        self.assertIn("X-Amz-Signature=", url)
        with urllib.request.urlopen(url) as r:
            self.assertEqual(r.read(), b"copy source")
        url = v4.generate_presigned_url("put_object", Params={"Bucket": "boto", "Key": "via-url"}, ExpiresIn=60)
        req = urllib.request.Request(url, data=b"uploaded through a presigned URL", method="PUT")
        with urllib.request.urlopen(req) as r:
            self.assertEqual(r.status, 200)
        self.assertEqual(self.s3.get_object(Bucket="boto", Key="via-url")["Body"].read(),
                         b"uploaded through a presigned URL")
        res = self.s3.delete_objects(Bucket="boto", Delete={"Objects": [{"Key": "src"}, {"Key": "dst"}]})
        self.assertEqual(sorted(d["Key"] for d in res["Deleted"]), ["dst", "src"])
        self.assertNotIn("Contents", self.s3.list_objects_v2(Bucket="boto", Prefix="src"))

    def test_unicode_and_odd_keys(self):
        for key in ["спутник/ключ", "space key", "plus+key", "a//b", "trailing/", "per%cent", "emoji-\U0001F600"]:
            self.s3.put_object(Bucket="boto", Key=key, Body=key.encode())
            self.assertEqual(self.s3.get_object(Bucket="boto", Key=key)["Body"].read(), key.encode())
            listed = self.s3.list_objects_v2(Bucket="boto", Prefix=key)["Contents"][0]["Key"]
            self.assertEqual(listed, key)


if __name__ == "__main__":
    result = unittest.main(exit=False, verbosity=2).result
    sys.exit(0 if result.wasSuccessful() else 1)
