# Object Store Credentials

NoKV keeps two kinds of object-store credentials apart. The shard owner
(`nokv serve`, and `nokv provision` when a root is created) is the only
process that writes objects. Every Agent process, whether it runs the native
CLI or the Python SDK, only reads them. Since the owner-mediated upload path
landed, a client hands each block of a publication to its owner over
workspace RPC, and the owner verifies the block against the staged plan before
writing it with its own credentials. An Agent therefore needs no permission to
create, overwrite, or delete any object, and a deployment should not grant
one.

## What each role touches

All keys live under the configured object root (`--object-root`, default
`/`, or `ObjectStoreConfig.s3(root=...)`), inside the bucket.

| Role | Keys | Operations |
| --- | --- | --- |
| Agent (CLI commands other than `serve` and `provision`; Python SDK) | `nokv/system/object-namespace` | GET (namespace marker verification before any read or publish) |
| Agent | `nokv/artifacts/*` | GET, including ranged reads, and HEAD |
| Owner (`serve`) | `nokv/artifacts/*` | PUT (create-only), GET, HEAD, DELETE (staged-object cleanup and reference GC) |
| Owner (`serve`, `provision`) | `nokv/system/object-namespace` | PUT once at provision, GET at every start |
| Owner (`serve`, `provision`) | `nokv/system/provider-admission/v1/*` | PUT, GET, DELETE (the write-conformance probe run before serving) |
| Owner (`serve`, shared recovery publication) | `nokv/recovery/checkpoints/v1/*`, `nokv/recovery/log-segments/v1/*` | PUT, GET, HEAD, DELETE |

Agents never list the bucket, never write under any prefix, and never read
the recovery or admission prefixes.

## Example policies

The examples use AWS IAM syntax; RustFS, MinIO, and Ceph RGW accept the same
statements. Replace `BUCKET` with the bucket and `ROOT` with the object root
without its leading slash (omit `ROOT/` when the root is `/`).

Agent processes, read-only under the root's prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": [
        "arn:aws:s3:::BUCKET/ROOT/nokv/system/object-namespace",
        "arn:aws:s3:::BUCKET/ROOT/nokv/artifacts/*"
      ]
    }
  ]
}
```

`HeadObject` is authorized by `s3:GetObject`, so the statement covers both.
No `s3:ListBucket` is needed.

The owner, read and write under the root's prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::BUCKET/ROOT/nokv/*"]
    }
  ]
}
```

Give neither role `s3:PutObject` outside `ROOT/nokv/`, and give no Agent
`s3:PutObject` or `s3:DeleteObject` at all. Several roots can share a bucket
under different object roots; each owner's policy then names its own root.

## Short-lived credentials

Prefer the provider's credential chain over flags: the CLI and the SDK read
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` from
the environment, so a harness can mint a session per job and never place a
secret in a command line, a Workbench, or a manifest.

With AWS STS, assume a role whose policy is the Agent policy above and export
the session:

```bash
eval "$(aws sts assume-role \
  --role-arn arn:aws:iam::ACCOUNT:role/nokv-agent-reader \
  --role-session-name "$JOB_ID" \
  --duration-seconds 3600 \
  --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
  --output text \
  | awk '{print "export AWS_ACCESS_KEY_ID=" $1 "; export AWS_SECRET_ACCESS_KEY=" $2 "; export AWS_SESSION_TOKEN=" $3}')"
```

Then run the Agent as usual:

```bash
nokv --root-id "$ROOT_ID" --control-dir "$CONTROL_DIR" \
  --object-bucket BUCKET --object-root /ROOT --object-region us-east-1 \
  workspace-path put run-42 outputs result.bin --operation-id "$OPERATION_ID" --file ./result.bin
```

The put succeeds although the session can only read: the blocks travel to the
owner, which writes them. The Python SDK takes the same session through
`ObjectStoreConfig.s3(bucket, root=..., access_key_id=..., secret_access_key=...,
session_token=...)` or, preferably, from the environment.

A session that expires mid-publication fails the Agent's next object read
with the provider's error; publications themselves are not affected, since
the owner holds its own long-lived credential.

## What this does not cover

The control plane (etcd or the local control directory) still needs a trusted
network and its own access control, and the object store authenticates only
what the deployment's IAM grants each credential. The workspace RPC TLS gate
exercises this boundary: every Agent process in it reaches the object store
through an endpoint that forwards only GET and HEAD, publishes a two-block
body, and reads it back ([`scripts/workbench/rpc_tls_gate.py`](../scripts/workbench/rpc_tls_gate.py),
scenario `t14`).
