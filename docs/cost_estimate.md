# Cost Estimate — SP Transit Performance Monitor

## Assumptions

- **Data volume:** ~15,000 vehicles × 30-second polling = ~1,800 records/min = ~2.6M records/day
- **Record size:** ~300 bytes average JSON → ~12 GB/day raw → ~360 GB/month raw
- **Operating hours:** 24/7 (buses run ~5am–midnight, but we ingest continuously)
- **Region:** us-east-1

## AWS Services

| Service | Configuration | Monthly Cost |
|---|---|---|
| **Lambda** | 2,880 invocations/day × 256MB × ~10s avg | ~$0.50 (mostly free tier) |
| **EventBridge Scheduler** | 1 rule, 30-second rate | Free |
| **Kinesis Data Streams** | On-demand, ~12 GB/day ingress | ~$18–22 |
| **Kinesis Firehose** | ~12 GB/day to S3, gzip compressed | ~$11 ($0.029/GB) |
| **S3 (raw)** | ~50 GB/month (gzip), lifecycle to IA at 30d | ~$1–2 |
| **S3 (checkpoints)** | ~5 GB | ~$0.12 |
| **S3 (Delta tables)** | ~50 GB/month | ~$1–2 |
| **CloudWatch** | Logs (14-day retention) + 4 alarms | ~$5 |
| **SNS** | Email notifications | Free |
| **SSM Parameter Store** | 1 SecureString parameter | Free |
| **AWS Subtotal** | | **~$37–43** |

## Databricks

| Resource | Configuration | Monthly Cost |
|---|---|---|
| **Streaming cluster** | i3.xlarge, 1-3 workers (spot), ~16h/day active | ~$80–120 |
| **Serverless SQL warehouse** | Dashboard queries, on-demand | ~$20–40 |
| **Jobs orchestration** | Included | $0 |
| **Unity Catalog** | Included | $0 |
| **Databricks Subtotal** | | **~$100–160** |

## Total Monthly Estimate

| Tier | Cost Range |
|---|---|
| **Minimum** (low utilization, spot instances) | **~$137** |
| **Typical** | **~$175** |
| **Maximum** (full utilization, on-demand fallback) | **~$203** |

## Cost Optimization Strategies

1. **Lambda:** Already minimal. Free tier covers ~1M invocations/month.
2. **Kinesis:** On-demand mode auto-scales. No over-provisioning risk.
3. **S3 lifecycle:** Raw data moves to IA at 30 days, Glacier at 90 days, expires at 365 days.
4. **Delta OPTIMIZE:** Run daily `OPTIMIZE` and `VACUUM` to minimize storage.
5. **Spot instances:** Streaming cluster uses `SPOT_WITH_FALLBACK` (up to 80% savings).
6. **Auto-termination:** Cluster terminates after 30 minutes of inactivity.
7. **No VPC/NAT Gateway:** Lambda runs outside VPC, saving ~$32/month.

## Comparison: What It Would Cost Without Optimization

| Without | Added Cost |
|---|---|
| On-demand Kinesis shards (provisioned) | +$15/shard × 4 shards = +$60 |
| NAT Gateway for Lambda in VPC | +$32 + data processing |
| On-demand cluster (no spot) | +$60–80 |
| No S3 lifecycle | +$5–10/month growing |
| Always-on SQL warehouse | +$100–200 |

**Without optimization: ~$350–500/month**
