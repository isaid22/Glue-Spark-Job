## 🚨 Error Summary

Job aborted due to stage failure:
ExecutorLostFailure (executor exited caused by one of the running tasks)
Reason: Remote RPC client dissociated.
Likely due to containers exceeding threshold, or network issue
This indicates the executor crashed, likely due to one or more of these:

💥 Too many concurrent writes to DynamoDB (you're using 1000 partitions)

🧠 Out of memory on the executor due to writing large batches

🌐 Network timeouts or throttling from DynamoDB

## ✅ How to Fix It (Step-by-Step)
### ✅ 1. Reduce Number of Partitions

You're using 1000 partitions, which is way too high for writing to DynamoDB.

Try reducing to 50–100 partitions max:

```
df = df.repartition(50)  # or even 20 depending on data volume
```

### ✅ 2. Avoid WriteDynamicFrame for Large Writes
WriteDynamicFrame.from_options(..., connection_type="dynamodb") is known to:

Not handle retries well

Cause executor crashes under high volume

Use underlying Hadoop connector with poor batching

Instead, use foreachPartition with boto3, which is more resilient and efficient.

```

import boto3

def write_to_dynamodb(partition):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table("your-dynamodb-table")

    with table.batch_writer() as batch:
        for row in partition:
            item = row.asDict()
            batch.put_item(Item=item)

df = dyf.toDF()  # convert from DynamicFrame
df = df.repartition(50)
df.foreachPartition(write_to_dynamodb)
```

### ✅ 3. Use On-Demand or Auto-Scaling in DynamoDB
If you're using provisioned capacity, it's easy to exceed write throughput with parallel writes.

Use on-demand mode for large but bursty loads

Or enable auto-scaling and set high maxWriteCapacity

Check CloudWatch for WriteThrottleEvents or high ConsumedWriteCapacity.


### ✅ 4. Upgrade Glue Worker Config
If you're still using the default worker size:

Switch to G.2X (8 vCPU, 32 GB RAM per worker)

Use enough workers (at least 10–20 for 200 GB)

Set this in Glue console or job arguments.

### ✅ 5. Watch for Memory/Network Errors
Enable logging to investigate further:

--enable-continuous-cloudwatch-log
--enable-metrics

Check CloudWatch logs for:

OutOfMemoryError

Container killed due to memory limit

DynamoDB write timeout or throttling

### 🛠️ Optional Improvements
Break data into chunks and call smaller Glue jobs or Lambda functions

Use Step Functions to orchestrate retries

Add error handling & logging inside write_to_dynamodb

### 🧪 Example Glue Script (Safe Write to DynamoDB)
Here’s a simple working pattern:

```
import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext

glueContext = GlueContext(SparkContext.getOrCreate())
dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://mybucket/mydata"]},
    format="parquet"
)

def write_partition(partition):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table("my-dynamodb-table")

    with table.batch_writer() as batch:
        for row in partition:
            batch.put_item(Item=row.asDict())

df = dyf.toDF()
df = df.repartition(50)
df.foreachPartition(write_partition)
```