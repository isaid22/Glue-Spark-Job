# Glue-Spark-Job
AWS Glue and Spark job

🔍 Problem: Driver Memory Full
When you're reading a large dataset (200 GB in this case) from S3 and writing to DynamoDB, the Glue driver can get overwhelmed if:

Too much data is collected to the driver (e.g., using collect(), toPandas(), or anything that forces driver-side execution),

Or, inefficient partitioning causes the driver to orchestrate too much without distributing work properly across executors.

✅ Fixes & Best Practices
1. Avoid collecting data to the driver
Make sure you’re not doing anything like this:

```
df = spark.read.parquet("s3://...")
df.collect()  # BAD: brings all data to the driver
```
Instead, rely on Spark transformations and foreachPartition or mapPartitions for actions.

2. Use foreachPartition to write to DynamoDB
DynamoDB write logic should not happen in the driver. Instead, use a function like:

```
import boto3
import json

def write_to_dynamodb(partition):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table("your-dynamodb-table")

    with table.batch_writer() as batch:
        for row in partition:
            item = row.asDict()
            # Optionally, transform item here
            batch.put_item(Item=item)

df = spark.read.parquet("s3://your-datalake/path")
df.foreachPartition(write_to_dynamodb)
```
⚠️ You must ensure that your Glue job has proper IAM permissions to write to DynamoDB.

3. Optimize Partitions
You don’t want huge partitions (which can OOM a worker), but also not too many tiny ones.

```
df = df.repartition(100)  # Adjust based on worker count / data size
```
You can also repartition based on a column, e.g., 
```
repartition("customer_id").
```

4. Use Sufficient Worker Memory
Glue DynamicFrame jobs have options for setting worker types and numbers:

G.1X: 4 vCPUs, 16 GB memory

G.2X: 8 vCPUs, 32 GB memory

In the Glue console or job parameters, increase:
```
--job-language=python
--enable-continuous-cloudwatch-log
--enable-metrics
```

Worker type: use G.2X

Number of workers: estimate based on data size (~200 GB → ~40-50 workers for large writes)

5. Tune Spark Config (Optional)
You can set Glue job parameters like:

```
--conf spark.executor.memory=4g
--conf spark.driver.memory=4g
--conf spark.executor.instances=20
```

🛠 Alternative (if Glue memory is too limiting)
Consider breaking it up:

Write data to S3 in chunks (e.g., partitioned files)

Use a Lambda or separate Glue job to stream each chunk into DynamoDB

This allows better retry logic and separation of concerns.

