cand = """## POSSIBLE ROOT CAUSE COMPONENTS:
# In metric files, the column for COMPONENTS is: object_id (apm/service/), object_id (apm/pod/), object_id (infra/infra_tidb/), object_id/object_type (other/), service name (logs/traces)
(if the root cause is at the node level, i.e., the root cause is a specific node)
- adservice
- cartservice
- checkoutservice
- currencyservice
- emailservice
- frontend
- paymentservice
- productcatalogservice
- recommendationservice
- redis-cart
- shippingservice

## POSSIBLE ROOT CAUSE PODS:
# In metric files, the column for PODS is: pod (apm/pod/, infra/infra_pod/, infra/infra_node/, other/), k8_pod (logs), pod (traces)
(if the root cause is at the pod level, i.e., the root cause is a specific container)
- adservice-0
- adservice-1
- adservice-2
- cartservice-0
- cartservice-1
- cartservice-2
- checkoutservice-0
- checkoutservice-1
- checkoutservice-2
- currencyservice-0
- currencyservice-1
- currencyservice-2
- emailservice-0
- emailservice-1
- emailservice-2
- frontend-0
- frontend-1
- frontend-2
- paymentservice-0
- paymentservice-1
- paymentservice-2
- productcatalogservice-0
- productcatalogservice-1
- productcatalogservice-2
- recommendationservice-0
- recommendationservice-1
- recommendationservice-2
- redis-cart-0
- shippingservice-0
- shippingservice-1
- shippingservice-2

## POSSIBLE ROOT CAUSE NODES:
# In metric files, the column for NODES is: kubernetes_node (infra/infra_node/, infra/infra_pod/, other/), k8_node_name (logs), kubernetes_node (traces)
(if the root cause is at the node level, i.e., the root cause is a specific node)
- aiops-k8s-01
- aiops-k8s-03
- aiops-k8s-04
- aiops-k8s-05
- aiops-k8s-06
- aiops-k8s-07
- aiops-k8s-08

## POSSIBLE ROOT CAUSE REASONS:
- disk IO overload
- CPU load
- memory load
- network latency
- packet loss
- process termination
- read I/O load
- write I/O load
- disk space consumption
- container CPU load
- container memory load
- container network packet retransmission
- container network packet corruption
- container network latency
- container packet loss
- container process termination
- container read I/O load
- container write I/O load
- node CPU load
- node CPU spike
- node memory consumption
- node disk read I/O consumption
- node disk write I/O consumption
- node disk space consumption
"""

schema = f"""
1. SYSTEM & DATA BACKGROUND
- This is a microservice-based e-commerce system deployed on Kubernetes. Each service runs multiple pods on different nodes.
- Telemetry data includes metrics, logs, and traces, all stored in Parquet format. All timestamps and file/directory names use UTC.

2. DATA DIRECTORY & FILE NAMING
- $DATE is the date of the data, e.g., 2025-01-01 in YYYY-MM-DD format.
- All data is located under dataset/phaseone/$DATE/, organized into three categories: log, trace, and metric.

- Logs:
  - Path: dataset/phaseone/$DATE/log-parquet/
  - Example file: log_filebeat-server_$DATE_HH-00-00.parquet
  - One file per hour, containing all logs for that hour.
  - Main columns, types, and descriptions:
    - k8_namespace (object): Kubernetes namespace of the pod.
    - @timestamp (object): Log event timestamp (UTC).
    - agent_name (object): Name of the log collection agent.
    - k8_pod (object): Name of the pod where the log was generated.
    - message (object): Log message content.
    - k8_node_name (object): Name of the node where the pod is running.
    - Other columns may exist but are less common.

- Traces:
  - Path: dataset/phaseone/$DATE/trace-parquet/
  - Example file: trace_jaeger-span_$DATE_HH-00-00.parquet
  - One file per hour, containing all trace data for that hour.
  - Main columns, types, and descriptions:
    - traceID (object): Unique identifier for the trace.
    - spanID (object): Unique identifier for the span.
    - flags (float64): Trace flags (e.g., sampling info).
    - operationName (object): Name of the operation or method.
    - references (object): Parent/child span relationships.
    - startTime (int64): Span start time (epoch ms).
    - startTimeMillis (int64): Span start time in milliseconds.
    - duration (int64): Duration of the span (microseconds or ms).
    - tags (object): Key-value tags for the span.
    - logs (object): Logs/events attached to the span.
    - process (object): Process/service info for the span.

- Metrics:
  - Path: dataset/phaseone/$DATE/metric-parquet/
  - This directory contains multiple subfolders, each with a specific purpose. For each subfolder, the columns and types are consistent across all files in that subfolder. Below are the details for each subfolder:

    - apm/service/: Service-level business metrics (e.g., service_adservice_$DATE.parquet)
      - Columns and types:
        - time (object): Timestamp of the metric record (UTC)
        - client_error (int64): Number of client-side errors
        - client_error_ratio (float64): Ratio of client errors to total requests
        - error (int64): Total number of errors
        - error_ratio (float64): Error ratio
        - object_id (object): Service identifier
        - object_type (object): Type of the object (service)
        - request (int64): Number of requests
        - response (int64): Number of responses
        - rrt (float64): Response round-trip time (ms)
        - rrt_max (int64): Maximum response time (ms)
        - server_error (int64): Number of server-side errors
        - server_error_ratio (int64): Ratio of server errors
        - timeout (int64): Number of timeouts

    - apm/pod/: Pod-level business metrics (e.g., pod_cartservice-1_$DATE.parquet)
      - Columns and types: (same as apm/service/)
        - [see above]

    - infra/infra_pod/: Pod-level infrastructure resource metrics (e.g., infra_pod_pod_cpu_usage_$DATE.parquet)
      - Columns and types:
        - time (object): Timestamp of the metric record (UTC)
        - cf (object): Column family or metric group
        - device (object): Device name (if applicable)
        - instance (object): Instance identifier
        - kpi_key (object): KPI key name
        - kpi_name (object): KPI descriptive name
        - kubernetes_node (object): Node name
        - mountpoint (object): Mount point (for storage metrics)
        - namespace (object): Kubernetes namespace
        - object_type (object): Type of the object (pod)
        - pod (object): Pod name
        - pod_cpu_usage (float64): Pod CPU usage
        - sql_type (object): SQL type (if applicable)
        - type (object): Metric type
        - [other metric columns]: Depending on the file, may include pod_memory_usage, pod_network_rx, pod_network_tx, etc. (float64)

    - infra/infra_node/: Node-level infrastructure resource metrics (e.g., infra_node_node_cpu_usage_rate_$DATE.parquet)
      - Columns and types:
        - time (object): Timestamp of the metric record (UTC)
        - cf (object): Column family or metric group
        - device (object): Device name (if applicable)
        - instance (object): Instance identifier
        - kpi_key (object): KPI key name
        - kpi_name (object): KPI descriptive name
        - kubernetes_node (object): Node name
        - mountpoint (object): Mount point (for storage metrics)
        - namespace (object): Kubernetes namespace
        - node_cpu_usage_rate (float64): Node CPU usage rate
        - object_type (object): Type of the object (node)
        - pod (object): Pod name (if applicable)
        - sql_type (object): SQL type (if applicable)
        - type (object): Metric type
        - [other metric columns]: Depending on the file, may include node_memory_usage, node_disk_read, node_disk_write, etc. (float64)

    - infra/infra_tidb/: TiDB database-related metrics (e.g., infra_tidb_block_cache_size_$DATE.parquet)
      - Columns and types:
        - time (object): Timestamp of the metric record (UTC)
        - block_cache_size (int64): Block cache size
        - cf (object): Column family or metric group
        - device (object): Device name (if applicable)
        - instance (object): Instance identifier
        - kpi_key (object): KPI key name
        - kpi_name (object): KPI descriptive name
        - kubernetes_node (object): Node name
        - mountpoint (object): Mount point (for storage metrics)
        - namespace (object): Kubernetes namespace
        - object_type (object): Type of the object (tidb)
        - pod (object): Pod name (if applicable)
        - sql_type (object): SQL type (if applicable)
        - type (object): Metric type
        - [other metric columns]: Depending on the file, may include tidb_qps, tidb_query_duration, etc. (float64/int64)

    - other/: Other infrastructure and database-related metrics (e.g., infra_pd_abnormal_region_count_$DATE.parquet, infra_tikv_qps_$DATE.parquet)
      - Columns and types:
        - time (object): Timestamp of the metric record (UTC)
        - cf (object): Column family or metric group
        - device (object): Device name (if applicable)
        - instance (object): Instance identifier
        - kpi_key (object): KPI key name
        - kpi_name (object): KPI descriptive name
        - kubernetes_node (object): Node name
        - mountpoint (object): Mount point (for storage metrics)
        - namespace (object): Kubernetes namespace
        - object_type (object): Type of the object (e.g., pd, tikv)
        - pod (object): Pod name (if applicable)
        - [metric_value] (int64/float64): The actual metric value, e.g., abnormal_region_count, qps, rocksdb_write_stall, etc. (the specific metric column varies by file)
        - sql_type (object): SQL type (if applicable)
        - type (object): Metric type

3. DATA CONTENT & USAGE
- Metrics: Service/pod/node-level KPIs (e.g. latency, error, timeout, resource usage, etc.).
  When performing aggregation or statistical analysis (such as quantile, mean, etc.), only apply these operations to numeric columns (e.g., float, int). Do not attempt to aggregate or calculate quantiles on non-numeric (object/string) columns.
  When handling time columns (e.g., 'time'), always check if the column is timezone-aware. If it is tz-aware, use `tz_convert` to change timezone. If it is tz-naive, use `tz_localize` to set the timezone. Do not use `tz_localize` on a tz-aware column.
  **Always use `pd.to_datetime` to ensure the time column is in datetime format before any `.dt` operations. Only use `.dt.tz_localize` or `.dt.tz_convert` on datetimelike columns.**
- Logs: Application and system logs, each row contains pod, node, timestamp, message, etc.
- Traces: Distributed tracing spans, each row contains service, operation, startTime, etc.
- Use pandas or similar tools to analyze, correlate, and identify root causes from all available data.

4. DATA AVAILABILITY
- For some queries, the corresponding time window may lack one or two types among log, metric, and trace data. In such cases, analyze and reason based on the available data types only.
- When encountering NULL/NaN/empty values, handle them appropriately. If a column contains only invalid data (e.g., all values are empty or NULL), you may skip this column in the analysis to reduce complexity.
- **All data is in UTC. Log and trace files are hourly, metric files are daily. Always merge all files that may cover the target time window before filtering by time. Do not assume data is missing unless you have checked all relevant files and confirmed no records exist in the specified range.**

"""