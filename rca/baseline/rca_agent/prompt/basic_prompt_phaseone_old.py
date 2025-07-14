cand = """## POSSIBLE ROOT CAUSE COMPONENTS:

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
"""

schema = """
1. SYSTEM & DATA BACKGROUND
- This is a microservice-based e-commerce system deployed on Kubernetes. Each service runs multiple pods on different nodes.
- Telemetry data includes metrics, logs, and traces, all stored in Parquet format. All timestamps and file/directory names use UTC.

2. DATA DIRECTORY & FILE NAMING
- All data is under: dataset/phaseone/{date}/
- Metrics: dataset/phaseone/{date}/metric-parquet/ (contains multiple subdirectories such as apm/service, apm/pod, infra/infra_pod, infra/infra_tidb, infra/infra_node, other, etc.). File names are diverse, e.g. service_adservice_{date}.parquet, pod_cartservice-1_{date}.parquet, infra_node_node_cpu_usage_rate_{date}.parquet. You must traverse all subdirectories and files to collect and process all available metric data.
- Logs: dataset/phaseone/{date}/log-parquet/, e.g. log_filebeat-server_{date}_HH-MM-SS.parquet. **Each hourly file (e.g., 16-00-00) contains data from the start of that hour to the next hour, i.e., 16:00:00~17:00:00.**
- Traces: dataset/phaseone/{date}/trace-parquet/, e.g. trace_jaeger-span_{date}_HH-MM-SS.parquet. **Each hourly file (e.g., 16-00-00) contains data from the start of that hour to the next hour, i.e., 16:00:00~17:00:00.**

3. DATA CONTENT & USAGE
- Metrics: Service/pod/node-level KPIs (e.g. latency, error, timeout, resource usage, etc.).
  Example columns: ['time', 'client_error', 'server_error', 'timeout', 'object_id', ...]
  When performing aggregation or statistical analysis (such as quantile, mean, etc.), only apply these operations to numeric columns (e.g., float, int). Do not attempt to aggregate or calculate quantiles on non-numeric (object/string) columns.
  When handling time columns (e.g., 'time'), always check if the column is timezone-aware. If it is tz-aware, use `tz_convert` to change timezone. If it is tz-naive, use `tz_localize` to set the timezone. Do not use `tz_localize` on a tz-aware column.
  **Always use `pd.to_datetime` to ensure the time column is in datetime format before any `.dt` operations. Only use `.dt.tz_localize` or `.dt.tz_convert` on datetimelike columns.**
- Logs: Application and system logs, each row contains pod, node, timestamp, message, etc.
  Example columns: ['k8_namespace', '@timestamp', 'agent_name', 'k8_pod', 'message', 'k8_node_name', ...]
- Traces: Distributed tracing spans, each row contains service, operation, startTime, etc.
  Example columns: ['traceID', 'spanID', 'operationName', 'startTime', 'duration', 'process', ...]
- Use pandas or similar tools to analyze, correlate, and identify root causes from all available data.

4. DATA AVAILABILITY
- For some queries, the corresponding time window may lack one or two types among log, metric, and trace data. In such cases, analyze and reason based on the available data types only.
- When encountering NULL/NaN/empty values, handle them appropriately. If a column contains only invalid data (e.g., all values are empty or NULL), you may skip this column in the analysis to reduce complexity.
- **All data is in UTC. Log and trace files are hourly, metric files are daily. Always merge all files that may cover the target time window before filtering by time. Do not assume data is missing unless you have checked all relevant files and confirmed no records exist in the specified range.**

"""