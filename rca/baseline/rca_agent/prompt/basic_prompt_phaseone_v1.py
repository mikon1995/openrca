cand = """## POSSIBLE ROOT CAUSE COMPONENTS:
# In metric files, the column for COMPONENTS is: object_id (apm/service/), object_id (apm/pod/), object_id (infra/infra_tidb/), object_id/object_type (other/), service name (logs/traces)
(if the root cause is at the node level, i.e., the root cause is a specific node)

- frontend
- productcatalogservice
- recommendationservice
- cartservice
- checkoutservice
- emailservice
- shippingservice
- currencyservice
- adservice
- paymentservice
- redis-cart


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
- aiops-k8s-02
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

schema = """
1. SYSTEM & DATA BACKGROUND
- This is a microservice-based e-commerce system deployed on Kubernetes. Each service runs multiple pods on different nodes.
- Telemetry data includes metrics, logs, and traces, all stored in Parquet format. All timestamps and file/directory names use UTC.

2. DATA DIRECTORY & FILE NAMING
- All data for each problem is located under dataset/phaseone/processed_data_v1/problems_data/3/problem_[uuid]/...
- Each problem_[uuid] directory contains:
  - A metadata.json file, which provides the description, mapping, and context for all data files in this directory. You MUST read the metadata.json file first before reading any data file.
  - One or more preprocessed parquet files for metrics, logs, and traces, already filtered to the relevant time window and components for this problem.
    - If 'log_data' in metadata['data_stats'], there is a log_data.parquet file. See metadata['data_stats']['log_data']['file_path'] for the exact path.
    - If 'trace_data' in metadata['data_stats'], there is a trace_data.parquet file. See metadata['data_stats']['trace_data']['file_path'] for the exact path.
    - If 'metric_data' in metadata['data_stats'], it is a list. Each item describes one or more metric parquet files with the same columns. Each item contains:
      - 'names': a list of metric file base names (without extension)
      - 'columns': the list of columns for these files
      - 'file_paths': a list of full paths to the corresponding parquet files
      - 'rows': total number of rows in all these files
    - You MUST use metadata['data_stats']['metric_data'] to enumerate all metric files and their columns. Do not assume a fixed file name or structure for metrics.

- Example directory structure:
  dataset/phaseone/processed_data_v1/problems_data/3/problem_3da57a36-286
    |- log_data.parquet
    |- metric-parquet/apm/pod/pod_adservice-1.parquet
    |- metric-parquet/apm/pod/pod_adservice-2.parquet
    |- ... (other metric files, possibly grouped by columns)
    |- trace_data.parquet
    |- metadata.json
    
    Example content of metadata.json:
    {
      "problem_id": "3da57a36-286",
      "anomaly_description": "A fault was detected from 2025-06-14T10:10:38Z to 2025-06-14T10:26:38Z. Please analyze its root cause.",
      "time_range": {
        "start": "2025-06-14T10:07:38+00:00",
        "end": "2025-06-14T10:29:38+00:00",
        "offset_minutes": 3
      },
      "data_stats": {
        "log_data": {
          "rows": 148726,
          "columns": [ ... ],
          "file_path": "log_data.parquet"
        },
        "trace_data": {
          "rows": 152747,
          "columns": [ ... ],
          "file_path": "trace_data.parquet"
        },
        "metric_data": {
          "rows": 16386,
          "columns": [ ... ],
          "file_paths": ["metric-parquet/apm/pod/pod_adservice-1.parquet", "metric-parquet/apm/pod/pod_adservice-2.parquet"]
        }
      }
    }
    you can get the file path of each data file by:
      - log data: 
        - file path: json.load(open('metadata.json'))['data_stats']['log_data']['file_path']
        - columns: json.load(open('metadata.json'))['data_stats']['log_data']['columns']
      - trace data: 
        - file path: json.load(open('metadata.json'))['data_stats']['trace_data']['file_path']
        - columns: json.load(open('metadata.json'))['data_stats']['trace_data']['columns']
      - metric data: 
        - file paths: json.load(open('metadata.json'))['data_stats']['metric_data']['file_paths']
        - columns: json.load(open('metadata.json'))['data_stats']['metric_data']['columns']

- The metadata.json file describes the content, time window, and mapping of each data file in the directory, and should be used as the authoritative reference for data interpretation. You MUST always use metadata.json to determine the available metric files, their columns, and paths. Never hardcode metric file names or columns.
"""
schema += cand
schema += """
  All time in the data is in UTC timezone.

"""
schema += """  
data of current problem you are solving is at dataset/phaseone/processed_data/problems_data/3/problem_"""