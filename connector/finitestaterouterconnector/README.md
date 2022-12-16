# Finite State Route Connector

The `finitestaterouteconnector` can be used to define multiple sets of routing rules (states), along with trigger rules that switch between these states.

## Example Config

```yaml
receivers:
  filelog:
    include: ./local/finitestaterouter/in/test.log
    operators:
      - type: key_value_parser
        delimiter: "="
        severity:
          parse_from: attributes.sev

connectors:
  count:
  finitestaterouter:
    initial_state: normal
    states:
      normal:
        logs:
          - min_severity: error
            pipelines: logs/gcp
        metrics:
          - match_all: true
            pipelines: metrics/gcp
        triggers:
          - metric_threshold:
              metric_name: log.count
              above: 3
              times: 2
            actions:
              - alert:
                  message: "Problem has occurred! Increasing log level!"
                  pipelines: logs/alerts
              - set_state: debug
      debug:
        logs:
          - min_severity: debug
            pipelines: logs/gcp
        metrics:
          - match_all: true
            pipelines: metrics/gcp
        triggers:
          - metric_threshold:
              metric_name: log.count
              below: 2
              times: 2
            actions:
              - alert:
                  message: "Problem appears to have resolved. Decreasing log level."
                  pipelines: logs/alerts
              - set_state: cooldown
          - metric_threshold:
              metric_name: log.count
              above: 3
              times: 2
            actions:
              - alert:
                  message: "Problem has escalated! Failing over to alternate pipeline."
                  pipelines: logs/alerts
              - set_state: failover
      cooldown:
        logs:
          - min_severity: info
            pipelines: logs/gcp
        metrics:
          - match_all: true
            pipelines: metrics/gcp
        triggers:
          - metric_threshold:
              metric_name: log.count
              above: 3
              times: 2
            actions:
              - alert:
                  message: "Problem has reoccured! Increasing log level again."
                  pipelines: logs/alerts
              - set_state: debug
          - metric_threshold:
              metric_name: log.count
              below: 2
              times: 2
            actions:
              - alert:
                  message: "Problem resolved. Returning log level to normal."
                  pipelines: logs/alerts
              - set_state: normal
      failover:
        logs:
          - min_severity: trace
            pipelines: logs/failover
        metrics:
          - match_all: true
            pipelines: metrics/gcp
        triggers:
          - metric_threshold:
              metric_name: log.count
              below: 2
              times: 2
            actions:
              - alert:
                  message: "Recovering. Returning cooldown."
                  pipelines: logs/alerts
              - set_state: cooldown

exporters:
  file/gcp_logs:
    path: ./local/finitestaterouter/out/gcp_logs.log
  file/gcp_metrics:
    path: ./local/finitestaterouter/out/gcp_metrics.log
  file/alerts:
    path: ./local/finitestaterouter/out/alerts.log
  file/failover:
    path: ./local/finitestaterouter/out/failover.log

service:
  pipelines:
    logs/in:
      receivers: [filelog]
      exporters: [count, finitestaterouter]
    logs/gcp:
      receivers: [finitestaterouter]
      exporters: [file/gcp_logs]
    logs/failover:
      receivers: [finitestaterouter]
      exporters: [file/failover]
    logs/alerts:
      receivers: [finitestaterouter]
      exporters: [file/alerts]
    metrics/counts:
      receivers: [count]
      exporters: [finitestaterouter]
    metrics/gcp:
      receivers: [finitestaterouter]
      exporters: [file/gcp_metrics]
```

## Limitations
