# Route Connector

The `routeconnector` can be used to conditionally route signals between pipelines
of the same type. It is loosely based on the `routingprocessor`, and behaves like a
`switch` statement. Each case, if satisfied, will route to one or more pipelines.
Additionally, one or more default pipelines may be specified.

## Example Config

The following configuration will do the following:

- Read logs from `./local/in/test.log`, expecting the following format: `sev=debug`, `sev=error`, etc
- Route error (or higher) logs to `./local/out/log_errs.log`
- Route other logs (lower than error) to `./local/out/log_other.log`
- Count the number of logs read at a time
- Route counts of `1` to `./local/out/count_ones.log`
- Route other counts to `./local/out/count_other.log`

```yaml
receivers:
  filelog:
    include: ./local/router/in/test.log
    operators:
      - type: key_value_parser
        delimiter: "="
        severity:
          parse_from: attributes.sev

connectors:
  route/logs:
    logs:
      - min_severity: error
        pipelines: logs/errs
      - match_all: true
        pipelines: logs/other
  count:
  route/counts:
    metrics:
      - max_int: 1
        pipelines: metrics/ones
      - match_all: true
        pipelines: metrics/other

exporters:
  file/log_errs:
    path: ./local/router/out/log_errs.log
  file/log_other:
    path: ./local/router/out/log_other.log
  file/count_ones:
    path: ./local/router/out/count_ones.log
  file/count_other:
    path: ./local/router/out/count_other.log


service:
  pipelines:
    logs/in:
      receivers: [filelog]
      exporters: [route/logs, count]
    logs/errs:
      receivers: [route/logs]
      exporters: [file/log_errs]
    logs/other:
      receivers: [route/logs]
      exporters: [file/log_other]

    metrics/counts:
      receivers: [count]
      exporters: [route/counts]
    metrics/ones:
      receivers: [route/counts]
      exporters: [file/count_ones]
    metrics/other:
      receivers: [route/counts]
      exporters: [file/count_other]
```

## Limitations

This is a very preliminary implementation:

- Only supports logs & metrics
- "Conditions" are not actually supported yet. Currently, routes only to defualt route, or to
 a route with `condition: "true"`

## Supported connection types

Connectors are always used in two or more pipelines. Therefore, support and stability
are defined per _pair of signal types_. The pipeline in which a connector is used as
an exporter is referred to below as the "Exporter pipeline". Likewise, the pipeline in
which the connector is used as a receiver is referred to below as the "Receiver pipeline".

| Exporter pipeline | Receiver pipeline | Stability         |
| ----------------- | ----------------- | ----------------- |
| traces            | traces            | [in development] (soon) |
| metrics           | metrics           | [in development]  |
| logs              | logs              | [in development]  |

[in development]:https://github.com/open-telemetry/opentelemetry-collector#in-development
