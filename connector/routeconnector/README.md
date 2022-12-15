# Route Connector

The `routeconnector` can be used to conditionally route signals between pipelines
of the same type. It is loosely based on the `routingprocessor`, and behaves like a
`switch` statement. Each case, if satisfied, will route to one or more pipelines.
Additionally, one or more default pipelines may be specified.

## Example Config

```yaml
receivers:
  filelog:
    include: ./local/router/in/test.log

connectors:
  route:
    logs:
      - condition: "false"
        pipelines: logs/out/one
      - condition: "true"
        pipelines: logs/out/two
    default: logs/out/default

exporters:
  file/one:
    path: ./local/router/out/one.log
  file/two:
    path: ./local/router/out/two.log
  file/default:
    path: ./local/router/out/default.log

service:
  pipelines:
    logs/in:
      receivers: [filelog]
      exporters: [route]
    logs/out/one:
      receivers: [route]
      exporters: [file/one]
    logs/out/two:
      receivers: [route]
      exporters: [file/two]
    logs/out/default:
      receivers: [route]
      exporters: [file/default]
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
