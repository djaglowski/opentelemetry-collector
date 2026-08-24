module go.opentelemetry.io/collector/semconv

go 1.19

require (
	github.com/hashicorp/go-version v1.6.0
	github.com/stretchr/testify v1.12.1
)

require go.yaml.in/yaml/v3 v3.0.5 // indirect

retract (
	v0.69.0 // Release failed, use v0.69.1
	v0.57.1 // Release failed, use v0.57.2
	v0.57.0 // Release failed, use v0.57.2
)
