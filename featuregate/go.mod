module go.opentelemetry.io/collector/featuregate

go 1.19

require (
	github.com/stretchr/testify v1.12.1
	go.uber.org/atomic v1.10.0
)

require go.yaml.in/yaml/v3 v3.0.5 // indirect

retract v0.69.0 // Release failed, use v0.69.1
