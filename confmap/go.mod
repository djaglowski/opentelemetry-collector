module go.opentelemetry.io/collector/confmap

go 1.19

require (
	github.com/knadh/koanf v1.5.0
	github.com/mitchellh/mapstructure v1.5.0
	github.com/stretchr/testify v1.12.1
	go.opentelemetry.io/collector/featuregate v0.72.0
	go.uber.org/atomic v1.10.0
	go.uber.org/multierr v1.9.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
)

replace go.opentelemetry.io/collector/featuregate => ../featuregate

retract v0.69.0 // Release failed, use v0.69.1
