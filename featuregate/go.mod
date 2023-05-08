module go.opentelemetry.io/collector/featuregate

go 1.19

require (
	github.com/stretchr/testify v1.8.2
	go.uber.org/atomic v1.11.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract v0.69.0 // Release failed, use v0.69.1
