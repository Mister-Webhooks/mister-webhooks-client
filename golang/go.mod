module github.com/mister-webhooks/mister-webhooks-client

go 1.23.0

toolchain go1.24.1

require github.com/jessekempf/mister-kafka v0.0.11

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/segmentio/kafka-go v0.4.47
	github.com/stretchr/testify v1.10.0
)

require (
	github.com/gabriel-vasile/mimetype v1.4.8 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	golang.org/x/crypto v0.33.0 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/fxamacker/cbor/v2 v2.8.0
	github.com/go-playground/validator/v10 v10.26.0
	github.com/hamba/avro/v2 v2.28.0
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)

replace github.com/segmentio/kafka-go v0.4.47 => github.com/jessekempf/kafka-go v0.4.47-jessekempf-p1
