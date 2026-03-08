build:
	cd engine && go build -o sankofa ./cmd/sankofa

run:
	cd engine && go run cmd/sankofa/main.go cmd/sankofa/oss_init.go

run-ee:
	cd engine && go run -tags enterprise cmd/sankofa/main.go cmd/sankofa/ee_init.go
