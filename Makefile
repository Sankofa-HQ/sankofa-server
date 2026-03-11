build:
	cd engine && go build -o sankofa ./cmd/sankofa

# The OSS runner automatically picks up oss_init.go and ignores ee_init.go
run:
	cd engine && go run ./cmd/sankofa

release-oss:
	@echo "Building Open Source Edition..."
	cd engine && env GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o sankofa-api ./cmd/sankofa

# The EE runner automatically picks up ee_init.go and ignores oss_init.go
run-ee:
	cd engine && go run -tags enterprise ./cmd/sankofa

release-ee:
	@echo "Building Enterprise Edition for Linux ARM64..."
	cd engine && env GOOS=linux GOARCH=arm64 go build -tags enterprise -ldflags="-s -w" -o sankofa-api ./cmd/sankofa
