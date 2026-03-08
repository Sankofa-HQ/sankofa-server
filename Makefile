# build:
# 	cd engine && go build -o sankofa ./cmd/sankofa

# run:
# 	cd engine && go run cmd/sankofa/main.go cmd/sankofa/oss_init.go

# run-ee:
# 	cd engine && go run -tags enterprise cmd/sankofa/main.go cmd/sankofa/ee_init.go


build:
	cd engine && go build -o sankofa ./cmd/sankofa

# The OSS runner automatically picks up oss_init.go and ignores ee_init.go
run:
	cd engine && go run ./cmd/sankofa

# The EE runner automatically picks up ee_init.go and ignores oss_init.go
run-ee:
	cd engine && go run -tags enterprise ./cmd/sankofa