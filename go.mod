module github.com/Velocidex/grpc-go-pool

go 1.21

toolchain go1.23.2

require google.golang.org/grpc v1.65.0

require (
	golang.org/x/net v0.30.0 // indirect
	golang.org/x/sys v0.26.0 // indirect
	golang.org/x/text v0.19.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241015192408-796eee8c2d53 // indirect
	google.golang.org/protobuf v1.35.1 // indirect
)

replace google.golang.org/grpc => github.com/grpc/grpc-go v1.67.1
