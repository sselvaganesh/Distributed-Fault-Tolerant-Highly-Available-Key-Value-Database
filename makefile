# Makefile

export GOPATH=`pwd`
go get -u github.com/golang/protobuf/protoc-gen-go
go build Replicas/replica.go
go build Client/client.go  
