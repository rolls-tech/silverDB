module silver

go 1.14

require (
	github.com/boltdb/bolt v1.3.1
	github.com/coreos/bbolt v1.3.4 // indirect
	github.com/coreos/etcd v3.3.20+incompatible
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.1
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.3.3
	github.com/golang/snappy v0.0.1
	github.com/google/uuid v1.1.1 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.14.4 // indirect
	github.com/hashicorp/memberlist v0.2.0
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/prometheus/client_golang v1.5.1 // indirect
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20200122045848-3419fae592fc // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.4 // indirect
	go.uber.org/zap v1.14.1 // indirect
	golang.org/x/net v0.0.0-20191002035440-2ec189313ef0
	golang.org/x/time v0.0.0-20200416051211-89c76fbcd5d1 // indirect
	golang.org/x/tools v0.0.0-20191104232314-dc038396d1f0 // indirect
	google.golang.org/genproto v0.0.0-20200420144010-e5e8543f8aeb // indirect
	google.golang.org/grpc v1.29.0 // indirect
	gopkg.in/yaml.v2 v2.2.8
	sigs.k8s.io/yaml v1.2.0 // indirect
	stathat.com/c/consistent v1.0.0
)

replace github.com/coreos/bbolt v1.3.4 => go.etcd.io/bbolt v1.3.4

replace go.etcd.io/bbolt v1.3.4 => github.com/coreos/bbolt v1.3.4
