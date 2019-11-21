package conf

type Cache struct {
	Enable bool `yaml:"enable"`
	List []string `yaml:"list,flow"`
}

type Storage struct {
	Type string `yaml:"type"`
	Node string `yaml:"node"`
	HostName string `yaml:"hostName"`
	HttpPort int `yaml:"httpPort"`
	TcpPort int `yaml:"tcpPort"`
	Cluster []string `yaml:"cluster,flow"`
	DataPath []string `yaml:"dataPath,flow"`
}

