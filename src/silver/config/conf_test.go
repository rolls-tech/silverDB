package config

import (
	"testing"
)

func TestConf(t *testing.T) {
	path := "D:\\dev\\silver\\src\\config\\conf.toml"
	GetStorageConf(path)
}
