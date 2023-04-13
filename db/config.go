package db

type Mongo struct {
	Connect  string `yaml:"connect"`
	Database string `yaml:"database"`
}
