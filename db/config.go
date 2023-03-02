package db

type Mongo struct {
	Connect          string `yaml:"connect"`
	Database         string `yaml:"database"`
	SpacesCollection string `yaml:"spaces"`
	LogCollection    string `yaml:"log"`
}
