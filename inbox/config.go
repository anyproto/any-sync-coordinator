package inbox

type Config struct {
	CollName   string `yaml:"collName"`
	FetchLimit int    `yaml:"fetchLimit"`
}
