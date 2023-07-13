package filelimit

type configGetter interface {
	GetFileLimit() Config
}

type Config struct {
	LimitDefault      uint64 `yaml:"limitDefault"`
	LimitAlphaUsers   uint64 `yaml:"limitAlphaUsers"`
	LimitNightlyUsers uint64 `yaml:"limitNightlyUsers"`
}
