package cafeapi

type configGetter interface {
	GetCafeApi() Config
}

type Config struct {
	Url string `yaml:"url"`
}
