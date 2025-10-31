package config

import (
	"errors"
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	AppName     string `mapstructure:"APP_NAME"`
	ServerHost  string `mapstructure:"SERVER_HOST"`
	ServerPort  int    `mapstructure:"SERVER_PORT"`
	DatabaseURL string `mapstructure:"DATABASE_URL"`
	LogLevel    string `mapstructure:"LOG_LEVEL"`
}

func Load() (*Config, error) {
	v := viper.New()
	v.SetConfigFile(".env")
	v.SetConfigType("env")
	v.AutomaticEnv()

	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		var configFileNotFound viper.ConfigFileNotFoundError
		if !errors.As(err, &configFileNotFound) {
			return nil, fmt.Errorf("load config: %w", err)
		}
	}

	cfg := new(Config)
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	normalize(cfg)

	return cfg, nil
}

func (c *Config) ServerAddress() string {
	return fmt.Sprintf("%s:%d", c.ServerHost, c.ServerPort)
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("APP_NAME", "dbos-poc")
	v.SetDefault("SERVER_HOST", "0.0.0.0")
	v.SetDefault("SERVER_PORT", 8080)
	v.SetDefault("LOG_LEVEL", "info")
}

func normalize(cfg *Config) {
	if cfg.AppName == "" {
		cfg.AppName = "dbos-poc"
	}
	if cfg.ServerHost == "" {
		cfg.ServerHost = "0.0.0.0"
	}
	if cfg.ServerPort == 0 {
		cfg.ServerPort = 8080
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
}
