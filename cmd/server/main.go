package main

import (
	"github.com/joho/godotenv"
	"github.com/kelseyhightower/envconfig"
	"github.com/langgenius/dify-plugin-daemon/internal/server"
	"github.com/langgenius/dify-plugin-daemon/internal/types/app"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/log"
)

func main() {
	var config app.Config

	// load env
	godotenv.Load()

	err := envconfig.Process("", &config)
	if err != nil {
		log.Panic("Error processing environment variables: %s", err.Error())
	}

	// 手动加载水平扩展配置
	var horizontalScalingConfig app.HorizontalScalingConfig
	err = envconfig.Process("", &horizontalScalingConfig)
	if err != nil {
		log.Panic("Error processing horizontal scaling environment variables: %s", err.Error())
	}
	config.HorizontalScaling = &horizontalScalingConfig

	config.SetDefault()

	if err := config.Validate(); err != nil {
		log.Panic("Invalid configuration: %s", err.Error())
	}

	(&server.App{}).Run(&config)
}
