package server

import (
	"github.com/langgenius/dify-plugin-daemon/internal/cluster"
	"github.com/langgenius/dify-plugin-daemon/internal/core/plugin_daemon/backwards_invocation/transaction"
	"github.com/langgenius/dify-plugin-daemon/internal/types/app"
)

type App struct {
	// cluster instance of this node
	// schedule all the tasks related to the cluster, like request direct
	cluster *cluster.Cluster

	// endpoint handler
	// customize behavior of endpoint
	endpointHandler EndpointHandler

	// serverless transaction handler
	// accept serverless transaction request and forward to the plugin daemon
	serverlessTransactionHandler *transaction.ServerlessTransactionHandler

	// 水平扩展配置
	config *app.Config
}

// GetDefaultHorizontalScalingConfig 获取默认的水平扩展配置
func (appInstance *App) GetDefaultHorizontalScalingConfig() *app.HorizontalScalingConfig {
	if appInstance.config != nil && appInstance.config.HorizontalScaling != nil {
		return appInstance.config.HorizontalScaling
	}
	// 返回默认配置 - 调用包级别的函数
	return app.GetDefaultHorizontalScalingConfig()
}

// SetConfig 设置应用配置
func (appInstance *App) SetConfig(config *app.Config) {
	appInstance.config = config
}