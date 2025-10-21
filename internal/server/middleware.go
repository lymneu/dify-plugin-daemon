package server

import (
	"errors"
	"io"

	"github.com/gin-gonic/gin"
	"github.com/langgenius/dify-plugin-daemon/internal/cluster"
	"github.com/langgenius/dify-plugin-daemon/internal/db"
	"github.com/langgenius/dify-plugin-daemon/internal/server/constants"
	"github.com/langgenius/dify-plugin-daemon/internal/types/exception"
	"github.com/langgenius/dify-plugin-daemon/internal/types/models"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/cache"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/cache/helper"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/log"
	"github.com/langgenius/dify-plugin-daemon/pkg/entities/plugin_entities"
)

func CheckingKey(key string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// get header X-Api-Key
		if c.GetHeader(constants.X_API_KEY) != key {
			c.AbortWithStatusJSON(401, exception.UnauthorizedError().ToResponse())
			return
		}

		c.Next()
	}
}

func (app *App) FetchPluginInstallation() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		pluginId := ctx.Request.Header.Get(constants.X_PLUGIN_ID)
		if pluginId == "" {
			ctx.AbortWithStatusJSON(400, exception.BadRequestError(errors.New("plugin_id is required")).ToResponse())
			return
		}

		tenantId := ctx.Param("tenant_id")
		if tenantId == "" {
			ctx.AbortWithStatusJSON(400, exception.BadRequestError(errors.New("tenant_id is required")).ToResponse())
			return
		}

		// fetch plugin installation with caching
		cacheKey := helper.PluginInstallationCacheKey(pluginId, tenantId)
		installation, err := cache.AutoGetWithGetter(
			cacheKey,
			func() (*models.PluginInstallation, error) {
				inst, err := db.GetOne[models.PluginInstallation](
					db.Equal("tenant_id", tenantId),
					db.Equal("plugin_id", pluginId),
				)
				if err != nil {
					return nil, err
				}
				return &inst, nil
			},
		)

		if err == db.ErrDatabaseNotFound {
			ctx.AbortWithStatusJSON(404, exception.ErrPluginNotFound().ToResponse())
			return
		}

		if err != nil {
			ctx.AbortWithStatusJSON(500, exception.InternalServerError(err).ToResponse())
			return
		}

		identity, err := plugin_entities.NewPluginUniqueIdentifier(installation.PluginUniqueIdentifier)
		if err != nil {
			ctx.AbortWithStatusJSON(400, exception.UniqueIdentifierError(err).ToResponse())
			return
		}

		ctx.Set(constants.CONTEXT_KEY_PLUGIN_INSTALLATION, *installation)
		ctx.Set(constants.CONTEXT_KEY_PLUGIN_UNIQUE_IDENTIFIER, identity)
		ctx.Next()
	}
}

// DistributedPluginInvoke handles distributed plugin invocation with load balancing
func (app *App) DistributedPluginInvoke() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		// get plugin unique identifier
		identityAny, ok := ctx.Get(constants.CONTEXT_KEY_PLUGIN_UNIQUE_IDENTIFIER)
		if !ok {
			ctx.AbortWithStatusJSON(
				500,
				exception.InternalServerError(errors.New("plugin unique identifier not found")).ToResponse(),
			)
			return
		}

		identity, ok := identityAny.(plugin_entities.PluginUniqueIdentifier)
		if !ok {
			ctx.AbortWithStatusJSON(
				500,
				exception.InternalServerError(errors.New("failed to parse plugin unique identifier")).ToResponse(),
			)
			return
		}

		// 检查是否启用分布式模式
		config := app.GetDefaultHorizontalScalingConfig()
		if !config.EnableDistributedMode {
			// 传统模式：检查插件是否在当前节点
			if ok, originalError := app.cluster.IsPluginOnCurrentNode(identity); !ok {
				app.redirectPluginInvokeByPluginIdentifier(ctx, identity, originalError)
				ctx.Abort()
				return
			}
		} else {
			// 分布式模式：使用负载均衡器选择最佳节点
			if !app.handleDistributedPluginInvoke(ctx, identity) {
				return
			}
		}

		ctx.Next()
	}
}

func (app *App) redirectPluginInvokeByPluginIdentifier(
	ctx *gin.Context,
	plugin_unique_identifier plugin_entities.PluginUniqueIdentifier,
	originalError error,
) {
	// 安全处理originalError
	originalErrorMsg := "plugin not found on current node"
	if originalError != nil {
		originalErrorMsg = originalError.Error()
	}

	// try find the correct node
	nodes, err := app.cluster.FetchPluginAvailableNodesById(plugin_unique_identifier.String())
	if err != nil {
		ctx.AbortWithStatusJSON(
			500,
			exception.InternalServerError(
				errors.New("failed to fetch plugin available nodes, "+originalErrorMsg+", "+err.Error()),
			).ToResponse(),
		)
		return
	} else if len(nodes) == 0 {
		ctx.AbortWithStatusJSON(
			404,
			exception.InternalServerError(
				errors.New("no available node, "+originalErrorMsg),
			).ToResponse(),
		)
		return
	}

	// redirect to the correct node
	nodeId := nodes[0]
	statusCode, header, body, err := app.cluster.RedirectRequest(nodeId, ctx.Request)
	if err != nil {
		log.Error("redirect request failed: %s", err.Error())
		ctx.AbortWithStatusJSON(
			500,
			exception.InternalServerError(errors.New("redirect request failed: "+err.Error())).ToResponse(),
		)
		return
	}

	// set status code
	ctx.Writer.WriteHeader(statusCode)

	// set header
	for key, values := range header {
		for _, value := range values {
			ctx.Writer.Header().Set(key, value)
		}
	}

	for {
		buf := make([]byte, 1024)
		n, err := body.Read(buf)
		if err != nil && err != io.EOF {
			break
		} else if err != nil {
			ctx.Writer.Write(buf[:n])
			ctx.Writer.Flush()
			break
		}

		if n > 0 {
			ctx.Writer.Write(buf[:n])
			ctx.Writer.Flush()
		}
	}
}

func (app *App) InitClusterID() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		ctx.Set(constants.CONTEXT_KEY_CLUSTER_ID, app.cluster.ID())
		ctx.Next()
	}
}

func (app *App) AdminAPIKey(key string) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		if ctx.GetHeader(constants.X_ADMIN_API_KEY) != key {
			ctx.AbortWithStatusJSON(401, gin.H{"message": "unauthorized"})
			return
		}

		ctx.Next()
	}
}

// handleDistributedPluginInvoke 处理分布式插件调用
func (app *App) handleDistributedPluginInvoke(ctx *gin.Context, identity plugin_entities.PluginUniqueIdentifier) bool {
	log.Info("Handling distributed plugin invoke for plugin: %s", identity.String())
	
	// 获取负载均衡器
	loadBalancer := app.cluster.GetLoadBalancer()
	if loadBalancer == nil {
		log.Error("Load balancer not available in distributed mode")
		ctx.AbortWithStatusJSON(
			500,
			exception.InternalServerError(errors.New("load balancer not available")).ToResponse(),
		)
		return false
	}

	// 使用负载均衡器选择最佳节点
	selectedNode, err := loadBalancer.SelectNode(identity.String(), []string{})
	if err != nil {
		log.Error("Failed to select node for plugin %s: %s", identity.String(), err.Error())
		ctx.AbortWithStatusJSON(
			500,
			exception.InternalServerError(errors.New("failed to select node: "+err.Error())).ToResponse(),
		)
		return false
	}
	
	log.Info("Selected node for plugin %s: %s", identity.String(), selectedNode)

	// 检查是否选择了当前节点
	currentNodeID := app.cluster.ID()
	if selectedNode == currentNodeID {
		// 在当前节点处理
		log.Info("Processing plugin %s on current node %s", identity.String(), currentNodeID)
		return true
	}

	// 需要转发到其他节点
	log.Info("Forwarding plugin %s request from node %s to %s", identity.String(), currentNodeID, selectedNode)
	
	// 记录连接数变化
	if distributedLB, ok := loadBalancer.(*cluster.DistributedLoadBalancer); ok {
		distributedLB.IncrementActiveConnections(selectedNode)
		defer distributedLB.DecrementActiveConnections(selectedNode)
	}

	// 转发请求到选定节点
	log.Info("About to redirect request. Method: %s, URL: %s", ctx.Request.Method, ctx.Request.URL.String())
	statusCode, header, body, err := app.cluster.RedirectRequest(selectedNode, ctx.Request)
	if err != nil {
		log.Error("Failed to forward request to node %s: %s", selectedNode, err.Error())
		// 更新节点健康状态
		loadBalancer.UpdateNodeHealth(selectedNode, false)
		ctx.AbortWithStatusJSON(
			500,
			exception.InternalServerError(errors.New("failed to forward request: "+err.Error())).ToResponse(),
		)
		return false
	}
	
	log.Info("Received response from redirected node. Status code: %d", statusCode)

	// 更新节点健康状态
	loadBalancer.UpdateNodeHealth(selectedNode, true)

	// 设置响应
	ctx.Writer.WriteHeader(statusCode)
	for key, values := range header {
		for _, value := range values {
			ctx.Writer.Header().Set(key, value)
		}
	}
	
	// 记录响应头信息
	log.Info("Response headers:")
	for key, values := range header {
		log.Info("  %s: %v", key, values)
	}

	// 流式传输响应体
	log.Info("Streaming response body...")
	for {
		buf := make([]byte, 1024)
		n, err := body.Read(buf)
		if err != nil && err != io.EOF {
			log.Error("Error reading response body: %v", err)
			break
		} else if err != nil {
			if n > 0 {
				ctx.Writer.Write(buf[:n])
				ctx.Writer.Flush()
			}
			log.Info("Finished streaming response body")
			break
		}

		if n > 0 {
			ctx.Writer.Write(buf[:n])
			ctx.Writer.Flush()
		}
	}

	ctx.Abort() // 阻止继续处理
	return false
}
