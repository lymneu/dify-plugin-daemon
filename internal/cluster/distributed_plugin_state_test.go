package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/langgenius/dify-plugin-daemon/internal/utils/cache"
	"github.com/langgenius/dify-plugin-daemon/pkg/entities/plugin_entities"
	"github.com/stretchr/testify/assert"
)

// setupTestRedis 设置测试Redis环境
func setupTestRedisForCluster(t *testing.T) {
	err := cache.InitRedisClient("localhost:6379", "", "", false, 2) // 使用DB 2进行测试
	if err != nil {
		t.Skipf("Redis not available for testing: %v", err)
	}
}

func TestDistributedPluginStateManager_RegisterPlugin(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	nodeID := "test-node-001"
	manager := NewDistributedPluginStateManager(nodeID)

	// 测试注册插件
	pluginID := "test-plugin-001"
	uniqueIdentifier := "test-plugin/v1.0"
	state := plugin_entities.PluginRuntimeStateInstalled

	err := manager.RegisterPlugin(pluginID, uniqueIdentifier, state)
	assert.NoError(t, err)

	// 验证插件状态
	pluginState, err := manager.GetPluginState(pluginID)
	assert.NoError(t, err)
	assert.Equal(t, pluginID, pluginState.PluginID)
	assert.Equal(t, uniqueIdentifier, pluginState.PluginUniqueIdentifier)
	assert.Equal(t, state, pluginState.State)
	assert.Equal(t, nodeID, pluginState.NodeID)

	// 清理
	manager.UnregisterPlugin(pluginID)
}

func TestDistributedPluginStateManager_UpdatePluginState(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	nodeID := "test-node-002"
	manager := NewDistributedPluginStateManager(nodeID)

	// 先注册插件
	pluginID := "test-plugin-002"
	uniqueIdentifier := "test-plugin-update/v1.0"
	initialState := plugin_entities.PluginRuntimeStateInstalled

	err := manager.RegisterPlugin(pluginID, uniqueIdentifier, initialState)
	assert.NoError(t, err)

	// 更新插件状态
	newState := plugin_entities.PluginRuntimeStateRunning
	err = manager.UpdatePluginState(pluginID, newState)
	assert.NoError(t, err)

	// 验证状态更新
	pluginState, err := manager.GetPluginState(pluginID)
	assert.NoError(t, err)
	assert.Equal(t, newState, pluginState.State)

	// 清理
	manager.UnregisterPlugin(pluginID)
}

func TestDistributedPluginStateManager_FindAvailableNodesForPlugin(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	// 创建多个节点的管理器
	node1 := "test-node-find-001"
	node2 := "test-node-find-002"
	manager1 := NewDistributedPluginStateManager(node1)
	manager2 := NewDistributedPluginStateManager(node2)

	uniqueIdentifier := "test-plugin-find/v1.0"

	// 在两个节点上注册相同的插件
	err := manager1.RegisterPlugin("plugin-on-node1", uniqueIdentifier, plugin_entities.PluginRuntimeStateRunning)
	assert.NoError(t, err)

	err = manager2.RegisterPlugin("plugin-on-node2", uniqueIdentifier, plugin_entities.PluginRuntimeStateRunning)
	assert.NoError(t, err)

	// 从任意管理器查找可用节点
	availableNodes, err := manager1.FindAvailableNodesForPlugin(uniqueIdentifier)
	assert.NoError(t, err)
	assert.Len(t, availableNodes, 2)
	assert.Contains(t, availableNodes, node1)
	assert.Contains(t, availableNodes, node2)

	// 清理
	manager1.UnregisterPlugin("plugin-on-node1")
	manager2.UnregisterPlugin("plugin-on-node2")
}

func TestDistributedPluginStateManager_LocalCaching(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	nodeID := "test-node-cache"
	manager := NewDistributedPluginStateManager(nodeID)

	// 注册插件
	pluginID := "test-plugin-cache"
	uniqueIdentifier := "test-plugin-cache/v1.0"
	state := plugin_entities.PluginRuntimeStateRunning

	err := manager.RegisterPlugin(pluginID, uniqueIdentifier, state)
	assert.NoError(t, err)

	// 第一次获取（从Redis）
	pluginState1, err := manager.GetPluginState(pluginID)
	assert.NoError(t, err)
	assert.Equal(t, pluginID, pluginState1.PluginID)

	// 第二次获取（应该从本地缓存）
	pluginState2, err := manager.GetPluginState(pluginID)
	assert.NoError(t, err)
	assert.Equal(t, pluginState1.PluginID, pluginState2.PluginID)

	// 清理
	manager.UnregisterPlugin(pluginID)
}

func TestDistributedPluginStateManager_TTLExpiration(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	nodeID := "test-node-ttl"
	manager := NewDistributedPluginStateManager(nodeID)

	// 注册插件
	pluginID := "test-plugin-ttl"
	uniqueIdentifier := "test-plugin-ttl/v1.0"
	state := plugin_entities.PluginRuntimeStateRunning

	err := manager.RegisterPlugin(pluginID, uniqueIdentifier, state)
	assert.NoError(t, err)

	// 验证插件存在
	_, err = manager.GetPluginState(pluginID)
	assert.NoError(t, err)

	// 由于TTL时间较长（30分钟），我们模拟过期情况
	// 直接从Redis删除键来模拟过期
	cache.Delete(manager.pluginStateKey(pluginID))

	// 应该无法获取过期的插件
	_, err = manager.GetPluginState(pluginID)
	assert.Error(t, err)
}

func TestDistributedPluginStateManager_ConcurrentAccess(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	nodeID := "test-node-concurrent"
	manager := NewDistributedPluginStateManager(nodeID)

	// 并发注册多个插件
	numPlugins := 10
	done := make(chan bool, numPlugins)

	for i := 0; i < numPlugins; i++ {
		go func(index int) {
			pluginID := fmt.Sprintf("concurrent-plugin-%d", index)
			uniqueIdentifier := fmt.Sprintf("concurrent-plugin-%d/v1.0", index)
			err := manager.RegisterPlugin(pluginID, uniqueIdentifier, plugin_entities.PluginRuntimeStateRunning)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numPlugins; i++ {
		<-done
	}

	// 验证所有插件都已注册
	plugins, err := manager.GetPluginsByNode(nodeID)
	assert.NoError(t, err)
	assert.Len(t, plugins, numPlugins)

	// 清理
	for i := 0; i < numPlugins; i++ {
		pluginID := fmt.Sprintf("concurrent-plugin-%d", i)
		manager.UnregisterPlugin(pluginID)
	}
}

func TestDistributedPluginStateManager_CrossNodeSync(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	// 创建两个不同节点的管理器
	node1 := "sync-node-001"
	node2 := "sync-node-002"
	manager1 := NewDistributedPluginStateManager(node1)
	manager2 := NewDistributedPluginStateManager(node2)

	// 在节点1注册插件
	pluginID := "sync-plugin-001"
	uniqueIdentifier := "sync-plugin/v1.0"
	err := manager1.RegisterPlugin(pluginID, uniqueIdentifier, plugin_entities.PluginRuntimeStateRunning)
	assert.NoError(t, err)

	// 从节点2应该能看到节点1的插件
	pluginState, err := manager2.GetPluginState(pluginID)
	assert.NoError(t, err)
	assert.Equal(t, node1, pluginState.NodeID)
	assert.Equal(t, uniqueIdentifier, pluginState.PluginUniqueIdentifier)

	// 从节点2查找可用节点
	availableNodes, err := manager2.FindAvailableNodesForPlugin(uniqueIdentifier)
	assert.NoError(t, err)
	assert.Contains(t, availableNodes, node1)

	// 清理
	manager1.UnregisterPlugin(pluginID)
}