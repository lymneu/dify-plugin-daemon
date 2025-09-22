package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/langgenius/dify-plugin-daemon/internal/types/app"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/cache"
	"github.com/langgenius/dify-plugin-daemon/pkg/entities/plugin_entities"
	"github.com/stretchr/testify/assert"
)

func TestDistributedLoadBalancer_RoundRobin(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	// 创建轮询负载均衡器
	config := &app.HorizontalScalingConfig{
		LoadBalancingStrategy: app.LoadBalancingRoundRobin,
		EnableDistributedMode: true,
	}

	pluginStateManager := NewDistributedPluginStateManager("test-node")
	loadBalancer := NewDistributedLoadBalancer(config, pluginStateManager)

	// 添加节点
	nodes := []string{"node-1", "node-2", "node-3"}
	for _, nodeID := range nodes {
		loadBalancer.AddNode(nodeID, map[string]any{"zone": "test"})
		loadBalancer.UpdateNodeHealth(nodeID, true)
	}

	// 注册插件到所有节点
	pluginID := "test-plugin-round-robin"
	for i, nodeID := range nodes {
		pluginStateManager := NewDistributedPluginStateManager(nodeID)
		err := pluginStateManager.RegisterPlugin(
			fmt.Sprintf("%s-%d", pluginID, i),
			pluginID,
			plugin_entities.PluginRuntimeStateRunning,
		)
		assert.NoError(t, err)
	}

	// 测试轮询选择
	selectedNodes := make(map[string]int)
	for i := 0; i < 9; i++ { // 3个节点 * 3轮
		node, err := loadBalancer.SelectNode(pluginID, []string{})
		assert.NoError(t, err)
		selectedNodes[node]++
	}

	// 每个节点应该被选择3次
	for _, nodeID := range nodes {
		assert.Equal(t, 3, selectedNodes[nodeID], "Node %s should be selected 3 times", nodeID)
	}
}

func TestDistributedLoadBalancer_LeastConnections(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	// 创建最少连接负载均衡器
	config := &app.HorizontalScalingConfig{
		LoadBalancingStrategy: app.LoadBalancingLeastConnections,
		EnableDistributedMode: true,
	}

	pluginStateManager := NewDistributedPluginStateManager("test-node")
	loadBalancer := NewDistributedLoadBalancer(config, pluginStateManager).(*DistributedLoadBalancer)

	// 添加节点
	nodes := []string{"node-low", "node-medium", "node-high"}
	for _, nodeID := range nodes {
		loadBalancer.AddNode(nodeID, map[string]any{})
		loadBalancer.UpdateNodeHealth(nodeID, true)
	}

	// 设置不同的连接数
	loadBalancer.IncrementActiveConnections("node-medium") // 1 connection
	loadBalancer.IncrementActiveConnections("node-high")   // 1 connection
	loadBalancer.IncrementActiveConnections("node-high")   // 2 connections

	// 注册插件到所有节点
	pluginID := "test-plugin-least-conn"
	for i, nodeID := range nodes {
		pluginStateManager := NewDistributedPluginStateManager(nodeID)
		err := pluginStateManager.RegisterPlugin(
			fmt.Sprintf("%s-%d", pluginID, i),
			pluginID,
			plugin_entities.PluginRuntimeStateRunning,
		)
		assert.NoError(t, err)
	}

	// 应该选择连接数最少的节点（node-low）
	selectedNode, err := loadBalancer.SelectNode(pluginID, []string{})
	assert.NoError(t, err)
	assert.Equal(t, "node-low", selectedNode)
}

func TestDistributedLoadBalancer_Random(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	// 创建随机负载均衡器
	config := &app.HorizontalScalingConfig{
		LoadBalancingStrategy: app.LoadBalancingRandom,
		EnableDistributedMode: true,
	}

	pluginStateManager := NewDistributedPluginStateManager("test-node")
	loadBalancer := NewDistributedLoadBalancer(config, pluginStateManager)

	// 添加节点
	nodes := []string{"node-random-1", "node-random-2", "node-random-3"}
	for _, nodeID := range nodes {
		loadBalancer.AddNode(nodeID, map[string]any{})
		loadBalancer.UpdateNodeHealth(nodeID, true)
	}

	// 注册插件到所有节点
	pluginID := "test-plugin-random"
	for i, nodeID := range nodes {
		pluginStateManager := NewDistributedPluginStateManager(nodeID)
		err := pluginStateManager.RegisterPlugin(
			fmt.Sprintf("%s-%d", pluginID, i),
			pluginID,
			plugin_entities.PluginRuntimeStateRunning,
		)
		assert.NoError(t, err)
	}

	// 测试随机选择
	selectedNodes := make(map[string]bool)
	for i := 0; i < 20; i++ {
		node, err := loadBalancer.SelectNode(pluginID, []string{})
		assert.NoError(t, err)
		assert.Contains(t, nodes, node)
		selectedNodes[node] = true
	}

	// 应该选择到多个不同的节点（随机性测试）
	assert.True(t, len(selectedNodes) > 1, "Should select multiple different nodes randomly")
}

func TestDistributedLoadBalancer_ExcludeNodes(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	config := &app.HorizontalScalingConfig{
		LoadBalancingStrategy: app.LoadBalancingRoundRobin,
		EnableDistributedMode: true,
	}

	pluginStateManager := NewDistributedPluginStateManager("test-node")
	loadBalancer := NewDistributedLoadBalancer(config, pluginStateManager)

	// 添加节点
	nodes := []string{"node-include-1", "node-include-2", "node-exclude"}
	for _, nodeID := range nodes {
		loadBalancer.AddNode(nodeID, map[string]any{})
		loadBalancer.UpdateNodeHealth(nodeID, true)
	}

	// 注册插件到所有节点
	pluginID := "test-plugin-exclude"
	for i, nodeID := range nodes {
		pluginStateManager := NewDistributedPluginStateManager(nodeID)
		err := pluginStateManager.RegisterPlugin(
			fmt.Sprintf("%s-%d", pluginID, i),
			pluginID,
			plugin_entities.PluginRuntimeStateRunning,
		)
		assert.NoError(t, err)
	}

	// 排除一个节点
	excludeNodes := []string{"node-exclude"}
	selectedNode, err := loadBalancer.SelectNode(pluginID, excludeNodes)
	assert.NoError(t, err)
	assert.NotEqual(t, "node-exclude", selectedNode)
	assert.Contains(t, []string{"node-include-1", "node-include-2"}, selectedNode)
}

func TestDistributedLoadBalancer_HealthCheck(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	config := &app.HorizontalScalingConfig{
		LoadBalancingStrategy: app.LoadBalancingRoundRobin,
		EnableDistributedMode: true,
	}

	pluginStateManager := NewDistributedPluginStateManager("test-node")
	loadBalancer := NewDistributedLoadBalancer(config, pluginStateManager)

	// 添加节点
	healthyNode := "node-healthy"
	unhealthyNode := "node-unhealthy"

	loadBalancer.AddNode(healthyNode, map[string]any{})
	loadBalancer.AddNode(unhealthyNode, map[string]any{})
	
	loadBalancer.UpdateNodeHealth(healthyNode, true)
	loadBalancer.UpdateNodeHealth(unhealthyNode, false)

	// 注册插件到两个节点
	pluginID := "test-plugin-health"
	for i, nodeID := range []string{healthyNode, unhealthyNode} {
		pluginStateManager := NewDistributedPluginStateManager(nodeID)
		err := pluginStateManager.RegisterPlugin(
			fmt.Sprintf("%s-%d", pluginID, i),
			pluginID,
			plugin_entities.PluginRuntimeStateRunning,
		)
		assert.NoError(t, err)
	}

	// 应该只选择健康的节点
	for i := 0; i < 5; i++ {
		selectedNode, err := loadBalancer.SelectNode(pluginID, []string{})
		assert.NoError(t, err)
		assert.Equal(t, healthyNode, selectedNode)
	}
}

func TestDistributedLoadBalancer_NoAvailableNodes(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	config := &app.HorizontalScalingConfig{
		LoadBalancingStrategy: app.LoadBalancingRoundRobin,
		EnableDistributedMode: true,
	}

	pluginStateManager := NewDistributedPluginStateManager("test-node")
	loadBalancer := NewDistributedLoadBalancer(config, pluginStateManager)

	// 不注册任何插件，测试没有可用节点的情况
	pluginID := "non-existent-plugin"
	_, err := loadBalancer.SelectNode(pluginID, []string{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no available nodes found")
}

func TestDistributedLoadBalancer_WeightedRandom(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	config := &app.HorizontalScalingConfig{
		LoadBalancingStrategy: app.LoadBalancingWeightedRandom,
		EnableDistributedMode: true,
	}

	pluginStateManager := NewDistributedPluginStateManager("test-node")
	loadBalancer := NewDistributedLoadBalancer(config, pluginStateManager).(*DistributedLoadBalancer)

	// 添加节点并设置不同的响应时间
	nodes := []string{"node-fast", "node-slow"}
	for _, nodeID := range nodes {
		loadBalancer.AddNode(nodeID, map[string]any{})
		loadBalancer.UpdateNodeHealth(nodeID, true)
	}

	// 设置不同的响应时间（较快的节点应该获得更高权重）
	loadBalancer.UpdateResponseTime("node-fast", 50*time.Millisecond)
	loadBalancer.UpdateResponseTime("node-slow", 500*time.Millisecond)

	// 注册插件到两个节点
	pluginID := "test-plugin-weighted"
	for i, nodeID := range nodes {
		pluginStateManager := NewDistributedPluginStateManager(nodeID)
		err := pluginStateManager.RegisterPlugin(
			fmt.Sprintf("%s-%d", pluginID, i),
			pluginID,
			plugin_entities.PluginRuntimeStateRunning,
		)
		assert.NoError(t, err)
	}

	// 测试加权随机选择，快节点应该被选择更多次
	selectedNodes := make(map[string]int)
	for i := 0; i < 100; i++ {
		node, err := loadBalancer.SelectNode(pluginID, []string{})
		assert.NoError(t, err)
		selectedNodes[node]++
	}

	// 验证快节点被选择的次数更多（由于是随机的，允许一定误差）
	fastCount := selectedNodes["node-fast"]
	slowCount := selectedNodes["node-slow"]
	
	assert.True(t, fastCount > slowCount, 
		"Fast node should be selected more often than slow node (fast: %d, slow: %d)", 
		fastCount, slowCount)
}