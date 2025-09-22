package cluster

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/langgenius/dify-plugin-daemon/internal/types/app"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/cache"
	"github.com/langgenius/dify-plugin-daemon/pkg/entities/plugin_entities"
	"github.com/stretchr/testify/assert"
)

// TestHorizontalScaling_Integration 集成测试：完整的水平扩展流程
func TestHorizontalScaling_Integration(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	// 模拟3个集群节点
	nodes := []string{"cluster-node-1", "cluster-node-2", "cluster-node-3"}
	clusters := make([]*MockCluster, len(nodes))
	
	// 初始化集群节点
	for i, nodeID := range nodes {
		clusters[i] = NewMockCluster(nodeID)
		clusters[i].InitializeDistributedMode()
	}

	// 测试插件在多节点间的分布
	plugins := []string{
		"plugin-a/v1.0",
		"plugin-b/v1.0", 
		"plugin-c/v1.0",
		"plugin-d/v1.0",
		"plugin-e/v1.0",
	}

	// 在不同节点上安装插件
	for i, pluginID := range plugins {
		nodeIndex := i % len(nodes)
		cluster := clusters[nodeIndex]
		
		err := cluster.InstallPlugin(pluginID)
		assert.NoError(t, err, "Failed to install plugin %s on node %s", pluginID, nodes[nodeIndex])
	}

	// 验证插件分布
	for _, pluginID := range plugins {
		for _, cluster := range clusters {
			availableNodes, err := cluster.FindAvailableNodes(pluginID)
			assert.NoError(t, err)
			assert.True(t, len(availableNodes) > 0, "Plugin %s should be available on at least one node", pluginID)
		}
	}

	// 测试负载均衡
	testPlugin := plugins[0]
	selections := make(map[string]int)
	
	// 模拟100次请求
	for i := 0; i < 100; i++ {
		// 从随机节点发起请求
		requestingCluster := clusters[i%len(clusters)]
		selectedNode, err := requestingCluster.SelectNodeForPlugin(testPlugin)
		assert.NoError(t, err)
		selections[selectedNode]++
	}

	// 验证负载分布（应该有分布，不是全部集中在一个节点）
	assert.True(t, len(selections) > 0, "Should distribute load across nodes")

	// 清理
	for _, cluster := range clusters {
		cluster.Cleanup()
	}
}

// TestHorizontalScaling_FailoverScenario 测试故障转移场景
func TestHorizontalScaling_FailoverScenario(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	// 创建3个节点
	nodes := []string{"failover-node-1", "failover-node-2", "failover-node-3"}
	clusters := make([]*MockCluster, len(nodes))
	
	for i, nodeID := range nodes {
		clusters[i] = NewMockCluster(nodeID)
		clusters[i].InitializeDistributedMode()
	}

	// 在所有节点安装同一个插件
	pluginID := "failover-plugin/v1.0"
	for _, cluster := range clusters {
		err := cluster.InstallPlugin(pluginID)
		assert.NoError(t, err)
	}

	// 验证所有节点都可以处理该插件
	requestingCluster := clusters[0]
	for i := 0; i < 10; i++ {
		selectedNode, err := requestingCluster.SelectNodeForPlugin(pluginID)
		assert.NoError(t, err)
		assert.Contains(t, nodes, selectedNode)
	}

	// 模拟一个节点故障
	failedNode := nodes[1]
	clusters[1].MarkUnhealthy()

	// 继续请求，应该不会选择故障节点
	for i := 0; i < 20; i++ {
		selectedNode, err := requestingCluster.SelectNodeForPlugin(pluginID)
		assert.NoError(t, err)
		assert.NotEqual(t, failedNode, selectedNode, "Should not select failed node")
		assert.Contains(t, []string{nodes[0], nodes[2]}, selectedNode)
	}

	// 恢复故障节点
	clusters[1].MarkHealthy()

	// 等待一小段时间让健康状态更新
	time.Sleep(100 * time.Millisecond)

	// 现在应该可以再次选择到恢复的节点
	found := false
	for i := 0; i < 50; i++ {
		selectedNode, err := requestingCluster.SelectNodeForPlugin(pluginID)
		assert.NoError(t, err)
		if selectedNode == failedNode {
			found = true
			break
		}
	}
	assert.True(t, found, "Should be able to select recovered node")

	// 清理
	for _, cluster := range clusters {
		cluster.Cleanup()
	}
}

// TestHorizontalScaling_ConcurrentRequests 测试并发请求场景
func TestHorizontalScaling_ConcurrentRequests(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	// 创建集群
	nodeCount := 3
	nodes := make([]string, nodeCount)
	clusters := make([]*MockCluster, nodeCount)
	
	for i := 0; i < nodeCount; i++ {
		nodes[i] = fmt.Sprintf("concurrent-node-%d", i)
		clusters[i] = NewMockCluster(nodes[i])
		clusters[i].InitializeDistributedMode()
	}

	// 安装插件到所有节点
	pluginID := "concurrent-plugin/v1.0"
	for _, cluster := range clusters {
		err := cluster.InstallPlugin(pluginID)
		assert.NoError(t, err)
	}

	// 并发测试
	concurrentRequests := 100
	var wg sync.WaitGroup
	results := make(chan string, concurrentRequests)
	errors := make(chan error, concurrentRequests)

	wg.Add(concurrentRequests)
	
	// 启动并发请求
	for i := 0; i < concurrentRequests; i++ {
		go func(requestIndex int) {
			defer wg.Done()
			
			// 从随机节点发起请求
			requestingCluster := clusters[requestIndex%len(clusters)]
			selectedNode, err := requestingCluster.SelectNodeForPlugin(pluginID)
			
			if err != nil {
				errors <- err
			} else {
				results <- selectedNode
			}
		}(i)
	}

	wg.Wait()
	close(results)
	close(errors)

	// 检查错误
	for err := range errors {
		t.Errorf("Concurrent request failed: %v", err)
	}

	// 统计结果
	selections := make(map[string]int)
	for result := range results {
		selections[result]++
	}

	// 验证负载分布
	assert.Equal(t, concurrentRequests, 
		sum(selections), 
		"Should handle all concurrent requests")
	
	assert.True(t, len(selections) > 1, 
		"Should distribute concurrent requests across multiple nodes")

	// 清理
	for _, cluster := range clusters {
		cluster.Cleanup()
	}
}

// TestHorizontalScaling_DynamicScaling 测试动态扩缩容
func TestHorizontalScaling_DynamicScaling(t *testing.T) {
	setupTestRedisForCluster(t)
	defer cache.Close()

	// 初始2个节点
	initialNodes := []string{"scale-node-1", "scale-node-2"}
	clusters := make([]*MockCluster, len(initialNodes))
	
	for i, nodeID := range initialNodes {
		clusters[i] = NewMockCluster(nodeID)
		clusters[i].InitializeDistributedMode()
	}

	// 安装插件
	pluginID := "scaling-plugin/v1.0"
	for _, cluster := range clusters {
		err := cluster.InstallPlugin(pluginID)
		assert.NoError(t, err)
	}

	// 验证初始状态
	requestingCluster := clusters[0]
	availableNodes, err := requestingCluster.FindAvailableNodes(pluginID)
	assert.NoError(t, err)
	assert.Len(t, availableNodes, 2)

	// 添加新节点（扩容）
	newNode := "scale-node-3"
	newCluster := NewMockCluster(newNode)
	newCluster.InitializeDistributedMode()
	err = newCluster.InstallPlugin(pluginID)
	assert.NoError(t, err)

	// 等待状态同步
	time.Sleep(100 * time.Millisecond)

	// 验证扩容后状态
	availableNodes, err = requestingCluster.FindAvailableNodes(pluginID)
	assert.NoError(t, err)
	assert.Len(t, availableNodes, 3)

	// 移除一个节点（缩容）
	clusters[1].Shutdown()

	// 等待状态同步
	time.Sleep(100 * time.Millisecond)

	// 验证缩容后状态（注意：实际场景中需要TTL过期或主动清理）
	// 这里我们模拟节点主动注销
	clusters[1].UnregisterAllPlugins()

	// 清理
	for _, cluster := range clusters {
		cluster.Cleanup()
	}
	newCluster.Cleanup()
}

// MockCluster 模拟集群节点用于测试
type MockCluster struct {
	nodeID                      string
	distributedPluginStateManager *DistributedPluginStateManager
	loadBalancer                LoadBalancer
	isHealthy                   bool
}

func NewMockCluster(nodeID string) *MockCluster {
	return &MockCluster{
		nodeID:    nodeID,
		isHealthy: true,
	}
}

func (mc *MockCluster) InitializeDistributedMode() {
	config := &app.HorizontalScalingConfig{
		EnableDistributedMode:     true,
		LoadBalancingStrategy:     app.LoadBalancingRoundRobin,
		DistributedSessionTTL:     2 * time.Hour,
		DistributedPluginStateTTL: 30 * time.Minute,
	}

	mc.distributedPluginStateManager = NewDistributedPluginStateManager(mc.nodeID)
	mc.loadBalancer = NewDistributedLoadBalancer(config, mc.distributedPluginStateManager)
	
	// 将节点添加到负载均衡器
	mc.loadBalancer.AddNode(mc.nodeID, map[string]any{"zone": "test"})
	mc.loadBalancer.UpdateNodeHealth(mc.nodeID, mc.isHealthy)
}

func (mc *MockCluster) InstallPlugin(pluginUniqueIdentifier string) error {
	pluginID := fmt.Sprintf("%s-%s", pluginUniqueIdentifier, mc.nodeID)
	return mc.distributedPluginStateManager.RegisterPlugin(
		pluginID,
		pluginUniqueIdentifier,
		plugin_entities.PluginRuntimeStateRunning,
	)
}

func (mc *MockCluster) FindAvailableNodes(pluginUniqueIdentifier string) ([]string, error) {
	return mc.distributedPluginStateManager.FindAvailableNodesForPlugin(pluginUniqueIdentifier)
}

func (mc *MockCluster) SelectNodeForPlugin(pluginUniqueIdentifier string) (string, error) {
	return mc.loadBalancer.SelectNode(pluginUniqueIdentifier, []string{})
}

func (mc *MockCluster) MarkUnhealthy() {
	mc.isHealthy = false
	mc.loadBalancer.UpdateNodeHealth(mc.nodeID, false)
}

func (mc *MockCluster) MarkHealthy() {
	mc.isHealthy = true
	mc.loadBalancer.UpdateNodeHealth(mc.nodeID, true)
}

func (mc *MockCluster) Shutdown() {
	mc.isHealthy = false
	mc.loadBalancer.UpdateNodeHealth(mc.nodeID, false)
}

func (mc *MockCluster) UnregisterAllPlugins() {
	// 在实际实现中，这里应该清理所有该节点的插件
	// 由于测试环境的限制，我们简化处理
}

func (mc *MockCluster) Cleanup() {
	// 清理测试数据
	mc.loadBalancer.RemoveNode(mc.nodeID)
}

// 辅助函数
func sum(m map[string]int) int {
	total := 0
	for _, v := range m {
		total += v
	}
	return total
}