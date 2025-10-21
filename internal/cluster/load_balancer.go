package cluster

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/langgenius/dify-plugin-daemon/internal/types/app"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/log"
)

// LoadBalancer 负载均衡器接口
type LoadBalancer interface {
	// 选择最佳节点处理请求
	SelectNode(pluginUniqueIdentifier string, excludeNodes []string) (string, error)
	// 更新节点健康状态
	UpdateNodeHealth(nodeID string, isHealthy bool)
	// 获取所有可用节点
	GetAvailableNodes() []string
	// 添加节点
	AddNode(nodeID string, metadata map[string]any)
	// 移除节点
	RemoveNode(nodeID string)
}

// NodeInfo 节点信息
type NodeInfo struct {
	ID               string            `json:"id"`
	LastSeenAt       time.Time         `json:"last_seen_at"`
	IsHealthy        bool              `json:"is_healthy"`
	LoadScore        float64           `json:"load_score"`        // 负载评分（0-1，越低越好）
	ActiveConnections int64            `json:"active_connections"` // 活跃连接数
	Metadata         map[string]any    `json:"metadata"`
	ResponseTime     time.Duration     `json:"response_time"`     // 平均响应时间
	FailureCount     int64             `json:"failure_count"`     // 连续失败次数
}

// DistributedLoadBalancer 分布式负载均衡器实现
type DistributedLoadBalancer struct {
	strategy          string
	nodes             sync.Map // map[string]*NodeInfo
	roundRobinCounter int64
	config            *app.HorizontalScalingConfig
	pluginStateManager *DistributedPluginStateManager
}

// NewDistributedLoadBalancer 创建分布式负载均衡器
func NewDistributedLoadBalancer(config *app.HorizontalScalingConfig, pluginStateManager *DistributedPluginStateManager) LoadBalancer {
	return &DistributedLoadBalancer{
		strategy:            config.LoadBalancingStrategy,
		config:              config,
		pluginStateManager: pluginStateManager,
	}
}

// SelectNode 选择最佳节点处理请求
func (dlb *DistributedLoadBalancer) SelectNode(pluginUniqueIdentifier string, excludeNodes []string) (string, error) {
	log.Info("Selecting node for plugin: %s", pluginUniqueIdentifier)
	
	// 首先查找有该插件的节点
	availableNodes, err := dlb.pluginStateManager.FindAvailableNodesForPlugin(pluginUniqueIdentifier)
	if err != nil {
		log.Error("Failed to find available nodes: %v", err)
		return "", fmt.Errorf("failed to find available nodes: %w", err)
	}

	log.Info("Available nodes for plugin %s: %v", pluginUniqueIdentifier, availableNodes)
	
	if len(availableNodes) == 0 {
		log.Error("No available nodes found for plugin: %s", pluginUniqueIdentifier)
		return "", errors.New("no available nodes found for plugin")
	}

	// 过滤掉排除的节点
	filteredNodes := make([]string, 0, len(availableNodes))
	excludeSet := make(map[string]bool)
	for _, nodeID := range excludeNodes {
		excludeSet[nodeID] = true
	}

	for _, nodeID := range availableNodes {
		if !excludeSet[nodeID] {
			// 检查节点健康状态
			if info := dlb.getNodeInfo(nodeID); info != nil && info.IsHealthy {
				filteredNodes = append(filteredNodes, nodeID)
				log.Info("Node %s is healthy and available", nodeID)
			} else {
				log.Info("Node %s is not healthy or not available", nodeID)
			}
		}
	}

	log.Info("Filtered healthy nodes: %v", filteredNodes)
	
	if len(filteredNodes) == 0 {
		log.Error("No healthy nodes available for plugin: %s", pluginUniqueIdentifier)
		return "", errors.New("no healthy nodes available")
	}

	// 根据策略选择节点
	log.Info("Load balancing strategy: %s", dlb.strategy)
	switch dlb.strategy {
	case app.LoadBalancingRoundRobin:
		selected := dlb.selectRoundRobin(filteredNodes)
		log.Info("Selected node using round-robin: %s", selected)
		return selected, nil
	case app.LoadBalancingLeastConnections:
		selected := dlb.selectLeastConnections(filteredNodes)
		log.Info("Selected node using least connections: %s", selected)
		return selected, nil
	case app.LoadBalancingRandom:
		selected := dlb.selectRandom(filteredNodes)
		log.Info("Selected node using random: %s", selected)
		return selected, nil
	case app.LoadBalancingWeightedRandom:
		selected := dlb.selectWeightedRandom(filteredNodes)
		log.Info("Selected node using weighted random: %s", selected)
		return selected, nil
	default:
		selected := dlb.selectRoundRobin(filteredNodes)
		log.Info("Selected node using default round-robin: %s", selected)
		return selected, nil
	}
}

// selectRoundRobin 轮询选择节点
func (dlb *DistributedLoadBalancer) selectRoundRobin(nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}
	
	index := atomic.AddInt64(&dlb.roundRobinCounter, 1) % int64(len(nodes))
	return nodes[index]
}

// selectLeastConnections 选择连接数最少的节点
func (dlb *DistributedLoadBalancer) selectLeastConnections(nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}

	var bestNode string
	minConnections := int64(^uint64(0) >> 1) // 最大int64值

	for _, nodeID := range nodes {
		if info := dlb.getNodeInfo(nodeID); info != nil {
			if info.ActiveConnections < minConnections {
				minConnections = info.ActiveConnections
				bestNode = nodeID
			}
		}
	}

	if bestNode == "" {
		return nodes[0] // 回退到第一个节点
	}

	return bestNode
}

// selectRandom 随机选择节点
func (dlb *DistributedLoadBalancer) selectRandom(nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}
	
	index := rand.Intn(len(nodes))
	return nodes[index]
}

// selectWeightedRandom 加权随机选择节点（基于响应时间和负载）
func (dlb *DistributedLoadBalancer) selectWeightedRandom(nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}

	// 计算权重（响应时间越短、负载越低，权重越高）
	weights := make([]float64, len(nodes))
	totalWeight := 0.0

	for i, nodeID := range nodes {
		if info := dlb.getNodeInfo(nodeID); info != nil {
			// 权重 = 1 / (响应时间(ms) + 负载评分*1000 + 1)
			responseTimeMs := float64(info.ResponseTime.Milliseconds())
			weight := 1.0 / (responseTimeMs + info.LoadScore*1000 + 1)
			weights[i] = weight
			totalWeight += weight
		} else {
			weights[i] = 0.1 // 默认较低权重
			totalWeight += 0.1
		}
	}

	// 加权随机选择
	r := rand.Float64() * totalWeight
	sum := 0.0
	for i, weight := range weights {
		sum += weight
		if r <= sum {
			return nodes[i]
		}
	}

	return nodes[len(nodes)-1] // 回退到最后一个节点
}

// getNodeInfo 获取节点信息
func (dlb *DistributedLoadBalancer) getNodeInfo(nodeID string) *NodeInfo {
	if info, ok := dlb.nodes.Load(nodeID); ok {
		return info.(*NodeInfo)
	}
	return nil
}

// UpdateNodeHealth 更新节点健康状态
func (dlb *DistributedLoadBalancer) UpdateNodeHealth(nodeID string, isHealthy bool) {
	now := time.Now()
	
	if existingInfo, ok := dlb.nodes.Load(nodeID); ok {
		info := existingInfo.(*NodeInfo)
		info.IsHealthy = isHealthy
		info.LastSeenAt = now
		
		if isHealthy {
			info.FailureCount = 0
		} else {
			info.FailureCount++
		}
		
		dlb.nodes.Store(nodeID, info)
	} else {
		// 创建新节点信息
		info := &NodeInfo{
			ID:               nodeID,
			LastSeenAt:       now,
			IsHealthy:        isHealthy,
			LoadScore:        0.5, // 默认中等负载
			ActiveConnections: 0,
			Metadata:         make(map[string]any),
			ResponseTime:     100 * time.Millisecond, // 默认响应时间
			FailureCount:     0,
		}
		
		if !isHealthy {
			info.FailureCount = 1
		}
		
		dlb.nodes.Store(nodeID, info)
	}
}

// GetAvailableNodes 获取所有可用节点
func (dlb *DistributedLoadBalancer) GetAvailableNodes() []string {
	var availableNodes []string
	
	dlb.nodes.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		info := value.(*NodeInfo)
		
		if info.IsHealthy && time.Since(info.LastSeenAt) < dlb.config.NodeHealthCheckInterval*3 {
			availableNodes = append(availableNodes, nodeID)
		}
		return true
	})
	
	return availableNodes
}

// AddNode 添加节点
func (dlb *DistributedLoadBalancer) AddNode(nodeID string, metadata map[string]any) {
	info := &NodeInfo{
		ID:               nodeID,
		LastSeenAt:       time.Now(),
		IsHealthy:        true,
		LoadScore:        0.5,
		ActiveConnections: 0,
		Metadata:         metadata,
		ResponseTime:     100 * time.Millisecond,
		FailureCount:     0,
	}
	
	dlb.nodes.Store(nodeID, info)
	log.Info("Added node to load balancer: %s", nodeID)
}

// RemoveNode 移除节点
func (dlb *DistributedLoadBalancer) RemoveNode(nodeID string) {
	dlb.nodes.Delete(nodeID)
	log.Info("Removed node from load balancer: %s", nodeID)
}

// IncrementActiveConnections 增加活跃连接数
func (dlb *DistributedLoadBalancer) IncrementActiveConnections(nodeID string) {
	if info := dlb.getNodeInfo(nodeID); info != nil {
		atomic.AddInt64(&info.ActiveConnections, 1)
	}
}

// DecrementActiveConnections 减少活跃连接数
func (dlb *DistributedLoadBalancer) DecrementActiveConnections(nodeID string) {
	if info := dlb.getNodeInfo(nodeID); info != nil {
		atomic.AddInt64(&info.ActiveConnections, -1)
	}
}

// UpdateResponseTime 更新节点响应时间
func (dlb *DistributedLoadBalancer) UpdateResponseTime(nodeID string, responseTime time.Duration) {
	if info := dlb.getNodeInfo(nodeID); info != nil {
		// 使用指数移动平均计算响应时间
		alpha := 0.1 // 平滑因子
		currentTime := float64(info.ResponseTime.Nanoseconds())
		newTime := float64(responseTime.Nanoseconds())
		info.ResponseTime = time.Duration(alpha*newTime + (1-alpha)*currentTime)
	}
}

// CleanupStaleNodes 清理过期节点
func (dlb *DistributedLoadBalancer) CleanupStaleNodes() {
	staleThreshold := dlb.config.NodeHealthCheckInterval * 5
	
	dlb.nodes.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		info := value.(*NodeInfo)
		
		if time.Since(info.LastSeenAt) > staleThreshold {
			dlb.RemoveNode(nodeID)
		}
		return true
	})
}