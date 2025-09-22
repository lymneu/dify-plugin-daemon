package app

import (
	"fmt"
	"time"
)

// HorizontalScalingConfig 水平扩展配置
type HorizontalScalingConfig struct {
	// 是否启用分布式模式
	EnableDistributedMode bool `envconfig:"ENABLE_DISTRIBUTED_MODE" default:"false"`
	
	// 是否启用会话本地缓存
	EnableSessionLocalCache bool `envconfig:"ENABLE_SESSION_LOCAL_CACHE" default:"true"`
	
	// 分布式会话TTL
	DistributedSessionTTL time.Duration `envconfig:"DISTRIBUTED_SESSION_TTL" default:"2h"`
	
	// 分布式插件状态TTL
	DistributedPluginStateTTL time.Duration `envconfig:"DISTRIBUTED_PLUGIN_STATE_TTL" default:"30m"`
	
	// 负载均衡策略
	LoadBalancingStrategy string `envconfig:"LOAD_BALANCING_STRATEGY" default:"round_robin"` // round_robin, least_connections, random
	
	// 节点健康检查间隔
	NodeHealthCheckInterval time.Duration `envconfig:"NODE_HEALTH_CHECK_INTERVAL" default:"30s"`
	
	// 插件状态同步间隔
	PluginStateSyncInterval time.Duration `envconfig:"PLUGIN_STATE_SYNC_INTERVAL" default:"10s"`
	
	// 自动清理过期状态的间隔
	AutoCleanupInterval time.Duration `envconfig:"AUTO_CLEANUP_INTERVAL" default:"5m"`
	
	// 最大重定向次数（仅在混合模式下使用）
	MaxRedirectAttempts int `envconfig:"MAX_REDIRECT_ATTEMPTS" default:"3"`
	
	// 是否禁用重定向机制（完全分布式模式）
	DisableRedirection bool `envconfig:"DISABLE_REDIRECTION" default:"true"`
	
	// 集群发现方式 
	ClusterDiscoveryMode string `envconfig:"CLUSTER_DISCOVERY_MODE" default:"redis"` // redis, consul, etcd
	
	// 节点注册TTL
	NodeRegistrationTTL time.Duration `envconfig:"NODE_REGISTRATION_TTL" default:"1m"`
	
	// 故障转移超时
	FailoverTimeout time.Duration `envconfig:"FAILOVER_TIMEOUT" default:"30s"`
}

// LoadBalancingStrategy 负载均衡策略枚举
const (
	LoadBalancingRoundRobin      = "round_robin"
	LoadBalancingLeastConnections = "least_connections" 
	LoadBalancingRandom          = "random"
	LoadBalancingWeightedRandom  = "weighted_random"
)

// ClusterDiscoveryMode 集群发现模式枚举
const (
	ClusterDiscoveryRedis  = "redis"
	ClusterDiscoveryConsul = "consul"
	ClusterDiscoveryEtcd   = "etcd"
)

// GetDefaultHorizontalScalingConfig 获取默认水平扩展配置
func GetDefaultHorizontalScalingConfig() *HorizontalScalingConfig {
	return &HorizontalScalingConfig{
		EnableDistributedMode:       false,
		EnableSessionLocalCache:     true,
		DistributedSessionTTL:       2 * time.Hour,
		DistributedPluginStateTTL:   30 * time.Minute,
		LoadBalancingStrategy:       LoadBalancingRoundRobin,
		NodeHealthCheckInterval:     30 * time.Second,
		PluginStateSyncInterval:     10 * time.Second,
		AutoCleanupInterval:         5 * time.Minute,
		MaxRedirectAttempts:         3,
		DisableRedirection:          true,
		ClusterDiscoveryMode:        ClusterDiscoveryRedis,
		NodeRegistrationTTL:         1 * time.Minute,
		FailoverTimeout:             30 * time.Second,
	}
}

// Validate 验证配置
func (hsc *HorizontalScalingConfig) Validate() error {
	// 验证负载均衡策略
	validStrategies := map[string]bool{
		LoadBalancingRoundRobin:      true,
		LoadBalancingLeastConnections: true,
		LoadBalancingRandom:          true,
		LoadBalancingWeightedRandom:  true,
	}
	
	if !validStrategies[hsc.LoadBalancingStrategy] {
		return fmt.Errorf("invalid load balancing strategy: %s", hsc.LoadBalancingStrategy)
	}
	
	// 验证集群发现模式
	validModes := map[string]bool{
		ClusterDiscoveryRedis:  true,
		ClusterDiscoveryConsul: true,
		ClusterDiscoveryEtcd:   true,
	}
	
	if !validModes[hsc.ClusterDiscoveryMode] {
		return fmt.Errorf("invalid cluster discovery mode: %s", hsc.ClusterDiscoveryMode)
	}
	
	// 验证时间配置
	if hsc.DistributedSessionTTL <= 0 {
		return fmt.Errorf("distributed session TTL must be positive")
	}
	
	if hsc.DistributedPluginStateTTL <= 0 {
		return fmt.Errorf("distributed plugin state TTL must be positive")
	}
	
	if hsc.NodeHealthCheckInterval <= 0 {
		return fmt.Errorf("node health check interval must be positive")
	}
	
	return nil
}