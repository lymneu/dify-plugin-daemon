package cluster

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/langgenius/dify-plugin-daemon/internal/utils/cache"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/log"
	"github.com/langgenius/dify-plugin-daemon/pkg/entities/plugin_entities"
)

const (
	// 分布式插件状态存储键前缀
	DISTRIBUTED_PLUGIN_STATE_PREFIX = "distributed:plugin"
	PLUGIN_NODE_MAPPING_PREFIX       = "plugin:node:mapping"
	PLUGIN_STATE_TTL                 = time.Minute * 30 // 插件状态TTL
)

// DistributedPluginState 分布式插件状态
type DistributedPluginState struct {
	PluginID               string                              `json:"plugin_id"`
	PluginUniqueIdentifier string                              `json:"plugin_unique_identifier"`
	NodeID                 string                              `json:"node_id"`
	State                  plugin_entities.PluginRuntimeState `json:"state"`
	LastScheduledAt        time.Time                           `json:"last_scheduled_at"`
	CreatedAt              time.Time                           `json:"created_at"`
	UpdatedAt              time.Time                           `json:"updated_at"`
	ExpiresAt              time.Time                           `json:"expires_at"`
	RestartCount           int                                 `json:"restart_count"`
	Metadata               map[string]any                      `json:"metadata"`
}

// DistributedPluginStateManager 分布式插件状态管理器
type DistributedPluginStateManager struct {
	nodeID string
	// 本地缓存提高性能
	localCache map[string]*DistributedPluginState
	cacheLock  sync.RWMutex
}

// NewDistributedPluginStateManager 创建分布式插件状态管理器
func NewDistributedPluginStateManager(nodeID string) *DistributedPluginStateManager {
	return &DistributedPluginStateManager{
		nodeID:     nodeID,
		localCache: make(map[string]*DistributedPluginState),
	}
}

// pluginStateKey 生成插件状态Redis键
func (dpsm *DistributedPluginStateManager) pluginStateKey(pluginID string) string {
	return fmt.Sprintf("%s:%s", DISTRIBUTED_PLUGIN_STATE_PREFIX, pluginID)
}

// nodePluginMappingKey 生成节点插件映射Redis键
func (dpsm *DistributedPluginStateManager) nodePluginMappingKey(nodeID string) string {
	return fmt.Sprintf("%s:%s", PLUGIN_NODE_MAPPING_PREFIX, nodeID)
}

// RegisterPlugin 注册插件到分布式状态管理
func (dpsm *DistributedPluginStateManager) RegisterPlugin(
	pluginID string,
	uniqueIdentifier string,
	state plugin_entities.PluginRuntimeState,
) error {
	log.Info("Registering plugin to distributed state: pluginID=%s, uniqueIdentifier=%s, nodeID=%s", 
		pluginID, uniqueIdentifier, dpsm.nodeID)
	
	distributedState := &DistributedPluginState{
		PluginID:               pluginID,
		PluginUniqueIdentifier: uniqueIdentifier,
		NodeID:                 dpsm.nodeID,
		State:                  state,
		LastScheduledAt:        time.Now(),
		CreatedAt:              time.Now(),
		UpdatedAt:              time.Now(),
		ExpiresAt:              time.Time{}, // 不再设置过期时间
		RestartCount:           0,
		Metadata:               make(map[string]any),
	}

	// 序列化状态
	stateData, err := json.Marshal(distributedState)
	if err != nil {
		return fmt.Errorf("failed to marshal plugin state: %w", err)
	}

	// 直接使用cache包的API，避免事务复杂性
	// 1. 存储插件状态（无TTL，插件运行期间持续有效）
	stateKey := dpsm.pluginStateKey(pluginID)
	log.Info("Storing plugin state with key: %s", stateKey)
	if err := cache.Store(stateKey, string(stateData), 0); err != nil {
		log.Error("Failed to store plugin state %s: %v", pluginID, err)
		return fmt.Errorf("failed to store plugin state: %w", err)
	}
	log.Info("Successfully stored plugin state for %s", pluginID)
	
	// 2. 更新节点插件映射
	nodeMappingKey := dpsm.nodePluginMappingKey(dpsm.nodeID)
	log.Info("Updating node mapping with key: %s, pluginID: %s, uniqueIdentifier: %s", 
		nodeMappingKey, pluginID, uniqueIdentifier)
	// 确保unique identifier以JSON格式存储，与GetMap的反序列化保持一致
	uniqueIdentifierJson, err := json.Marshal(uniqueIdentifier)
	if err != nil {
		return fmt.Errorf("failed to marshal unique identifier: %w", err)
	}
	if err := cache.SetMapOneField(nodeMappingKey, pluginID, string(uniqueIdentifierJson)); err != nil {
		log.Error("Failed to update node mapping %s: %v", nodeMappingKey, err)
		return fmt.Errorf("failed to update node mapping: %w", err)
	}
	log.Info("Successfully updated node mapping for plugin %s", pluginID)
	
	// 3. 设置节点映射永不过期（插件运行期间持续有效）
	// 注释掉TTL设置，避免运行中插件被自动清理
	// if _, err := cache.Expire(nodeMappingKey, PLUGIN_STATE_TTL); err != nil {
	//	log.Warn("Failed to set TTL for node mapping %s: %v", nodeMappingKey, err)
	// } else {
	//	log.Info("Successfully set TTL for node mapping %s", nodeMappingKey)
	// }
	log.Info("Node mapping stored without TTL for persistent availability")
	
	// 4. 验证数据是否正确存储 - 直接使用存储的键名，不再添加前缀
	// 因为cache.GetMap会自动添加plugin_daemon:前缀，所以我们需要去掉PLUGIN_NODE_MAPPING_PREFIX
	verifyKey := fmt.Sprintf("%s:%s", PLUGIN_NODE_MAPPING_PREFIX, dpsm.nodeID)
	if pluginMap, err := cache.GetMap[string](verifyKey); err != nil {
		log.Warn("Failed to verify node mapping: %v", err)
	} else {
		log.Info("Verification: Node %s now has %d plugins: %v", dpsm.nodeID, len(pluginMap), pluginMap)
	}
	
	log.Info("Successfully registered plugin %s to distributed state", pluginID)
	return nil
}

// UpdatePluginState 更新插件状态
func (dpsm *DistributedPluginStateManager) UpdatePluginState(
	pluginID string,
	state plugin_entities.PluginRuntimeState,
) error {
	// 先获取现有状态
	existingState, err := dpsm.GetPluginState(pluginID)
	if err != nil {
		return fmt.Errorf("failed to get existing plugin state: %w", err)
	}

	// 更新状态
	existingState.State = state
	existingState.LastScheduledAt = time.Now()
	existingState.UpdatedAt = time.Now()
	// 不再更新ExpiresAt，因为插件状态不再过期

	// 序列化
	stateData, err := json.Marshal(existingState)
	if err != nil {
		return fmt.Errorf("failed to marshal updated plugin state: %w", err)
	}

	// 存储到Redis（无TTL）
	stateKey := dpsm.pluginStateKey(pluginID)
	if err := cache.Store(stateKey, string(stateData), 0); err != nil {
		return fmt.Errorf("failed to store updated plugin state: %w", err)
	}

	// 更新本地缓存
	dpsm.cacheLock.Lock()
	dpsm.localCache[pluginID] = existingState
	dpsm.cacheLock.Unlock()

	return nil
}

// GetPluginState 获取插件状态
func (dpsm *DistributedPluginStateManager) GetPluginState(pluginID string) (*DistributedPluginState, error) {
	// 先检查本地缓存
	dpsm.cacheLock.RLock()
	if state, exists := dpsm.localCache[pluginID]; exists {
		dpsm.cacheLock.RUnlock()
		return state, nil
	}
	dpsm.cacheLock.RUnlock()

	// 从Redis获取
	stateKey := dpsm.pluginStateKey(pluginID)
	stateDataStr, err := cache.GetString(stateKey)
	if err != nil {
		if err == cache.ErrNotFound {
			return nil, fmt.Errorf("plugin state not found: %s", pluginID)
		}
		return nil, fmt.Errorf("failed to get plugin state: %w", err)
	}

	var state DistributedPluginState
	if err := json.Unmarshal([]byte(stateDataStr), &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal plugin state: %w", err)
	}

	// 不再检查过期，因为插件状态不再过期

	// 更新本地缓存
	dpsm.cacheLock.Lock()
	dpsm.localCache[pluginID] = &state
	dpsm.cacheLock.Unlock()

	return &state, nil
}

// UnregisterPlugin 注销插件
func (dpsm *DistributedPluginStateManager) UnregisterPlugin(pluginID string) error {
	// 删除插件状态
	stateKey := dpsm.pluginStateKey(pluginID)
	if _, err := cache.Del(stateKey); err != nil {
		log.Error("failed to delete plugin state: %v", err)
	}

	// 从节点映射中删除
	nodeMappingKey := dpsm.nodePluginMappingKey(dpsm.nodeID)
	if err := cache.DelMapField(nodeMappingKey, pluginID); err != nil {
		log.Error("failed to remove plugin from node mapping: %v", err)
	}

	// 从本地缓存中删除
	dpsm.cacheLock.Lock()
	delete(dpsm.localCache, pluginID)
	dpsm.cacheLock.Unlock()

	return nil
}

// GetPluginsByNode 获取节点上的所有插件
func (dpsm *DistributedPluginStateManager) GetPluginsByNode(nodeID string) ([]string, error) {
	// 直接使用PLUGIN_NODE_MAPPING_PREFIX:nodeID格式，避免双重前缀
	nodeMappingKey := fmt.Sprintf("%s:%s", PLUGIN_NODE_MAPPING_PREFIX, nodeID)
	pluginMap, err := cache.GetMap[string](nodeMappingKey)
	if err != nil {
		if err == cache.ErrNotFound {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to get plugins by node: %w", err)
	}

	var pluginIDs []string
	for pluginID := range pluginMap {
		pluginIDs = append(pluginIDs, pluginID)
	}
	return pluginIDs, nil
}

// FindAvailableNodesForPlugin 查找可用于运行插件的节点
func (dpsm *DistributedPluginStateManager) FindAvailableNodesForPlugin(pluginUniqueIdentifier string) ([]string, error) {
	log.Info("Finding available nodes for plugin: %s", pluginUniqueIdentifier)
	// 扫描所有节点映射，找到有该插件的节点（需要包含cache包的前缀）
	pattern := fmt.Sprintf("plugin_daemon:%s:*", PLUGIN_NODE_MAPPING_PREFIX)
	log.Info("Scanning with pattern: %s", pattern)
	var availableNodes []string

	err := cache.ScanKeysAsync(pattern, func(keys []string) error {
		log.Info("Found %d keys matching pattern", len(keys))
		for _, key := range keys {
			// 从完整键名中提取nodeID（移除plugin_daemon:和PLUGIN_NODE_MAPPING_PREFIX前缀）
			prefixToRemove := fmt.Sprintf("plugin_daemon:%s:", PLUGIN_NODE_MAPPING_PREFIX)
			nodeID := key[len(prefixToRemove):]
			log.Info("Checking node: %s (key: %s)", nodeID, key)
			
			// 获取该节点的插件映射 - 直接使用PLUGIN_NODE_MAPPING_PREFIX:nodeID格式
			// 避免双重前缀，因为cache.GetMap会自动添加plugin_daemon:前缀
			nodeMappingKey := fmt.Sprintf("%s:%s", PLUGIN_NODE_MAPPING_PREFIX, nodeID)
			pluginMap, err := cache.GetMap[string](nodeMappingKey)
			if err != nil {
				log.Warn("Failed to get plugin map for node %s: %v", nodeID, err)
				continue
			}
			log.Info("Node %s has %d plugins", nodeID, len(pluginMap))

			// 检查是否有目标插件
			for pluginID, identifier := range pluginMap {
				log.Info("  Plugin: %s -> %s", pluginID, identifier)
				// 注意：identifier是JSON字符串，需要解析后再比较
				parsedIdentifier := identifier
				// 尝试解析JSON字符串
				if err := json.Unmarshal([]byte(identifier), &parsedIdentifier); err == nil {
					log.Info("  Parsed identifier: %s", parsedIdentifier)
					if parsedIdentifier == pluginUniqueIdentifier {
						log.Info("Found matching plugin on node: %s", nodeID)
						availableNodes = append(availableNodes, nodeID)
						break
					}
				} else {
					// 如果解析失败，直接比较原始值
					log.Info("  Failed to parse identifier as JSON, comparing raw values")
					if identifier == pluginUniqueIdentifier {
						log.Info("Found matching plugin on node: %s", nodeID)
						availableNodes = append(availableNodes, nodeID)
						break
					}
				}
				
				// 特殊处理：检查是否是包含节点ID的pluginID格式
				// 在RegisterPlugin中，我们使用了pluginIDWithNode := fmt.Sprintf("%s@%s", identity.String(), c.id)
				// 所以这里也要检查这种格式
				if strings.Contains(pluginID, "@") {
					parts := strings.Split(pluginID, "@")
					if len(parts) == 2 && parts[0] == pluginUniqueIdentifier {
						log.Info("Found matching plugin with node info on node: %s", nodeID)
						availableNodes = append(availableNodes, nodeID)
						break
					}
				}
			}
		}
		return nil
	})

	log.Info("Found %d available nodes for plugin %s: %v", len(availableNodes), pluginUniqueIdentifier, availableNodes)
	return availableNodes, err
}

// CleanupExpiredPluginStates 清理过期的插件状态
// 注意：由于插件状态不再过期，这个方法不再有实际作用
func (dpsm *DistributedPluginStateManager) CleanupExpiredPluginStates() error {
	// 不再有过期状态需要清理
	log.Info("No expired plugin states to cleanup (TTL disabled)")
	return nil
}

// GetAllActivePlugins 获取所有活跃插件状态
func (dpsm *DistributedPluginStateManager) GetAllActivePlugins() ([]*DistributedPluginState, error) {
	pattern := fmt.Sprintf("%s:*", DISTRIBUTED_PLUGIN_STATE_PREFIX)
	var activePlugins []*DistributedPluginState

	err := cache.ScanKeysAsync(pattern, func(keys []string) error {
		for _, key := range keys {
			pluginID := key[len(DISTRIBUTED_PLUGIN_STATE_PREFIX)+1:]
			state, err := dpsm.GetPluginState(pluginID)
			if err != nil {
				continue // 跳过获取失败或过期的插件
			}
			activePlugins = append(activePlugins, state)
		}
		return nil
	})

	return activePlugins, err
}