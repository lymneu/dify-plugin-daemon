package cluster

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/langgenius/dify-plugin-daemon/internal/core/plugin_manager"
	"github.com/langgenius/dify-plugin-daemon/internal/types/app"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/log"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/mapping"
	"github.com/langgenius/dify-plugin-daemon/pkg/entities/plugin_entities"
)

type Cluster struct {
	// id is the unique id of the cluster
	id string

	// i_am_master is the flag to indicate whether the current node is the master node
	iAmMaster bool

	// main http port of the current node
	port uint16

	// plugins stores all the plugin life time of the current node
	// DEPRECATED: 这个字段将被 distributedPluginStateManager 替代
	plugins    mapping.Map[string, *pluginLifeTime]
	pluginLock sync.RWMutex

	// 新的分布式插件状态管理器
	distributedPluginStateManager *DistributedPluginStateManager

	// 负载均衡器
	loadBalancer LoadBalancer

	manager *plugin_manager.PluginManager

	// 配置信息，用于初始化负载均衡器
	config *app.Config

	// nodes stores all the nodes of the cluster
	nodes mapping.Map[string, node]

	// signals for waiting for the cluster to stop
	stopChan chan bool
	stopped  int32

	isInAutoGcNodes   int32
	isInAutoGcPlugins int32

	// channels to notify cluster event
	notifyBecomeMasterChan            chan bool
	notifyMasterGcChan                chan bool
	notifyMasterGcCompletedChan       chan bool
	notifyVotingChan                  chan bool
	notifyVotingCompletedChan         chan bool
	notifyPluginScheduleChan          chan bool
	notifyPluginScheduleCompletedChan chan bool
	notifyNodeUpdateChan              chan bool
	notifyNodeUpdateCompletedChan     chan bool
	notifyClusterStoppedChan          chan bool

	showLog bool

	masterGcInterval              time.Duration
	masterLockingInterval         time.Duration
	masterLockExpiredTime         time.Duration
	nodeVoteInterval              time.Duration
	nodeDisconnectedTimeout       time.Duration
	updateNodeStatusInterval      time.Duration
	pluginSchedulerInterval       time.Duration
	pluginSchedulerTickerInterval time.Duration
	pluginDeactivatedTimeout      time.Duration
}

func NewCluster(config *app.Config, plugin_manager *plugin_manager.PluginManager) *Cluster {
	clusterID := uuid.New().String()
	return &Cluster{
		id:                            clusterID,
		port:                          uint16(config.ServerPort),
		stopChan:                      make(chan bool),
		showLog:                       config.DisplayClusterLog,
		masterGcInterval:              MASTER_GC_INTERVAL,
		masterLockingInterval:         MASTER_LOCKING_INTERVAL,
		masterLockExpiredTime:         MASTER_LOCK_EXPIRED_TIME,
		nodeVoteInterval:              NODE_VOTE_INTERVAL,
		nodeDisconnectedTimeout:       NODE_DISCONNECTED_TIMEOUT,
		updateNodeStatusInterval:      UPDATE_NODE_STATUS_INTERVAL,
		pluginSchedulerInterval:       PLUGIN_SCHEDULER_INTERVAL,
		pluginSchedulerTickerInterval: PLUGIN_SCHEDULER_TICKER_INTERVAL,
		pluginDeactivatedTimeout:      PLUGIN_DEACTIVATED_TIMEOUT,

		manager: plugin_manager,
		config:  config, // 保存配置引用

		// 初始化分布式插件状态管理器
		distributedPluginStateManager: NewDistributedPluginStateManager(clusterID),

		// 初始化负载均衡器
		loadBalancer: nil, // 将在Launch时初始化

		notifyBecomeMasterChan:            make(chan bool),
		notifyMasterGcChan:                make(chan bool),
		notifyMasterGcCompletedChan:       make(chan bool),
		notifyVotingChan:                  make(chan bool),
		notifyVotingCompletedChan:         make(chan bool),
		notifyPluginScheduleChan:          make(chan bool),
		notifyPluginScheduleCompletedChan: make(chan bool),
		notifyNodeUpdateChan:              make(chan bool),
		notifyNodeUpdateCompletedChan:     make(chan bool),
		notifyClusterStoppedChan:          make(chan bool),
	}
}

func (c *Cluster) Launch() {
	// 初始化负载均衡器（如果启用分布式模式）
	if c.config != nil && c.config.HorizontalScaling != nil {
		c.InitializeLoadBalancer(c.config.HorizontalScaling)
	}
	
	go c.clusterLifetime()
}

func (c *Cluster) Close() error {
	if atomic.CompareAndSwapInt32(&c.stopped, 0, 1) {
		close(c.stopChan)
	}

	return nil
}

func (c *Cluster) ID() string {
	return c.id
}

// trigger for master event
func (c *Cluster) notifyBecomeMaster() {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return
	}

	select {
	case c.notifyBecomeMasterChan <- true:
	default:
	}
}

// receive the master event
func (c *Cluster) NotifyBecomeMaster() <-chan bool {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return nil
	}
	return c.notifyBecomeMasterChan
}

// trigger for master gc event
func (c *Cluster) notifyMasterGC() {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return
	}

	select {
	case c.notifyMasterGcChan <- true:
	default:
	}
}

// trigger for master gc completed event
func (c *Cluster) notifyMasterGCCompleted() {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return
	}

	select {
	case c.notifyMasterGcCompletedChan <- true:
	default:
	}
}

// trigger for voting event
func (c *Cluster) notifyVoting() {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return
	}

	select {
	case c.notifyVotingChan <- true:
	default:
	}
}

// trigger for voting completed event
func (c *Cluster) notifyVotingCompleted() {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return
	}

	select {
	case c.notifyVotingCompletedChan <- true:
	default:
	}
}

// trigger for plugin schedule event
func (c *Cluster) notifyPluginSchedule() {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return
	}

	select {
	case c.notifyPluginScheduleChan <- true:
	default:
	}
}

// trigger for plugin schedule completed event
func (c *Cluster) notifyPluginScheduleCompleted() {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return
	}

	select {
	case c.notifyPluginScheduleCompletedChan <- true:
	default:
	}
}

// trigger for node update event
func (c *Cluster) notifyNodeUpdate() {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return
	}

	select {
	case c.notifyNodeUpdateChan <- true:
	default:
	}
}

// trigger for node update completed event
func (c *Cluster) notifyNodeUpdateCompleted() {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return
	}

	select {
	case c.notifyNodeUpdateCompletedChan <- true:
	default:
	}
}

// trigger for cluster stopped event
func (c *Cluster) notifyClusterStopped() {
	select {
	case c.notifyClusterStoppedChan <- true:
	default:
	}
}

// receive the master gc event
func (c *Cluster) NotifyMasterGC() <-chan bool {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return nil
	}
	return c.notifyMasterGcChan
}

// receive the master gc completed event
func (c *Cluster) NotifyMasterGCCompleted() <-chan bool {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return nil
	}
	return c.notifyMasterGcCompletedChan
}

// receive the voting event
func (c *Cluster) NotifyVoting() <-chan bool {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return nil
	}
	return c.notifyVotingChan
}

// receive the voting completed event
func (c *Cluster) NotifyVotingCompleted() <-chan bool {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return nil
	}
	return c.notifyVotingCompletedChan
}

// receive the plugin schedule event
func (c *Cluster) NotifyPluginSchedule() <-chan bool {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return nil
	}
	return c.notifyPluginScheduleChan
}

// receive the plugin schedule completed event
func (c *Cluster) NotifyPluginScheduleCompleted() <-chan bool {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return nil
	}
	return c.notifyPluginScheduleCompletedChan
}

// receive the node update event
func (c *Cluster) NotifyNodeUpdate() <-chan bool {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return nil
	}
	return c.notifyNodeUpdateChan
}

// receive the node update completed event
func (c *Cluster) NotifyNodeUpdateCompleted() <-chan bool {
	if atomic.LoadInt32(&c.stopped) == 1 {
		return nil
	}
	return c.notifyNodeUpdateCompletedChan
}

// receive the cluster stopped event
func (c *Cluster) NotifyClusterStopped() <-chan bool {
	return c.notifyClusterStoppedChan
}

// === 新的分布式插件管理方法 ===

// RegisterDistributedPlugin 注册插件到分布式系统
func (c *Cluster) RegisterDistributedPlugin(
	pluginID string,
	uniqueIdentifier string,
	state plugin_entities.PluginRuntimeState,
) error {
	return c.distributedPluginStateManager.RegisterPlugin(pluginID, uniqueIdentifier, state)
}

// UpdateDistributedPluginState 更新分布式插件状态
func (c *Cluster) UpdateDistributedPluginState(
	pluginID string,
	state plugin_entities.PluginRuntimeState,
) error {
	return c.distributedPluginStateManager.UpdatePluginState(pluginID, state)
}

// GetDistributedPluginState 获取分布式插件状态
func (c *Cluster) GetDistributedPluginState(pluginID string) (*DistributedPluginState, error) {
	return c.distributedPluginStateManager.GetPluginState(pluginID)
}

// UnregisterDistributedPlugin 注销分布式插件
func (c *Cluster) UnregisterDistributedPlugin(pluginID string) error {
	return c.distributedPluginStateManager.UnregisterPlugin(pluginID)
}

// IsPluginOnCurrentNode 检查插件是否在当前节点
func (c *Cluster) IsPluginOnCurrentNode(uniqueIdentifier plugin_entities.PluginUniqueIdentifier) (bool, error) {
	// 先检查旧的本地plugins map
	c.pluginLock.RLock()
	_, existsLocally := c.plugins.Load(uniqueIdentifier.String())
	c.pluginLock.RUnlock()
	
	if existsLocally {
		return true, nil
	}

	// 检查分布式状态
	pluginIDs, err := c.distributedPluginStateManager.GetPluginsByNode(c.id)
	if err != nil {
		return false, err
	}

	for _, pluginID := range pluginIDs {
		state, err := c.distributedPluginStateManager.GetPluginState(pluginID)
		if err != nil {
			continue
		}
		if state.PluginUniqueIdentifier == uniqueIdentifier.String() {
			return true, nil
		}
	}

	return false, nil
}

// FetchPluginAvailableNodesById 获取可用于运行指定插件的节点列表
func (c *Cluster) FetchPluginAvailableNodesById(pluginUniqueIdentifier string) ([]string, error) {
	return c.distributedPluginStateManager.FindAvailableNodesForPlugin(pluginUniqueIdentifier)
}

// 兼容性方法：同时支持旧的和新的插件管理方式

// RegisterPlugin 兼容性插件注册方法
func (c *Cluster) RegisterPlugin(lifetime plugin_entities.PluginLifetime) error {
	identity, err := lifetime.Identity()
	if err != nil {
		return err
	}

	if c.showLog {
		log.Info("registering plugin %s", identity.String())
	}

	// 先使用旧的方式注册（保持兼容性）
	if c.plugins.Exists(identity.String()) {
		return errors.New("plugin has been registered")
	}

	l := &pluginLifeTime{
		lifetime: lifetime,
	}

	lifetime.OnStop(func() {
		c.pluginLock.Lock()
		c.plugins.Delete(identity.String())
		// 同时从分布式状态中移除（使用包含节点ID的pluginID）
		pluginIDWithNode := fmt.Sprintf("%s@%s", identity.String(), c.id)
		c.UnregisterDistributedPlugin(pluginIDWithNode)
		c.pluginLock.Unlock()
	})

	c.pluginLock.Lock()
	if !lifetime.Stopped() {
		c.plugins.Store(identity.String(), l)
		// 同时注册到分布式状态
		log.Info("Registering plugin to distributed state: %s", identity.String())
		// 使用包含节点ID的pluginID以确保唯一性，而unique identifier保持原始值
		pluginIDWithNode := fmt.Sprintf("%s@%s", identity.String(), c.id)
		err := c.RegisterDistributedPlugin(
			pluginIDWithNode,    // pluginID：包含节点信息的唯一标识
			identity.String(),   // uniqueIdentifier：纯插件标识
			lifetime.RuntimeState(),
		)
		if err != nil {
			log.Error("Failed to register plugin to distributed state: %s, error: %v", identity.String(), err)
		} else {
			log.Info("Successfully registered plugin to distributed state: %s", identity.String())
		}
	}
	c.pluginLock.Unlock()

	if c.showLog {
		log.Info("start to schedule plugin %s", identity)
	}

	return nil
}

// GetLoadBalancer 获取负载均衡器
func (c *Cluster) GetLoadBalancer() LoadBalancer {
	return c.loadBalancer
}

// InitializeLoadBalancer 初始化负载均衡器
func (c *Cluster) InitializeLoadBalancer(config *app.HorizontalScalingConfig) {
	if config != nil && config.EnableDistributedMode {
		c.loadBalancer = NewDistributedLoadBalancer(config, c.distributedPluginStateManager)
		
		// 将当前节点添加到负载均衡器
		c.loadBalancer.AddNode(c.id, map[string]any{"port": c.port})
		c.loadBalancer.UpdateNodeHealth(c.id, true)
		
		log.Info("Initialized distributed load balancer with strategy: %s, added node: %s", config.LoadBalancingStrategy, c.id)
	}
}

// === 工具方法 ===

// getPluginStateKey 生成插件状态键
func (c *Cluster) getPluginStateKey(nodeId string, plugin_id string) string {
	return nodeId + ":" + plugin_id
}

// getScanPluginsByNodeKey 生成按节点扫描插件的键模式
func (c *Cluster) getScanPluginsByNodeKey(nodeId string) string {
	return nodeId + ":*"
}

// getScanPluginsByIdKey 生成按插件ID扫描的键模式
func (c *Cluster) getScanPluginsByIdKey(plugin_id string) string {
	return "*:" + plugin_id
}

// splitNodePluginJoin 分割节点-插件组合键
func (c *Cluster) splitNodePluginJoin(node_plugin_join string) (nodeId string, plugin_hashed_id string, err error) {
	split := strings.Split(node_plugin_join, ":")
	if len(split) != 2 {
		return "", "", errors.New("invalid node_plugin_join")
	}
	return split[0], split[1], nil
}
