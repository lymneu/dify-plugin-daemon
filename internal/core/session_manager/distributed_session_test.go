package session_manager

import (
	"testing"
	"time"

	"github.com/langgenius/dify-plugin-daemon/internal/core/plugin_daemon/access_types"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/cache"
	"github.com/langgenius/dify-plugin-daemon/pkg/entities/plugin_entities"
	"github.com/stretchr/testify/assert"
)

// setupTestRedis 设置测试Redis环境
func setupTestRedis(t *testing.T) {
	err := cache.InitRedisClient("localhost:6379", "", "", false, 1) // 使用DB 1进行测试
	if err != nil {
		t.Skipf("Redis not available for testing: %v", err)
	}
}

func TestDistributedSessionStorage_CreateAndGetSession(t *testing.T) {
	setupTestRedis(t)
	defer cache.Close()

	storage := NewRedisSessionStorage()

	// 创建测试会话
	session := &Session{
		ID:                     "test-session-123",
		TenantID:               "test-tenant",
		UserID:                 "test-user",
		PluginUniqueIdentifier: "test-plugin",
		ClusterID:              "test-cluster",
		InvokeFrom:             access_types.PLUGIN_ACCESS_TYPE_TOOL,
		Action:                 access_types.PLUGIN_ACCESS_ACTION_INVOKE_TOOL,
		Context:                map[string]any{"test": "value"},
	}

	// 测试创建会话
	err := storage.CreateSession(session)
	assert.NoError(t, err)

	// 测试获取会话
	retrievedSession, err := storage.GetSession("test-session-123")
	assert.NoError(t, err)
	assert.Equal(t, session.ID, retrievedSession.ID)
	assert.Equal(t, session.TenantID, retrievedSession.TenantID)
	assert.Equal(t, session.UserID, retrievedSession.UserID)

	// 清理测试数据
	storage.DeleteSession("test-session-123")
}

func TestDistributedSessionStorage_SessionExpiry(t *testing.T) {
	setupTestRedis(t)
	defer cache.Close()

	storage := &RedisSessionStorage{}

	// 创建一个短TTL的会话进行过期测试
	session := &Session{
		ID:       "test-expiry-session",
		TenantID: "test-tenant",
		UserID:   "test-user",
	}

	// 手动创建过期会话（设置过去的时间）
	expiredSession := &DistributedSession{
		ID:        session.ID,
		TenantID:  session.TenantID,
		UserID:    session.UserID,
		CreatedAt: time.Now().Add(-1 * time.Hour),
		ExpiresAt: time.Now().Add(-30 * time.Minute), // 已过期
	}

	// 直接存储过期会话
	sessionKey := storage.sessionKey("test-expiry-session")
	expiredData := `{"id":"test-expiry-session","tenant_id":"test-tenant","user_id":"test-user","expires_at":"2023-01-01T00:00:00Z"}`
	cache.Store(sessionKey, expiredData, time.Minute*5)

	// 尝试获取过期会话，应该返回错误
	_, err := storage.GetSession("test-expiry-session")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "session expired")
}

func TestDistributedSessionManager_Integration(t *testing.T) {
	setupTestRedis(t)
	defer cache.Close()

	// 初始化分布式会话管理器
	InitDistributedSessionManager(true)
	manager := GetSessionManager()

	// 创建测试会话
	session := &Session{
		ID:                     "test-manager-session",
		TenantID:               "test-tenant",
		UserID:                 "test-user",
		PluginUniqueIdentifier: "test-plugin",
		ClusterID:              "test-cluster",
		InvokeFrom:             access_types.PLUGIN_ACCESS_TYPE_TOOL,
		Action:                 access_types.PLUGIN_ACCESS_ACTION_INVOKE_TOOL,
	}

	// 测试创建会话
	err := manager.CreateSession(session)
	assert.NoError(t, err)

	// 测试获取会话（应该从本地缓存获取）
	retrievedSession, err := manager.GetSession("test-manager-session")
	assert.NoError(t, err)
	assert.Equal(t, session.ID, retrievedSession.ID)

	// 测试删除会话
	err = manager.DeleteSession("test-manager-session")
	assert.NoError(t, err)

	// 确认会话已删除
	_, err = manager.GetSession("test-manager-session")
	assert.Error(t, err)
}

func TestNewSession_WithDistributedStorage(t *testing.T) {
	setupTestRedis(t)
	defer cache.Close()

	// 初始化分布式会话管理器
	InitDistributedSessionManager(true)

	// 使用NewSession函数创建会话
	payload := NewSessionPayload{
		TenantID:               "test-tenant",
		UserID:                 "test-user",
		PluginUniqueIdentifier: "test-plugin",
		ClusterID:              "test-cluster",
		InvokeFrom:             access_types.PLUGIN_ACCESS_TYPE_TOOL,
		Action:                 access_types.PLUGIN_ACCESS_ACTION_INVOKE_TOOL,
		IgnoreCache:            false,
	}

	session := NewSession(payload)
	assert.NotEmpty(t, session.ID)
	assert.Equal(t, payload.TenantID, session.TenantID)

	// 验证会话可以通过GetSession获取
	retrievedSession, err := GetSession(GetSessionPayload{
		ID:          session.ID,
		IgnoreCache: false,
	})
	assert.NoError(t, err)
	assert.Equal(t, session.ID, retrievedSession.ID)

	// 清理
	DeleteSession(DeleteSessionPayload{
		ID:          session.ID,
		IgnoreCache: false,
	})
}