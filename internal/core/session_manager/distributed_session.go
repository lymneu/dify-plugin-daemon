package session_manager

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/langgenius/dify-plugin-daemon/internal/core/plugin_daemon/access_types"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/cache"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/log"
	"github.com/langgenius/dify-plugin-daemon/pkg/entities/plugin_entities"
)

const (
	// Redis键前缀
	DISTRIBUTED_SESSION_PREFIX = "distributed:session"
	SESSION_INDEX_PREFIX       = "session:index"
	SESSION_TTL                = time.Hour * 2 // 会话TTL 2小时
)

// DistributedSessionStorage 分布式会话存储接口
type DistributedSessionStorage interface {
	// 创建会话
	CreateSession(session *Session) error
	// 获取会话
	GetSession(sessionID string) (*Session, error)
	// 删除会话
	DeleteSession(sessionID string) error
	// 更新会话
	UpdateSession(session *Session) error
	// 按条件查询会话
	GetSessionsByTenant(tenantID string) ([]*Session, error)
	GetSessionsByUser(tenantID, userID string) ([]*Session, error)
	// 会话健康检查和清理
	CleanupExpiredSessions() error
}

// RedisSessionStorage Redis会话存储实现
type RedisSessionStorage struct{}

// DistributedSession 分布式会话结构
type DistributedSession struct {
	ID                     string                                 `json:"id"`
	TenantID               string                                 `json:"tenant_id"`
	UserID                 string                                 `json:"user_id"`
	PluginUniqueIdentifier plugin_entities.PluginUniqueIdentifier `json:"plugin_unique_identifier"`
	ClusterID              string                                 `json:"cluster_id"`
	InvokeFrom             access_types.PluginAccessType          `json:"invoke_from"`
	Action                 access_types.PluginAccessAction        `json:"action"`
	Declaration            *plugin_entities.PluginDeclaration     `json:"declaration"`
	ConversationID         *string                                `json:"conversation_id"`
	MessageID              *string                                `json:"message_id"`
	AppID                  *string                                `json:"app_id"`
	EndpointID             *string                                `json:"endpoint_id"`
	Context                map[string]any                         `json:"context"`
	CreatedAt              time.Time                              `json:"created_at"`
	UpdatedAt              time.Time                              `json:"updated_at"`
	ExpiresAt              time.Time                              `json:"expires_at"`
	NodeID                 string                                 `json:"node_id"` // 处理该会话的节点ID
}

// 新建RedisSessionStorage实例
func NewRedisSessionStorage() DistributedSessionStorage {
	return &RedisSessionStorage{}
}

// sessionKey 生成会话的Redis键
func (r *RedisSessionStorage) sessionKey(sessionID string) string {
	return fmt.Sprintf("%s:%s", DISTRIBUTED_SESSION_PREFIX, sessionID)
}

// indexKey 生成索引键
func (r *RedisSessionStorage) indexKey(indexType, value string) string {
	return fmt.Sprintf("%s:%s:%s", SESSION_INDEX_PREFIX, indexType, value)
}

// CreateSession 创建分布式会话
func (r *RedisSessionStorage) CreateSession(session *Session) error {
	distributedSession := &DistributedSession{
		ID:                     session.ID,
		TenantID:               session.TenantID,
		UserID:                 session.UserID,
		PluginUniqueIdentifier: session.PluginUniqueIdentifier,
		ClusterID:              session.ClusterID,
		InvokeFrom:             session.InvokeFrom,
		Action:                 session.Action,
		Declaration:            session.Declaration,
		ConversationID:         session.ConversationID,
		MessageID:              session.MessageID,
		AppID:                  session.AppID,
		EndpointID:             session.EndpointID,
		Context:                session.Context,
		CreatedAt:              time.Now(),
		UpdatedAt:              time.Now(),
		ExpiresAt:              time.Now().Add(SESSION_TTL),
		NodeID:                 "", // 将在分配时设置
	}

	// 序列化会话数据
	sessionData, err := json.Marshal(distributedSession)
	if err != nil {
		return fmt.Errorf("failed to marshal session: %w", err)
	}

	sessionKey := r.sessionKey(session.ID)

	// 存储主会话数据
	if err := cache.Store(sessionKey, string(sessionData), SESSION_TTL); err != nil {
		return fmt.Errorf("failed to store session: %w", err)
	}

	return nil
}

// GetSession 获取分布式会话
func (r *RedisSessionStorage) GetSession(sessionID string) (*Session, error) {
	sessionKey := r.sessionKey(sessionID)

	sessionDataStr, err := cache.GetString(sessionKey)
	if err != nil {
		if err == cache.ErrNotFound {
			return nil, fmt.Errorf("session not found: %s", sessionID)
		}
		return nil, fmt.Errorf("failed to get session: %w", err)
	}

	var distributedSession DistributedSession
	if err := json.Unmarshal([]byte(sessionDataStr), &distributedSession); err != nil {
		return nil, fmt.Errorf("failed to unmarshal session: %w", err)
	}

	// 检查会话是否过期
	if time.Now().After(distributedSession.ExpiresAt) {
		// 异步清理过期会话
		go func() {
			if err := r.DeleteSession(sessionID); err != nil {
				log.Error("failed to cleanup expired session %s: %v", sessionID, err)
			}
		}()
		return nil, fmt.Errorf("session expired: %s", sessionID)
	}

	// 转换为原始Session结构
	session := &Session{
		ID:                     distributedSession.ID,
		TenantID:               distributedSession.TenantID,
		UserID:                 distributedSession.UserID,
		PluginUniqueIdentifier: distributedSession.PluginUniqueIdentifier,
		ClusterID:              distributedSession.ClusterID,
		InvokeFrom:             distributedSession.InvokeFrom,
		Action:                 distributedSession.Action,
		Declaration:            distributedSession.Declaration,
		ConversationID:         distributedSession.ConversationID,
		MessageID:              distributedSession.MessageID,
		AppID:                  distributedSession.AppID,
		EndpointID:             distributedSession.EndpointID,
		Context:                distributedSession.Context,
	}

	return session, nil
}

// DeleteSession 删除分布式会话
func (r *RedisSessionStorage) DeleteSession(sessionID string) error {
	sessionKey := r.sessionKey(sessionID)
	_, err := cache.Del(sessionKey)
	return err
}

// UpdateSession 更新分布式会话
func (r *RedisSessionStorage) UpdateSession(session *Session) error {
	// 先获取现有会话来保持创建时间
	_, err := r.GetSession(session.ID)
	if err != nil {
		return err
	}

	distributedSession := &DistributedSession{
		ID:                     session.ID,
		TenantID:               session.TenantID,
		UserID:                 session.UserID,
		PluginUniqueIdentifier: session.PluginUniqueIdentifier,
		ClusterID:              session.ClusterID,
		InvokeFrom:             session.InvokeFrom,
		Action:                 session.Action,
		Declaration:            session.Declaration,
		ConversationID:         session.ConversationID,
		MessageID:              session.MessageID,
		AppID:                  session.AppID,
		EndpointID:             session.EndpointID,
		Context:                session.Context,
		CreatedAt:              time.Now(), // 简化处理
		UpdatedAt:              time.Now(),
		ExpiresAt:              time.Now().Add(SESSION_TTL),
	}

	sessionData, err := json.Marshal(distributedSession)
	if err != nil {
		return fmt.Errorf("failed to marshal session: %w", err)
	}

	sessionKey := r.sessionKey(session.ID)
	return cache.Store(sessionKey, string(sessionData), SESSION_TTL)
}

// GetSessionsByTenant 按租户获取会话 (简化实现)
func (r *RedisSessionStorage) GetSessionsByTenant(tenantID string) ([]*Session, error) {
	// 这里需要一个更复杂的实现来扫描所有会话
	// 暂时返回空列表
	return []*Session{}, nil
}

// GetSessionsByUser 按用户获取会话 (简化实现)
func (r *RedisSessionStorage) GetSessionsByUser(tenantID, userID string) ([]*Session, error) {
	// 这里需要一个更复杂的实现来扫描所有会话
	// 暂时返回空列表
	return []*Session{}, nil
}

// CleanupExpiredSessions 清理过期会话
func (r *RedisSessionStorage) CleanupExpiredSessions() error {
	// 扫描所有会话键并检查是否过期
	pattern := fmt.Sprintf("%s:*", DISTRIBUTED_SESSION_PREFIX)
	
	return cache.ScanKeysAsync(pattern, func(keys []string) error {
		for _, key := range keys {
			sessionID := key[len(DISTRIBUTED_SESSION_PREFIX)+1:]
			_, err := r.GetSession(sessionID)
			if err != nil && err.Error() == fmt.Sprintf("session expired: %s", sessionID) {
				// GetSession 会自动清理过期会话
				continue
			}
		}
		return nil
	})
}