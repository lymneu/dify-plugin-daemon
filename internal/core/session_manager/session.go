package session_manager

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/langgenius/dify-plugin-daemon/internal/core/dify_invocation"
	"github.com/langgenius/dify-plugin-daemon/internal/core/plugin_daemon/access_types"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/cache"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/log"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/parser"
	"github.com/langgenius/dify-plugin-daemon/pkg/entities/plugin_entities"
)

// 移除全局变量，使用分布式存储
// var (
//	sessions     map[string]*Session = map[string]*Session{}
//	session_lock sync.RWMutex
// )

// 分布式会话管理器
type DistributedSessionManager struct {
	storage DistributedSessionStorage
	// 本地缓存用于性能优化（可选）
	localCache map[string]*Session
	cacheLock  sync.RWMutex
	cacheEnabled bool
}

// 全局分布式会话管理器实例
var (
	globalSessionManager *DistributedSessionManager
	once                 sync.Once
)

// InitDistributedSessionManager 初始化分布式会话管理器
func InitDistributedSessionManager(enableLocalCache bool) {
	once.Do(func() {
		globalSessionManager = &DistributedSessionManager{
			storage:      NewRedisSessionStorage(),
			localCache:   make(map[string]*Session),
			cacheEnabled: enableLocalCache,
		}
	})
}

// GetSessionManager 获取全局会话管理器
func GetSessionManager() *DistributedSessionManager {
	if globalSessionManager == nil {
		InitDistributedSessionManager(true) // 默认开启本地缓存
	}
	return globalSessionManager
}

// session need to implement the backwards_invocation.BackwardsInvocationWriter interface
type Session struct {
	ID                  string                              `json:"id"`
	runtime             plugin_entities.PluginLifetime      `json:"-"`
	backwardsInvocation dify_invocation.BackwardsInvocation `json:"-"`

	TenantID               string                                 `json:"tenant_id"`
	UserID                 string                                 `json:"user_id"`
	PluginUniqueIdentifier plugin_entities.PluginUniqueIdentifier `json:"plugin_unique_identifier"`
	ClusterID              string                                 `json:"cluster_id"`
	InvokeFrom             access_types.PluginAccessType          `json:"invoke_from"`
	Action                 access_types.PluginAccessAction        `json:"action"`
	Declaration            *plugin_entities.PluginDeclaration     `json:"declaration"`

	// information about incoming request
	ConversationID *string        `json:"conversation_id"`
	MessageID      *string        `json:"message_id"`
	AppID          *string        `json:"app_id"`
	EndpointID     *string        `json:"endpoint_id"`
	Context        map[string]any `json:"context"`
}

func sessionKey(id string) string {
	return fmt.Sprintf("session_info:%s", id)
}

type NewSessionPayload struct {
	TenantID               string                                 `json:"tenant_id"`
	UserID                 string                                 `json:"user_id"`
	PluginUniqueIdentifier plugin_entities.PluginUniqueIdentifier `json:"plugin_unique_identifier"`
	ClusterID              string                                 `json:"cluster_id"`
	InvokeFrom             access_types.PluginAccessType          `json:"invoke_from"`
	Action                 access_types.PluginAccessAction        `json:"action"`
	Declaration            *plugin_entities.PluginDeclaration     `json:"declaration"`
	BackwardsInvocation    dify_invocation.BackwardsInvocation    `json:"backwards_invocation"`
	IgnoreCache            bool                                   `json:"ignore_cache"`
	ConversationID         *string                                `json:"conversation_id"`
	MessageID              *string                                `json:"message_id"`
	AppID                  *string                                `json:"app_id"`
	EndpointID             *string                                `json:"endpoint_id"`
	Context                map[string]any                         `json:"context"`
}

func NewSession(payload NewSessionPayload) *Session {
	s := &Session{
		ID:                     uuid.New().String(),
		TenantID:               payload.TenantID,
		UserID:                 payload.UserID,
		PluginUniqueIdentifier: payload.PluginUniqueIdentifier,
		ClusterID:              payload.ClusterID,
		InvokeFrom:             payload.InvokeFrom,
		Action:                 payload.Action,
		Declaration:            payload.Declaration,
		backwardsInvocation:    payload.BackwardsInvocation,
		ConversationID:         payload.ConversationID,
		MessageID:              payload.MessageID,
		AppID:                  payload.AppID,
		EndpointID:             payload.EndpointID,
		Context:                payload.Context,
	}

	// 使用分布式会话管理器
	manager := GetSessionManager()
	
	// 存储到分布式存储
	if err := manager.CreateSession(s); err != nil {
		log.Error("failed to create distributed session: %v", err)
		// 如果分布式存储失败，回退到旧的缓存方式
		if !payload.IgnoreCache {
			if err := cache.Store(sessionKey(s.ID), s, time.Minute*30); err != nil {
				log.Error("fallback to cache also failed: %v", err)
			}
		}
	}

	return s
}

// CreateSession 分布式会话创建
func (dsm *DistributedSessionManager) CreateSession(session *Session) error {
	// 存储到分布式存储
	if err := dsm.storage.CreateSession(session); err != nil {
		return fmt.Errorf("failed to store session in distributed storage: %w", err)
	}

	// 如果开启了本地缓存，也存储到本地
	if dsm.cacheEnabled {
		dsm.cacheLock.Lock()
		dsm.localCache[session.ID] = session
		dsm.cacheLock.Unlock()
	}

	return nil
}

type GetSessionPayload struct {
	ID          string `json:"id"`
	IgnoreCache bool   `json:"ignore_cache"`
}

func GetSession(payload GetSessionPayload) (*Session, error) {
	manager := GetSessionManager()
	return manager.GetSession(payload.ID)
}

// GetSession 分布式会话获取
func (dsm *DistributedSessionManager) GetSession(sessionID string) (*Session, error) {
	// 先检查本地缓存
	if dsm.cacheEnabled {
		dsm.cacheLock.RLock()
		if session, exists := dsm.localCache[sessionID]; exists {
			dsm.cacheLock.RUnlock()
			return session, nil
		}
		dsm.cacheLock.RUnlock()
	}

	// 从分布式存储获取
	session, err := dsm.storage.GetSession(sessionID)
	if err != nil {
		// 如果分布式存储失败，尝试从旧的缓存获取
		if legacySession, cacheErr := cache.Get[Session](sessionKey(sessionID)); cacheErr == nil {
			return legacySession, nil
		}
		return nil, fmt.Errorf("session not found: %s, storage error: %w", sessionID, err)
	}

	// 更新本地缓存
	if dsm.cacheEnabled {
		dsm.cacheLock.Lock()
		dsm.localCache[sessionID] = session
		dsm.cacheLock.Unlock()
	}

	return session, nil
}

type DeleteSessionPayload struct {
	ID          string `json:"id"`
	IgnoreCache bool   `json:"ignore_cache"`
}

func DeleteSession(payload DeleteSessionPayload) {
	manager := GetSessionManager()
	if err := manager.DeleteSession(payload.ID); err != nil {
		log.Error("failed to delete distributed session %s: %v", payload.ID, err)
	}

	// 如果分布式删除失败，也尝试从旧缓存删除
	if !payload.IgnoreCache {
		if _, err := cache.Del(sessionKey(payload.ID)); err != nil {
			log.Error("delete session info from legacy cache failed, %s", err)
		}
	}
}

// DeleteSession 分布式会话删除
func (dsm *DistributedSessionManager) DeleteSession(sessionID string) error {
	// 从分布式存储删除
	if err := dsm.storage.DeleteSession(sessionID); err != nil {
		log.Error("failed to delete session from distributed storage: %v", err)
		// 不返回错误，继续清理本地缓存
	}

	// 清理本地缓存
	if dsm.cacheEnabled {
		dsm.cacheLock.Lock()
		delete(dsm.localCache, sessionID)
		dsm.cacheLock.Unlock()
	}

	return nil
}

type CloseSessionPayload struct {
	IgnoreCache bool `json:"ignore_cache"`
}

func (s *Session) Close(payload CloseSessionPayload) {
	DeleteSession(DeleteSessionPayload{
		ID:          s.ID,
		IgnoreCache: payload.IgnoreCache,
	})
}

func (s *Session) BindRuntime(runtime plugin_entities.PluginLifetime) {
	s.runtime = runtime
}

func (s *Session) Runtime() plugin_entities.PluginLifetime {
	return s.runtime
}

func (s *Session) BindBackwardsInvocation(backwardsInvocation dify_invocation.BackwardsInvocation) {
	s.backwardsInvocation = backwardsInvocation
}

func (s *Session) BackwardsInvocation() dify_invocation.BackwardsInvocation {
	return s.backwardsInvocation
}

type PLUGIN_IN_STREAM_EVENT string

const (
	PLUGIN_IN_STREAM_EVENT_REQUEST  PLUGIN_IN_STREAM_EVENT = "request"
	PLUGIN_IN_STREAM_EVENT_RESPONSE PLUGIN_IN_STREAM_EVENT = "backwards_response"
)

func (s *Session) Message(event PLUGIN_IN_STREAM_EVENT, data any) []byte {
	return parser.MarshalJsonBytes(map[string]any{
		"session_id":      s.ID,
		"conversation_id": s.ConversationID,
		"message_id":      s.MessageID,
		"app_id":          s.AppID,
		"endpoint_id":     s.EndpointID,
		"context":         s.Context,
		"event":           event,
		"data":            data,
	})
}

func (s *Session) Write(event PLUGIN_IN_STREAM_EVENT, action access_types.PluginAccessAction, data any) error {
	if s.runtime == nil {
		return errors.New("runtime not bound")
	}
	s.runtime.Write(s.ID, action, s.Message(event, data))
	return nil
}
