package local_runtime

import (
	"github.com/langgenius/dify-plugin-daemon/internal/core/plugin_daemon/access_types"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/log"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/parser"
	"github.com/langgenius/dify-plugin-daemon/pkg/entities"
	"github.com/langgenius/dify-plugin-daemon/pkg/entities/plugin_entities"
)

func (r *LocalPluginRuntime) Listen(session_id string) *entities.Broadcast[plugin_entities.SessionMessage] {
	// 添加对stdioHolder的空值检查
	if r.stdioHolder == nil {
		log.Error("stdioHolder is nil when listening for session %s, plugin: %s", session_id, r.Config.Identity())
		// 返回一个空的Broadcast对象而不是nil，避免上层调用出现空指针异常
		emptyBroadcast := entities.NewBroadcast[plugin_entities.SessionMessage]()
		emptyBroadcast.OnClose(func() {
			log.Warn("Closing empty broadcast for session %s", session_id)
		})
		return emptyBroadcast
	}
	
	listener := entities.NewBroadcast[plugin_entities.SessionMessage]()
	listener.OnClose(func() {
		r.stdioHolder.removeStdioHandlerListener(session_id)
	})
	r.stdioHolder.setupStdioEventListener(session_id, func(b []byte) {
		// unmarshal the session message
		data, err := parser.UnmarshalJsonBytes[plugin_entities.SessionMessage](b)
		if err != nil {
			log.Error("unmarshal json failed: %s, failed to parse session message", err.Error())
			return
		}

		listener.Send(data)
	})
	return listener
}

func (r *LocalPluginRuntime) Write(session_id string, action access_types.PluginAccessAction, data []byte) {
	// 添加对stdioHolder的空值检查
	if r.stdioHolder == nil {
		log.Error("stdioHolder is nil when writing for session %s, plugin: %s", session_id, r.Config.Identity())
		return
	}
	
	r.stdioHolder.write(append(data, '\n'))
}