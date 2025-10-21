package cluster

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/langgenius/dify-plugin-daemon/internal/utils/log"
)

func constructRedirectUrl(ip address, request *http.Request) string {
	// 获取原始路径
	originalPath := request.URL.Path
	rawQuery := request.URL.RawQuery
	
	log.Info("Constructing redirect URL. Original path: %s, Query: %s", originalPath, rawQuery)
	
	// 检查路径是否已经包含正确的前缀
	path := originalPath
	if !strings.HasPrefix(path, "/e/") && !strings.HasPrefix(path, "/plugin/") {
		// 如果路径不以/e或/plugin开头，需要添加/e前缀
		// 但要确保不重复添加
		if !strings.HasPrefix(path, "/e") {
			path = "/e" + path
			log.Info("Added /e prefix to path. New path: %s", path)
		}
	}
	
	// 构建完整的URL
	url := "http://" + ip.fullAddress() + path
	if rawQuery != "" {
		url += "?" + rawQuery
	}
	
	log.Info("Final redirect URL: %s", url)
	return url
}

// basic redirect request
func redirectRequestToIp(ip address, request *http.Request) (int, http.Header, io.ReadCloser, error) {
	url := constructRedirectUrl(ip, request)
	
	log.Info("Redirecting request to: %s", url)

	// create a new request
	redirectedRequest, err := http.NewRequest(
		request.Method,
		url,
		request.Body,
	)

	if err != nil {
		log.Error("Failed to create redirect request: %v", err)
		return 0, nil, nil, err
	}

	// copy headers
	for key, values := range request.Header {
		for _, value := range values {
			redirectedRequest.Header.Add(key, value)
		}
	}
	
	// 记录请求头信息
	log.Info("Redirect request headers:")
	for key, values := range redirectedRequest.Header {
		log.Info("  %s: %v", key, values)
	}

	client := http.DefaultClient
	resp, err := client.Do(redirectedRequest)

	if err != nil {
		log.Error("Failed to execute redirect request: %v", err)
		return 0, nil, nil, err
	}
	
	log.Info("Redirect request successful. Status code: %d", resp.StatusCode)

	return resp.StatusCode, resp.Header, resp.Body, nil
}

// RedirectRequest redirects the request to the specified node
func (c *Cluster) RedirectRequest(
	node_id string, request *http.Request,
) (int, http.Header, io.ReadCloser, error) {
	log.Info("Redirecting request to node: %s", node_id)
	
	node, ok := c.nodes.Load(node_id)
	if !ok {
		log.Error("Node not found: %s", node_id)
		return 0, nil, nil, errors.New("node not found")
	}

	ips := c.SortIps(node)
	if len(ips) == 0 {
		log.Error("No available IP found for node: %s", node_id)
		return 0, nil, nil, errors.New("no available ip found")
	}

	ip := ips[0]
	log.Info("Selected IP for redirect: %s", ip.fullAddress())

	return redirectRequestToIp(ip, request)
}