package cluster

import (
	"errors"
	"io"
	"net/http"
	"strings"
)

func constructRedirectUrl(ip address, request *http.Request) string {
	// 确保路径以/e开头，这是API端点的基础路径
	basePath := "/e"
	path := request.URL.Path
	
	// 如果请求路径不以/e开头，需要添加/e前缀
	if !strings.HasPrefix(path, basePath) {
		if strings.HasPrefix(path, "/plugin/") {
			// 对于/plugin/路径，保持原样
		} else {
			// 对于其他路径，添加/e前缀
			path = basePath + path
		}
	}
	
	url := "http://" + ip.fullAddress() + path
	if request.URL.RawQuery != "" {
		url += "?" + request.URL.RawQuery
	}
	return url
}

// basic redirect request
func redirectRequestToIp(ip address, request *http.Request) (int, http.Header, io.ReadCloser, error) {
	url := constructRedirectUrl(ip, request)

	// create a new request
	redirectedRequest, err := http.NewRequest(
		request.Method,
		url,
		request.Body,
	)

	if err != nil {
		return 0, nil, nil, err
	}

	// copy headers
	for key, values := range request.Header {
		for _, value := range values {
			redirectedRequest.Header.Add(key, value)
		}
	}

	client := http.DefaultClient
	resp, err := client.Do(redirectedRequest)

	if err != nil {
		return 0, nil, nil, err
	}

	return resp.StatusCode, resp.Header, resp.Body, nil
}

// RedirectRequest redirects the request to the specified node
func (c *Cluster) RedirectRequest(
	node_id string, request *http.Request,
) (int, http.Header, io.ReadCloser, error) {
	node, ok := c.nodes.Load(node_id)
	if !ok {
		return 0, nil, nil, errors.New("node not found")
	}

	ips := c.SortIps(node)
	if len(ips) == 0 {
		return 0, nil, nil, errors.New("no available ip found")
	}

	ip := ips[0]

	return redirectRequestToIp(ip, request)
}
