package dcache

import (
	"fmt"
	"log"
	"net/http"
	"strings"
)

// 提供被其他节点访问的能力（基于http）

const defaultBasePath = "/_dcache/"

// 承载节点间HTTP通信的核心数据结构
type HTTPPool struct {
	self string // 用来记录自己的地址，包括主机名/IP 和端口
	// 作为节点间通讯地址的前缀，默认是 /_dcache/，那么 http://example.com/_dcache/ 开头的请求，
	// 就用于节点间的访问。因为一个主机上还可能承载其他的服务，加一段 Path 是一个好习惯。
	// 比如，大部分网站的 API 接口，一般以 /api 作为前缀。
	basePath string
}

func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

// Log info with server name
func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 首先判断访问路径的前缀是否是 basePath，不是返回错误。
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log(r.Method, r.URL.Path)
	// 我们约定访问路径格式为 /<basepath>/<groupname>/<key>，通过 groupname 得到 group 实例，
	// 再使用 group.Get(key) 获取缓存数据。
	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	groupName := parts[0]
	key := parts[1]

	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(view.ByteSlice())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
