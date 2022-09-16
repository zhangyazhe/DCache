package dcache

type PeerPicker interface {
	// PickPeer 用于根据传入的 key 选择相应节点 PeerGetter
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter 是一个节点的客户端
type PeerGetter interface {
	// Get 用于从对应 group 查找缓存值。PeerGetter 就对应于流程中的 HTTP 客户端。
	Get(group string, key string) ([]byte, error)
}
