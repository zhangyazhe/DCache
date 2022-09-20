package dcache

import (
	pb "DCache/dcache/dcachepb"
	"DCache/dcache/singleflight"
	"fmt"
	"log"
	"sync"
)

// Group 是 DCache 最核心的数据结构，负责与用户的交互，并且控制缓存值存储和获取的流程。
//
//                            是
// 接收 key --> 检查是否被缓存 -----> 返回缓存值 ⑴
//                |  否                         是
//                |-----> 是否应当从远程节点获取 -----> 与远程节点交互 --> 返回缓存值 ⑵
//                            |  否
//                            |-----> 调用`回调函数`，获取值并添加到缓存 --> 返回缓存值 ⑶
//
// 进一步细化流程(2):
// 使用一致性哈希选择节点        是                                    是
//    |-----> 是否是远程节点 -----> HTTP 客户端访问远程节点 --> 成功？-----> 服务端返回返回值
//                    |  否                                    ↓  否
//                    |----------------------------> 回退到本地节点处理
//
// 如果缓存不存在，应从数据源（文件，数据库等）获取数据并添加到缓存中。DCache 是否应该支持多种数据源的配置呢？
// 不应该，一是数据源的种类太多，没办法一一实现；二是扩展性不好。如何从源头获取数据，应该是用户决定的事情，我们就把这件事交给用户好了。
// 因此，我们设计了一个回调函数(callback)，在缓存不存在时，调用这个函数，得到源数据。

// A Getter loads data for a key.
type Getter interface {
	Get(key string) ([]byte, error)
}

// A GetterFunc implements Getter with a function.
// 定义函数类型 GetterFunc，并实现 Getter 接口的 Get 方法。
// 函数类型实现某一个接口，称之为接口型函数，方便使用者在调用时既能够传入函数作为参数，也能够传入实现了该接口的结构体作为参数。
// 了解接口型函数的使用场景，可以参考 https://geektutu.com/post/7days-golang-q1.html
// 具体使用方法见单测
type GetterFunc func(key string) ([]byte, error)

// Get implements Getter interface function
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// A Group is a cache namespace.
// 一个 Group 可以认为是一个缓存的命名空间，每个 Group 拥有一个唯一的名称 name。
// 比如可以创建三个 Group，缓存学生的成绩命名为 scores，缓存学生信息的命名为 info，缓存学生课程的命名为 courses。
// 第二个属性是 getter Getter，即缓存未命中时获取源数据的回调(callback)。
// 第三个属性是 mainCache cache，即一开始实现的并发缓存。
type Group struct {
	name      string
	getter    Getter
	mainCache cache
	peers     PeerPicker
	sf        *singleflight.Group
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
		sf:        &singleflight.Group{},
	}
	groups[name] = g
	return g
}

// GetGroup returns the named group previously created with NewGroup, or
// nil if there's no such group.
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// Get value for a key from cache
// Get 是最核心的函数，实现了上面的(1)(2)(3)。这里是整个分布式缓存系统的入口
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	// 检查是否被缓存
	if v, ok := g.mainCache.get(key); ok {
		// 发现本地有缓存，直接返回
		log.Println("[GeeCache] hit")
		return v, nil
	}
	// 本地没有缓存，尝试从数据库读取数据或者从其他缓存节点读取
	return g.load(key)
}

// load 先判断是否可以从其他节点获取数据，如果可以则尝试获取。如果不可以，则尝试从本地获取
// load 使用 PickPeer() 方法选择节点，若非本机节点，则调用 getFromPeer() 从远程获取。若是本机节点或失败，则回退到 getLocally()
func (g *Group) load(key string) (value ByteView, err error) {
	if g.peers != nil {
		// 判断是否可以从其他缓存节点获取缓存
		if peer, ok := g.peers.PickPeer(key); ok {
			ret, err := g.sf.Do(key, func() (interface{}, error) {
				value, err := g.GetFromPeer(peer, key)
				if err != nil {
					log.Println("[dcache] Failed to get from peer, try to get locally.", err)
				}
				return value, nil
			})
			return ret.(ByteView), err
		}
	}
	return g.getLocally(key)
}

func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.sf.Do(key, func() (interface{}, error) {
		return g.getter.Get(key)
	})
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes.([]byte))}
	g.populateCache(key, value)
	return value, nil
}

// populateCache 将 key, value 添加到缓存
func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

// RegisterPeers registers a PeerPicker for choosing remote peer
// RegisterPeers 将实现了 PeerPicker 接口的 HTTPPool 注入到 Group 中
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

// GetFromPeer 使用实现了 PeerGetter 接口的 httpGetter 从访问远程节点，获取缓存值
func (g *Group) GetFromPeer(peer PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{res.Value}, nil
}
