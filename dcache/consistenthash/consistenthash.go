package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// 一致性哈希

type Hash func(data []byte) uint32

type Map struct {
	replicas int // 虚拟节点的倍数
	// 哈希环, sorted。我们将所有节点（真实节点+虚拟节点）的hash值都存储在keys中并排序。某个key对应的hash来了后，比新hash
	// 小的第一个hash对应的节点即为这个key对应的节点
	keys    []int
	hash    Hash           // 允许自定义的hash函数
	hashMap map[int]string // 虚拟节点hash值到真实节点的映射
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add adds some keys to the hash
// Add 接收若干个真实节点的名称，然后将真实节点和虚拟节点都加入到hash环
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.hashMap[hash] = key
			m.keys = append(m.keys, hash)
		}
	}
	sort.Ints(m.keys)
}

// Get gets the closest node in the hash for the provided key
// Get 根据要查询的数据的key选择节点
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(key)))
	if hash > m.keys[len(m.keys)-1] {
		return m.hashMap[m.keys[0]]
	}
	for _, key := range m.keys {
		if key >= hash {
			return m.hashMap[key]
		}
	}
	return ""
}
