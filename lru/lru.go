package lru

import "container/list"

type Cache struct {
	maxBytes int64                    // maxBytes is the max memory bytes the cache can use
	nbyte    int64                    // nbytes is the memory bytes the cache is using now
	ll       *list.List               // list.List是标准库中双向链表
	cache    map[string]*list.Element // list.Element 为双向链表中每个节点的类型，其中定义了前后向的指针，以及类型为空接口的Value
	// 当某条记录被移除时的回调函数
	OnEvicted func(key string, value Value)
}

// 键值对 entry 是双向链表节点的数据类型，在链表中仍保存每个值对应的 key 的好处在于，淘汰队首节点时，需要用 key 从字典中删除对应的映射
// value 的类型是接口类型 Value，这样的设计允许值是任何实现了 Value 接口的类型，更具通用性
type entry struct {
	key   string
	value Value
}

type Value interface {
	Len() int
}

// New is the Constructor of Cache
func New(maxBytes int64, onEvicted func(key string, value Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     map[string]*list.Element{},
		OnEvicted: onEvicted,
	}
}

// Get look ups a key's value
// 查找的步骤：1.从字典中找到对应的双向链表的节点 2.将该节点移动到队尾
func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele) // 将链表中的节点 ele 移动到队尾（双向链表作为队列，队首队尾是相对的，在这里约定 front 为队尾）
		return ele.Value.(*entry).value, ok
	}
	return
}

// RemoveOldest removes the oldest item
// 删除双向链表队首的元素，然后将其在map中对应的映射也删除
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.nbyte = c.nbyte - int64(len(kv.key)) - int64(kv.value.Len())
		c.ll.Remove(ele)
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

// Add adds a value to the cache
func (c *Cache) Add(key string, value Value) {
	ele, exist := c.cache[key]
	if exist {
		c.nbyte = c.nbyte - int64(ele.Value.(*entry).value.Len()) + int64(value.Len())
		ele.Value.(*entry).value = value
		c.ll.MoveToFront(ele)
	} else {
		entry := &entry{
			key:   key,
			value: value,
		}
		ele := c.ll.PushFront(entry)
		c.cache[key] = ele
		c.nbyte += int64(len(key)) + int64(value.Len())
	}
	for c.maxBytes != 0 && c.nbyte > c.maxBytes {
		c.RemoveOldest()
	}
}

// Len the number of cache entries
func (c *Cache) Len() int {
	return c.ll.Len()
}
