package dcache

import (
	"fmt"
	"log"
	"reflect"
	"testing"
)

// 在这个测试用例中，我们借助 GetterFunc 的类型转换，将一个匿名回调函数转换成了接口 f Getter。
// 调用该接口的方法 f.Get(key string)，实际上就是在调用匿名回调函数。
func TestGetter(t *testing.T) {
	var f Getter = GetterFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	})

	expect := []byte("key")
	if v, _ := f.Get("key"); !reflect.DeepEqual(v, expect) {
		t.Errorf("callback failed")
	}
}

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func TestGet(t *testing.T) {
	loadCounts := make(map[string]int, len(db)) // loadCounts用于记录从数据库中读取数据的次数
	g := NewGroup("scores", 2<<10, GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				if _, ok := loadCounts[key]; !ok {
					loadCounts[key] = 0
				}
				loadCounts[key] += 1
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	for k, v := range db {
		// 第一次读取数据时，该数据不在缓存中，需要通过回调函数获取源数据。loadCounts会记录每项数据都被读取过一次
		if view, err := g.Get(k); err != nil || view.String() != v {
			t.Fatal("failed to get value of Tom")
		} // load from callback function
		// 再一次读取数据，此时数据应该已经存储在缓存中，不会再访问数据库。数据库中数据被访问的次数不应增加
		if _, err := g.Get(k); err != nil || loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		} // cache hit
	}

	if view, err := g.Get("unknown"); err == nil {
		t.Fatalf("the value of unknow should be empty, but %s got", view)
	}
}
