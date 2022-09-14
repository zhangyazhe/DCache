package consistenthash

import (
	"strconv"
	"testing"
)

func TestHashing(t *testing.T) {
	hash := New(3, func(key []byte) uint32 {
		i, _ := strconv.Atoi(string(key))
		return uint32(i)
	})
	hash.Add("2", "4", "6")
	testcases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}
	for k, v := range testcases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, expect %s, get %s", k, v, hash.Get(k))
		}
	}
	hash.Add("8")
	testcases["27"] = "8"
	for k, v := range testcases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, expect %s, get %s", k, v, hash.Get(k))
		}
	}
}
