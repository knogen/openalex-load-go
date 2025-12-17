package load

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
)

const ShardCount = 32 // 分片数量，32 通常足够解决锁竞争

// ShardedSet 是一个线程安全、分片的 uint64 集合
type ShardedSet struct {
	shards [ShardCount]map[uint64]struct{}
	locks  [ShardCount]sync.RWMutex
	count  int64 // 用于原子计数
}

// NewShardedSet 初始化
func NewShardedSet() *ShardedSet {
	s := &ShardedSet{}
	for i := 0; i < ShardCount; i++ {
		s.shards[i] = make(map[uint64]struct{})
	}
	return s
}

// Add 线程安全地添加一个 ID
func (s *ShardedSet) Add(idStr string) {
	h := hashID(idStr)
	shardIdx := h % ShardCount

	s.locks[shardIdx].Lock()
	if _, exists := s.shards[shardIdx][h]; !exists {
		s.shards[shardIdx][h] = struct{}{}
		s.locks[shardIdx].Unlock()
		atomic.AddInt64(&s.count, 1) // 计数加 1
	} else {
		s.locks[shardIdx].Unlock()
	}
}

// AddBatch 批量添加 (性能极其重要)
// Worker 在本地积攒一批 hash 后一次性写入，减少锁获取次数
func (s *ShardedSet) AddBatch(hashes []uint64) {
	// 这一步虽然看起来在遍历，但因为是纯内存计算，比锁开销小得多
	// 更好的做法是先按分片分组，但为了代码简洁，这里直接循环加锁
	// 在高并发下，建议在 Worker 端就按分片 group 好，这里简化处理
	for _, h := range hashes {
		shardIdx := h % ShardCount
		s.locks[shardIdx].Lock()
		if _, exists := s.shards[shardIdx][h]; !exists {
			s.shards[shardIdx][h] = struct{}{}
			atomic.AddInt64(&s.count, 1)
		}
		s.locks[shardIdx].Unlock()
	}
}

// Contains 检查是否存在
func (s *ShardedSet) Contains(idStr string) bool {
	h := hashID(idStr)
	shardIdx := h % ShardCount

	s.locks[shardIdx].RLock() // 使用读锁
	_, exists := s.shards[shardIdx][h]
	s.locks[shardIdx].RUnlock()
	return exists
}

// Size 返回总数
func (s *ShardedSet) Size() int64 {
	return atomic.LoadInt64(&s.count)
}

// hashID 将字符串转为 uint64
// 使用 FNV-1a 算法，标准库实现，速度快且碰撞率极低
func hashID(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
