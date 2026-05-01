package dbcache

import "errors"

// 错误集合,均供调用方 errors.Is 判别。
var (
	// ErrNotFound 表示 key 在数据源中不存在(即"已知不存在",已被负缓存)。
	// Loader 应在数据源 miss 时返回该错误,Cache 据此走负缓存路径。
	ErrNotFound = errors.New("dbcache: not found")

	// ErrNilLoader New 时未传 loader。
	ErrNilLoader = errors.New("dbcache: nil loader")

	// ErrNilStore Store 实例为 nil(组合时常见,如 NewTieredStore(nil, ...))。
	ErrNilStore = errors.New("dbcache: nil store")

	// ErrClosed 在 Cache.Close 之后再调用读写 API 时返回。
	ErrClosed = errors.New("dbcache: cache closed")
)
