// Package reliablequeue 提供基于 Redis Streams 消费组的至少一次可靠队列。
//
// Publish 使用 XADD 追加消息；Subscribe 注册显式消费组，未确认消息保留在 PEL，
// 由 XAUTOCLAIM 在消费者退出或处理失败后重新认领。Handler 返回的 Publications
// 会先发布并按配置等待副本确认，随后才确认输入消息。
//
// 队列只保证至少一次投递，业务 Handler 必须使用稳定请求号实现幂等。它不替代
// PostgreSQL transactional outbox，也不提供跨 Redis 与业务数据库的事务。
package reliablequeue
