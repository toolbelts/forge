// Package ioc 提供轻量依赖注入容器与应用生命周期管理:
//
//   - Container 支持 Bind(每次调用 factory)、Singleton(首调缓存)、Instance(已构造好对象)
//     三种绑定,各自带 Named 变体可按名字区分多份同类型
//   - Get / GetNamed / Has / HasNamed 解析依赖,resolveSingleton 在解析栈中检测循环依赖
//   - App.Use 注册 Provider,App.Run(ctx, fn) 顺序跑 Register → Setup → (fn 或 Serve) → Shutdown;
//     fn == nil 走 serve 模式(阻塞到信号或 Server 退出),fn != nil 走一次性任务模式
//   - Shutdown 按已 Setup 的逆序执行,WithShutdownTimeout 控制全局超时
//
// 任一 Provider 在 Register / Setup 阶段返回错误即终止启动并对已 Setup 的依赖反向 Shutdown,
// 避免半启动状态。Serve 阶段任一 Server 退出或收到 SIGINT/SIGTERM 都会触发统一关停;
// fn 模式由 fn 自己决定何时返回,fn 与 Shutdown 错误经 errors.Join 聚合。
package ioc
