# MiniOS - Mini Object Storage Service

一个简单的对象存储服务，采用扁平化命名空间管理数据，支持跨进程 API 进行对象的上传、下载和删除。

## 项目架构

```
┌──────────────────────────────────────────────────────┐
│  CLI Client (client.rs)                              │
│  put / get / delete / list / status / start / stop   │
└──────────┬──────────────────┬───────────────────────┘
           │ Unix Socket (IPC) │ Shared Memory (shm)
           ▼                   ▼
┌──────────────────────────────────────────────────────┐
│  Server Daemon (server.rs)                           │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────────┐ │
│  │ LRU Cache│ │ Shm Mgr  │ │  Storage Engine      │ │
│  │ (cache)  │ │ (shm.rs) │ │  (storage.rs)        │ │
│  │ 热点缓存 │ │ 页式分配 │ │  store.odb 持久化    │ │
│  └──────────┘ └──────────┘ └──────────────────────┘ │
└──────────────────────────────────────────────────────┘
```

### store.odb 文件布局

```
┌──────────────────┐
│ Super Block      │ 4096 bytes — magic, version, block_size,
│                  │   total_blocks, free_blocks, object_count,
│                  │   metadata offset/size, bitmap offset/size
├──────────────────┤
│ Block Bitmap     │ 每 bit 对应一个数据块 (1=占用, 0=空闲)
├──────────────────┤
│ Metadata Area    │ 可变长元数据条目
│  ┌─────────────┐ │ uuid(16) + flags(1) + name_len(2) + name +
│  │ Entry 0     │ │ size(8) + type_len(2) + content_type +
│  │ Entry 1     │ │ created_at(8) + tags_len(2) + tags +
│  │ ...         │ │ block_count(4) + block_ptrs[] + entry_size(4)
│  └─────────────┘ │
├──────────────────┤
│ Data Blocks      │ 固定大小 4KB 数据块
│  ┌─────────────┐ │
│  │ Block 0     │ │
│  │ Block 1     │ │
│  │ ...         │ │
│  └─────────────┘ │
└──────────────────┘
```

### 共享内存布局

```
┌──────────────────┐
│ Header (64 B)    │ magic, version, total_size, page_size,
│                  │   num_pages, bitmap_offset, data_offset
├──────────────────┤
│ Page Bitmap      │ 每 bit 对应一个 page (1=占用, 0=空闲)
├──────────────────┤
│ Data Pages       │ num_pages × page_size (默认 4KB/page)
└──────────────────┘
```

## 功能特性

### 核心功能
- **扁平化命名空间**：摒弃层级目录，通过 UUID 和名称管理对象
- **对象存储**：Put / Get / Delete / List 操作
- **单一文件持久化**：所有数据存储在 `store.odb` 单一复合文件中
- **超级块管理**：记录全局元信息（对象总数、块大小、偏移量等）
- **自由块位图**：支持数据块的动态分配与回收
- **可变长元数据**：支持自定义标签、内容类型等扩展属性

### 共享内存与页式分配
- **页式管理**：共享内存按 4KB 固定页划分
- **位图分配**：页分配位图标记空闲/占用状态
- **alloc_page()**：在位图中寻找连续 N 个空闲页
- **free_page()**：释放对象占用的页
- **大对象分页传输**：大数据拆分为多个页，通过页号链表串联

### LRU 缓存
- **可配置容量**：通过命令行参数设置缓存对象数量
- **命中率统计**：实时统计缓存命中/未命中/淘汰次数
- **缓存预热**：启动时预加载最近访问的对象

### 多客户端并发
- **独立守护进程**：以 daemon 方式运行
- **Unix Domain Socket IPC**：进程间通信
- **线程池处理**：每个客户端连接分配独立线程

## 编译与运行

### 环境要求
- Rust 1.75+
- Linux 操作系统（使用了 Unix domain socket、shm_open、mmap 等 Linux 特性）

### 编译

```bash
cd minios
cargo build --release
```

编译产物位于 `target/release/minios`。

### 启动服务器

```bash
# 前台运行（调试用）
./minios --server

# 后台运行
./minios --server --daemonize

# 自定义配置
./minios --server \
  --store-path /data/minios/store.odb \
  --socket-path /tmp/minios.sock \
  --shm-name minios_shm \
  --shm-size 33554432 \
  --page-size 4096 \
  --block-size 4096 \
  --total-blocks 51200 \
  --cache-capacity 256 \
  --cache-warmup 10 \
  --log-level info
```

### 客户端命令

```bash
# 上传文件
./minios put --name myfile.txt --file /path/to/file.txt --type text/plain --tags '{"author":"me"}'

# 下载文件（按 UUID）
./minios get --key 550e8400-e29b-41d4-a716-446655440000 --output downloaded.txt

# 下载文件（按名称）
./minios get --key myfile.txt --output downloaded.txt

# 删除对象
./minios delete --key myfile.txt

# 列出所有对象
./minios list
./minios list --long   # 详细信息

# 查看服务器状态
./minios status

# 启动/停止服务器
./minios start --daemon
./minios stop
```

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--server` / `-s` | - | 以服务器模式运行 |
| `--socket-path` | `/tmp/minios.sock` | Unix socket 路径 |
| `--shm-name` | `/minios_shm` | 共享内存名称 |
| `--shm-size` | `16777216` (16MB) | 共享内存大小 |
| `--page-size` | `4096` | 页大小（字节） |
| `--store-path` | `./store.odb` | 对象数据库文件路径 |
| `--block-size` | `4096` | 数据块大小（字节） |
| `--total-blocks` | `25600` | 数据块总数（~100MB） |
| `--max-objects` | `10000` | 最大对象数 |
| `--cache-capacity` | `128` | LRU 缓存容量 |
| `--cache-warmup` | `0` | 预热加载对象数 |
| `--log-level` | `info` | 日志级别 |
| `--daemonize` | - | 以守护进程方式运行 |
| `--pid-file` | `/tmp/minios.pid` | PID 文件路径 |

## 项目结构

```
minios/
├── Cargo.toml          # 项目配置与依赖
├── README.md           # 项目文档
└── src/
    ├── main.rs         # 入口点（服务器/客户端模式分发）
    ├── error.rs        # 统一错误类型
    ├── config.rs       # 命令行参数与配置
    ├── storage.rs      # store.odb 存储引擎
    │                   #   - SuperBlock 超级块
    │                   #   - MetadataEntry 元数据条目
    │                   #   - ObjectStorage 对象存储 CRUD
    ├── shm.rs          # 共享内存管理器
    │                   #   - ShmHeader 头部结构
    │                   #   - SharedMemory 页式分配
    ├── cache.rs        # LRU 缓存
    │                   #   - ObjectCache 线程安全缓存
    │                   #   - CacheStats 命中率统计
    ├── ipc.rs          # IPC 通信协议
    │                   #   - ClientMessage/ServerMessage
    │                   #   - IpcServer/IpcClient
    ├── server.rs       # 服务器守护进程
    │                   #   - 请求路由与处理
    │                   #   - PID 文件管理
    └── client.rs       # CLI 客户端
                        #   - 所有客户端命令实现
```

## 扩展说明

1. **多线程处理**：每个客户端连接分配独立线程，支持多生产者-多消费者模型
2. **日志系统**：基于 env_logger，支持多级别日志输出（trace/debug/info/warn/error）
