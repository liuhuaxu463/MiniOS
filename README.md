# MiniOS - Mini Object Storage Service

一个简单的对象存储服务，采用扁平化命名空间管理数据，支持跨进程 API 进行对象的上传、下载和删除。

## 项目架构

```
┌──────────────────────────────────────────────────────┐
│  CLI Client (client.rs)                              │
│  put / get / delete / list / status / start / stop   │
│  cache-resize / cache-switch / cache-benchmark       │
└──────────┬──────────────────┬───────────────────────┘
           │ Unix Socket (IPC) │ Shared Memory (shm)
           ▼                   ▼
┌──────────────────────────────────────────────────────┐
│  Server Daemon (server.rs)                           │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────────┐ │
│  │ Multi-   │ │ Shm Mgr  │ │  Storage Engine      │ │
│  │ Cache    │ │ (shm.rs) │ │  (storage.rs)        │ │
│  │ LRU/FIFO │ │ 页式分配 │ │  store.odb 持久化    │ │
│  │ /LFU     │ │          │ │                      │ │
│  └──────────┘ └──────────┘ └──────────────────────┘ │
│  ┌──────────────────────────────────────────────────┐│
│  │ Prometheus 监控 (metrics.rs)                     ││
│  │ GET :9090/metrics  → Prometheus 指标             ││
│  │ GET :9090/         → HTML Dashboard              ││
│  └──────────────────────────────────────────────────┘│
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

### 多缓存算法（Cache）
- **三种淘汰策略**：LRU（最近最少使用）、FIFO（先进先出）、LFU（最不频繁使用）
- **动态缩放**：运行时通过 `cache-resize` 命令调整缓存容量
- **算法对比**：`cache-benchmark` 命令用当前 workload 实测三种算法并排名
- **命中率统计**：实时统计 hits/misses/evictions/hit_rate
- **缓存预热**：启动时通过 `--cache-warmup` 预加载最近访问的对象

### Prometheus 监控与 Web Dashboard
- **`/metrics` 端点**：标准 Prometheus 文本格式，可接入 Grafana
- **HTML Dashboard**：自动刷新仪表板，展示存储/缓存/共享内存状态
- **零依赖**：基于 `std::net::TcpListener`，无需额外 Web 框架

### 多客户端并发
- **独立守护进程**：以 daemon 方式运行
- **Unix Domain Socket IPC**：进程间通信
- **线程池处理**：每个客户端连接分配独立线程
- **日志系统**：基于 env_logger，支持多级别（trace~error）

---

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

---

## 使用指南

### 启动服务器

```bash
# 前台运行（调试用）
./target/release/minios --server

# 自定义缓存算法和容量
./target/release/minios --server \
  --cache-algorithm lfu \
  --cache-capacity 256 \
  --cache-warmup 10 \
  --log-level info

# 启用 Prometheus 监控（默认端口 9090）
./target/release/minios --server --metrics-port 9090

# 禁用 Web 监控
./target/release/minios --server --metrics-port 0
```

### 客户端命令

```bash
# ─── 基本 CRUD ───
# 上传文件
./minios put --name myfile.txt --file /path/to/file.txt --type text/plain --tags '{"author":"me"}'

# 下载文件（按 UUID 或名称均可）
./minios get --key myfile.txt --output downloaded.txt

# 删除对象
./minios delete --key myfile.txt

# 列出所有对象
./minios list
./minios list --long            # 详细信息

# 查看服务器状态
./minios status

# ─── 缓存管理 ───
# 运行时动态调整缓存容量
./minios cache-resize --capacity 512

# 切换缓存算法（需重启服务器生效，命令会给出指引）
./minios cache-switch --algorithm fifo

# 运行缓存算法对比测试 —— 用当前所有对象模拟随机 GET 访问，对比 LRU/FIFO/LFU 的命中率
./minios cache-benchmark --iterations 200

# ─── 服务器启停 ───
./minios start --daemon          # 后台启动
./minios stop                    # 停止
```

---

## 多缓存算法详解

### 算法说明

| 算法 | 淘汰策略 | 适用场景 |
|------|----------|----------|
| **LRU** (默认) | 淘汰最久未访问的条目 | 通用场景，适合热/冷数据分明的 workload |
| **FIFO** | 淘汰最早插入的条目 | 数据无明显的冷热之分，按时间顺序淘汰 |
| **LFU** | 淘汰访问次数最少的条目 | 少量热点 + 大量低频一次性的场景 |

### 启动时选择算法

```bash
./target/release/minios --server --cache-algorithm lfu --cache-capacity 128
```

`--cache-algorithm` 取值：`lru`（默认）、`fifo`、`lfu`

### 运行时动态缩放

```bash
# 扩容到 512
./target/release/minios cache-resize --capacity 512

# 缩容到 32（多余条目按当前算法淘汰）
./target/release/minios cache-resize --capacity 32
```

### 算法对比 Benchmark

此功能非常适合**课程实验报告**的需求。它会用服务器中现有的所有对象模拟 N 次随机 GET 访问，分别运行在三种算法的独立缓存实例上，输出排名：

```bash
# 运行 200 次迭代的对比测试
./target/release/minios cache-benchmark --iterations 200
```

输出示例：
```
  Rank    Algorithm    Hits         Misses       Hit Rate
  ------+------------+------------+------------+----------
  🥇 1st  LFU          187          13           93.50%
  🥈 2nd  LRU          171          29           85.50%
  🥉 3rd  FIFO         142          58           71.00%

  Best algorithm: LFU (93.50% hit rate)
```

**使用方法**：先上传一系列具有不同访问模式的对象（一些大文件 + 一些频繁访问的小文件），然后运行 benchmark 观察哪种算法最适合当前 workload。

---

## Prometheus 监控与 Dashboard

### 启动监控

```bash
./target/release/minios --server --metrics-port 9090
```

### 端点列表

| 端点 | 格式 | 说明 |
|------|------|------|
| `http://<IP>:9090/metrics` | Prometheus text | 供 Prometheus/Grafana 采集 |
| `http://<IP>:9090/` | HTML | 自动刷新的可视化仪表盘 |

### Prometheus 配置

在 `prometheus.yml` 中添加：

```yaml
scrape_configs:
  - job_name: 'minios'
    static_configs:
      - targets: ['<VM_IP>:9090']
```

### 暴露的指标

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `minios_uptime_seconds` | gauge | 运行时间 |
| `minios_objects_total` | gauge | 对象总数 |
| `minios_storage_blocks_total` | gauge | 总块数 |
| `minios_storage_blocks_used` | gauge | 已用块数 |
| `minios_storage_blocks_free` | gauge | 空闲块数 |
| `minios_storage_bytes_total` | gauge | 总容量 |
| `minios_storage_bytes_used` | gauge | 已用容量 |
| `minios_cache_hits_total` | counter | 缓存命中数 |
| `minios_cache_misses_total` | counter | 缓存未命中数 |
| `minios_cache_evictions_total` | counter | 淘汰次数 |
| `minios_cache_size` | gauge | 当前缓存条目数 |
| `minios_cache_capacity` | gauge | 缓存最大容量 |
| `minios_cache_hit_rate_percent` | gauge | 缓存命中率 |
| `minios_cache_algorithm_info{algorithm}` | gauge | 当前算法标签 |
| `minios_shm_pages_total` | gauge | 共享内存总页数 |
| `minios_shm_pages_free` | gauge | 共享内存空闲页数 |

### HTML Dashboard

访问 `http://<VM_IP>:9090/` 会看到一个每 5 秒自动刷新的仪表盘，包含：
- 运行时间
- 存储容量（带进度条）
- 缓存详细信息（算法、命中率、淘汰次数）
- 共享内存使用情况

---

## 命令行参数完整列表

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
| `--cache-algorithm` | `lru` | 缓存算法：`lru` / `fifo` / `lfu` |
| `--cache-capacity` | `128` | 缓存容量（对象数） |
| `--cache-warmup` | `0` | 预热加载 N 个对象 |
| `--metrics-port` | `9090` | Prometheus 端口（0=禁用） |
| `--log-level` | `info` | 日志级别 |
| `--daemonize` | - | 以守护进程方式运行 |
| `--pid-file` | `/tmp/minios.pid` | PID 文件路径 |

## 客户端子命令

| 命令 | 说明 | 示例 |
|------|------|------|
| `put` | 上传对象 | `minios put -n "x" -f ./x.txt -t text/plain --tags '{}'` |
| `get` | 下载对象 | `minios get -k "x" -o ./out.txt` |
| `delete` | 删除对象 | `minios delete -k "x"` |
| `list` | 列出对象 | `minios list [-l]` |
| `status` | 服务器状态 | `minios status` |
| `cache-resize` | 动态缩放缓存 | `minios cache-resize -n 256` |
| `cache-switch` | 切换缓存算法 | `minios cache-switch -a fifo` |
| `cache-benchmark` | 对比缓存算法 | `minios cache-benchmark -n 200` |
| `start` | 启动服务器 | `minios start [--daemon]` |
| `stop` | 停止服务器 | `minios stop` |

---

## 项目结构

```
minios/
├── Cargo.toml          # 项目配置与依赖
├── README.md           # 项目文档
├── TEST_GUIDE.txt      # 手动测试指南
├── test.sh             # 自动化测试脚本
└── src/
    ├── main.rs         # 入口点（服务器/客户端模式分发）
    ├── error.rs        # 统一错误类型
    ├── config.rs       # 命令行参数与配置
    ├── storage.rs      # store.odb 存储引擎
    │                   #   - SuperBlock 超级块
    │                   #   - MetadataEntry 元数据条目
    │                   #   - ObjectStorage 对象存储 CRUD
    ├── shm.rs          # 共享内存管理器
    │                   #   - Header + Page Bitmap + Data Pages
    ├── cache.rs        # 多算法缓存
    │                   #   - ObjectCache (LRU/FIFO/LFU)
    │                   #   - CacheStats + AlgorithmBenchmark
    │                   #   - 动态扩容/缩容
    ├── ipc.rs          # IPC 通信协议
    │                   #   - ClientMessage/ServerMessage
    │                   #   - IpcServer/IpcClient
    ├── server.rs       # 服务器守护进程
    │                   #   - 请求路由与处理
    │                   #   - PID 文件管理
    ├── client.rs       # CLI 客户端
    │                   #   - 所有客户端命令实现
    └── metrics.rs      # Prometheus 监控 + Web Dashboard
                        #   - /metrics (Prometheus 文本格式)
                        #   - /        (HTML 仪表盘)
```

## 扩展说明

1. **多线程处理**：每个客户端连接分配独立线程，支持多生产者-多消费者模型
2. **日志系统**：基于 env_logger，支持多级别日志输出（trace/debug/info/warn/error）
3. **多缓存算法**：支持 LRU / FIFO / LFU 三种淘汰策略，可运行时缩放和对比
4. **Prometheus 监控**：零依赖 TCP 服务器，暴露标准 `/metrics` 端点和 HTML 仪表盘

---

## Prometheus + Grafana 生产级监控部署指南

以下步骤教你如何搭建业界标准的监控可视化平台。MiniOS 代码无需任何修改。

### 架构概览

```
MiniOS Server (:9090) → /metrics (Prometheus 格式)
         ↑
         │ 每 5 秒抓取一次
         │
   Prometheus (:9091) → 时序数据库（存储历史数据）
         ↑
         │ PromQL 查询
         │
   Grafana (:3000) → 可视化 Dashboard（折线图、仪表盘、表格）
```

### 第一步：确认 MiniOS 监控端点正常

```bash
# 启动 MiniOS（确保 metrics 端口开启）
./target/release/minios --server --metrics-port 9090 &

# 验证 /metrics 端点
curl http://localhost:9090/metrics
```

预期输出类似：
```
minios_uptime_seconds 120
minios_objects_total 5
minios_cache_hits_total 42
minios_cache_hit_rate_percent 78.50
...
```

### 第二步：安装 Prometheus

```bash
# 下载并解压
cd ~
wget https://github.com/prometheus/prometheus/releases/download/v3.7.3/prometheus-3.7.3.linux-amd64.tar.gz
tar xzf prometheus-3.7.3.linux-amd64.tar.gz
cd prometheus-3.7.3.linux-amd64

# 创建配置文件 prometheus.yml
cat > prometheus.yml << 'EOF'
global:
  scrape_interval: 5s
  evaluation_interval: 5s

scrape_configs:
  - job_name: 'minios'
    static_configs:
      - targets: ['localhost:9090']
EOF

# 启动 Prometheus
./prometheus --config.file=prometheus.yml --web.listen-address=:9091 &
```

启动后访问 `http://<虚拟机IP>:9091`，可以看到 Prometheus 自带的 Web UI。

验证数据是否在抓取：
1. 在 Prometheus Web UI 顶部的搜索框输入 `minios_objects_total`
2. 点击 Execute
3. 切换到 Graph 标签 —— 应该能看到数据

### 第三步：安装 Grafana

```bash
# 安装 Grafana（Ubuntu/Debian）
sudo apt-get install -y software-properties-common wget
sudo wget -q -O /usr/share/keyrings/grafana.key https://apt.grafana.com/gpg.key
echo "deb [signed-by=/usr/share/keyrings/grafana.key] https://apt.grafana.com stable main" \
    | sudo tee /etc/apt/sources.list.d/grafana.list
sudo apt-get update
sudo apt-get install -y grafana

# 启动 Grafana
sudo systemctl start grafana-server
sudo systemctl enable grafana-server
```

初始登录：`http://<虚拟机IP>:3000`，用户名 `admin`，密码 `admin`（首次登录会要求修改）。

### 第四步：配置 Grafana 数据源

1. 登录 Grafana → 左侧菜单 → Connections → Data sources
2. 点击「Add data source」→ 选择「Prometheus」
3. URL 填写 `http://localhost:9091`
4. 点击底部「Save & test」→ 应显示绿色的 "Successfully queried Prometheus API"

### 第五步：创建监控 Dashboard

#### 方法 A：手动创建（5-10 分钟）

在 Grafana 中点击「Dashboards」→「New」→「Add visualization」，逐个添加面板。每个面板的查询语句如下：

| 面板名称 | PromQL 查询 | 图表类型 | Legend |
|----------|------------|----------|--------|
| 对象总数 | `minios_objects_total` | Stat | — |
| 缓存命中率 | `minios_cache_hit_rate_percent` | Gauge | — |
| 命中/未命中趋势 | `rate(minios_cache_hits_total[5m])` 和 `rate(minios_cache_misses_total[5m])` | Time series | Hits / Misses |
| 存储使用量 | `minios_storage_bytes_used` 和 `minios_storage_bytes_total` | Time series | Used / Total |
| 数据块使用 | `minios_storage_blocks_used` | Time series | — |
| 淘汰速率 | `rate(minios_cache_evictions_total[5m])` | Time series | — |
| 缓存条目数 | `minios_cache_size` | Time series | — |
| 运行时间 | `minios_uptime_seconds` | Stat | — |
| 共享内存空闲页 | `minios_shm_pages_free` | Time series | — |

#### 方法 B：导入预配置 Dashboard

将以下 JSON 保存为 `minios-dashboard.json`，然后在 Grafana 中「Dashboards」→「Import」→ 粘贴导入。

<details>
<summary>点击展开 Dashboard JSON</summary>

```json
{
  "title": "MiniOS 监控面板",
  "panels": [
    {
      "title": "对象总数",
      "type": "stat",
      "targets": [{"expr": "minios_objects_total", "legendFormat": ""}],
      "gridPos": {"x": 0, "y": 0, "w": 6, "h": 4}
    },
    {
      "title": "缓存命中率",
      "type": "gauge",
      "targets": [{"expr": "minios_cache_hit_rate_percent", "legendFormat": ""}],
      "fieldConfig": {
        "defaults": {"unit": "percent", "min": 0, "max": 100}
      },
      "gridPos": {"x": 6, "y": 0, "w": 6, "h": 4}
    },
    {
      "title": "运行时间",
      "type": "stat",
      "targets": [{"expr": "minios_uptime_seconds", "legendFormat": ""}],
      "fieldConfig": {"defaults": {"unit": "s"}},
      "gridPos": {"x": 12, "y": 0, "w": 6, "h": 4}
    },
    {
      "title": "缓存命中/未命中速率",
      "type": "timeseries",
      "targets": [
        {"expr": "rate(minios_cache_hits_total[5m])", "legendFormat": "命中数/s"},
        {"expr": "rate(minios_cache_misses_total[5m])", "legendFormat": "未命中数/s"}
      ],
      "gridPos": {"x": 0, "y": 4, "w": 12, "h": 8}
    },
    {
      "title": "存储容量使用",
      "type": "timeseries",
      "targets": [
        {"expr": "minios_storage_bytes_used", "legendFormat": "已用"},
        {"expr": "minios_storage_bytes_total", "legendFormat": "总量"}
      ],
      "fieldConfig": {"defaults": {"unit": "bytes"}},
      "gridPos": {"x": 12, "y": 4, "w": 6, "h": 8}
    },
    {
      "title": "缓存淘汰速率",
      "type": "timeseries",
      "targets": [
        {"expr": "rate(minios_cache_evictions_total[5m])", "legendFormat": "淘汰数/s"}
      ],
      "gridPos": {"x": 0, "y": 12, "w": 8, "h": 6}
    },
    {
      "title": "共享内存空闲页",
      "type": "timeseries",
      "targets": [
        {"expr": "minios_shm_pages_free", "legendFormat": "空闲页"}
      ],
      "gridPos": {"x": 8, "y": 12, "w": 8, "h": 6}
    }
  ]
}
```
</details>

### 效果展示

配置完成后，Grafana Dashboard 会实时显示：

```
┌─────────────┬─────────────┬─────────────┐
│ 对象总数: 5  │ 命中率: 78% │ 运行: 12分  │
│    ↑ 3      │   Gauge     │   720s      │
├─────────────┴─────────────┴─────────────┤
│         缓存命中/未命中趋势              │
│    ╱╲╱╲╱╲╱ ╱╲╱╲╱╲ Hit(绿色)           │
│   ╱        ╱        Miss(红色)          │
│  时间 →                                │
├──────────────────────┬─────────────────┤
│   存储容量使用         │  淘汰速率        │
│   ████████░░ 45%     │  ╱╲  ╱╲╲        │
│                      │ ╱  ╲╱  ╲╲       │
└──────────────────────┴─────────────────┘
```

### 常用 PromQL 查询参考

```promql
# 过去 5 分钟缓存命中率平均值
avg_over_time(minios_cache_hit_rate_percent[5m])

# 过去 1 小时存储增长速率（字节/秒）
rate(minios_storage_bytes_used[1h])

# 命中率低于 50% 时的告警（可配置 Grafana Alert）
minios_cache_hit_rate_percent < 50

# 当前使用的缓存算法
minios_cache_algorithm_info{algorithm="LFU"}

# 过去 15 分钟的命中数
increase(minios_cache_hits_total[15m])
```

### 总结

| 步骤 | 命令 | 说明 |
|------|------|------|
| 1 | 启动 MiniOS | `--metrics-port 9090` 必须开启 |
| 2 | 安装 Prometheus | 下载解压 → 写 5 行配置 → 启动 |
| 3 | 安装 Grafana | `apt install grafana` |
| 4 | 配数据源 | Grafana UI 里填 `localhost:9091` |
| 5 | 建 Dashboard | 复制 JSON 导入，或手动拖面板 |

无需修改任何 Rust 代码，三个进程独立运行，彼此通过 HTTP 通信。
