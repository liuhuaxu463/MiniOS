//! 统一错误类型模块。
//!
//! 定义了 MiniOS 系统中所有可能的错误种类，使用 `thiserror` 派生宏
//! 自动生成 `Display` 和 `std::error::Error` 的实现。

use std::result;
use thiserror::Error;

/// MiniOS 统一错误类型。
///
/// 覆盖 I/O、JSON 序列化、存储引擎、共享内存、
/// IPC 通信、缓存等各模块的错误场景。
#[derive(Error, Debug)]
pub enum MiniOsError {
    /// 底层 I/O 错误（文件读写、网络等）
    #[error("I/O 错误: {0}")]
    Io(#[from] std::io::Error),

    /// JSON 序列化/反序列化错误
    #[error("JSON 序列化错误: {0}")]
    Json(#[from] serde_json::Error),

    /// 存储引擎内部错误
    #[error("存储错误: {0}")]
    Storage(String),

    /// 共享内存操作错误
    #[error("共享内存错误: {0}")]
    Shm(String),

    /// 进程间通信错误
    #[error("IPC 错误: {0}")]
    Ipc(String),

    /// 缓存操作错误
    #[error("缓存错误: {0}")]
    #[allow(dead_code)]
    Cache(String),

    /// 服务器内部错误
    #[error("服务器错误: {0}")]
    Server(String),

    /// 客户端内部错误
    #[error("客户端错误: {0}")]
    Client(String),

    /// 请求的对象不存在
    #[error("对象未找到: {0}")]
    NotFound(String),

    /// 尝试创建已存在的对象（名称冲突）
    #[error("对象已存在: {0}")]
    AlreadyExists(String),

    /// 磁盘空间不足
    #[error("存储空间不足")]
    NoSpace,

    /// 非法参数
    #[error("非法参数: {0}")]
    InvalidArgument(String),

    /// UTF-8 解码错误（字节数组转字符串时发生）
    #[error("UTF-8 转换错误: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    /// UTF-8 解码错误（Vec<u8> 转 String 时发生）
    #[error("字符串 UTF-8 错误: {0}")]
    FromUtf8(#[from] std::string::FromUtf8Error),
}

/// MiniOS 统一 Result 类型别名。
///
/// 所有可能失败的函数均返回此类型，失败时携带 `MiniOsError`。
pub type Result<T> = result::Result<T, MiniOsError>;
