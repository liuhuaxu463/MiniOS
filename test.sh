#!/bin/bash
# ============================================================================
# MiniOS 全功能自动化测试脚本
# 使用方法: chmod +x test.sh && ./test.sh
# ============================================================================

set -eu

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

PASS=0
FAIL=0
MINIOS="./target/release/minios"
SERVER_PID=""
SOCKET="/tmp/minios_test.sock"
SHM="/minios_shm"
STORE="./test_store.odb"
PID_FILE="/tmp/minios_test.pid"
TEST_DIR="/tmp/minios_test_files"

# --------------------------------------------------------------------------
# 辅助函数
# --------------------------------------------------------------------------
print_header() {
    echo ""
    echo -e "${CYAN}============================================================${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}============================================================${NC}"
}

print_step() {
    echo ""
    echo -e "${BLUE}>>> $1${NC}"
}

assert_pass() {
    PASS=$((PASS + 1))
    echo -e "    ${GREEN}✓ PASS${NC}: $1"
}

assert_fail() {
    FAIL=$((FAIL + 1))
    echo -e "    ${RED}✗ FAIL${NC}: $1"
}

# Full cleanup: deletes everything including the store database
full_cleanup() {
    print_step "完整清理（含 store.odb）..."
    # 停止可能残留的服务器
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        sleep 1
    fi
    # 尝试通过 socket 停止
    if [ -S "$SOCKET" ]; then
        "$MINIOS" --socket-path "$SOCKET" stop 2>/dev/null || true
    fi
    rm -f "$SOCKET" "$STORE" "$PID_FILE"
    rm -rf "$TEST_DIR"
    echo "  完整清理完成"
}

# Soft cleanup: stop server but KEEP the store database
soft_cleanup() {
    print_step "软清理（保留 store.odb 数据）..."
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        sleep 1
    fi
    if [ -S "$SOCKET" ]; then
        "$MINIOS" --socket-path "$SOCKET" stop 2>/dev/null || true
    fi
    rm -f "$SOCKET" "$PID_FILE"
    echo "  软清理完成"
}

cleanup() {
    full_cleanup
}

start_server() {
    print_step "启动 MiniOS 服务器..."
    "$MINIOS" \
        --server \
        --socket-path "$SOCKET" \
        --shm-name "$SHM" \
        --shm-size 16777216 \
        --page-size 4096 \
        --store-path "$STORE" \
        --block-size 4096 \
        --total-blocks 51200 \
        --max-objects 1000 \
        --cache-capacity 64 \
        --cache-warmup 0 \
        --log-level warn \
        --pid-file "$PID_FILE" &
    SERVER_PID=$!
    echo "  服务器 PID: $SERVER_PID"

    # 等待服务器就绪
    for i in $(seq 1 30); do
        if [ -S "$SOCKET" ]; then
            echo -e "  ${GREEN}服务器已就绪 (socket: $SOCKET)${NC}"
            sleep 0.5
            return 0
        fi
        sleep 0.3
    done
    echo -e "  ${RED}服务器启动超时！${NC}"
    exit 1
}

stop_server() {
    print_step "通过客户端命令停止服务器..."
    if "$MINIOS" --socket-path "$SOCKET" stop 2>/dev/null; then
        echo -e "  ${GREEN}服务器已通过 IPC 命令停止${NC}"
        assert_pass "服务器正常停止"
    else
        echo -e "  ${YELLOW}IPC 停止失败，强制终止...${NC}"
        if [ -n "$SERVER_PID" ]; then
            kill "$SERVER_PID" 2>/dev/null || true
        fi
        assert_fail "服务器正常停止"
    fi
    wait "$SERVER_PID" 2>/dev/null || true
    sleep 1
}

# --------------------------------------------------------------------------
# 主测试流程
# --------------------------------------------------------------------------
main() {
    trap cleanup EXIT

    print_header "MiniOS 全功能测试套件"
    echo "测试开始时间: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "工作目录: $(pwd)"

    # ======================================================================
    # 阶段 1: 编译
    # ======================================================================
    print_header "阶段 1: 编译项目"

    print_step "执行 cargo build --release..."
    if cargo build --release 2>&1 | tail -5; then
        assert_pass "项目编译成功"
    else
        assert_fail "项目编译失败"
        exit 1
    fi

    print_step "检查二进制文件..."
    if [ -x "$MINIOS" ]; then
        echo "  二进制文件: $MINIOS"
        echo "  文件大小:   $(ls -lh "$MINIOS" | awk '{print $5}')"
        assert_pass "二进制文件存在且可执行"
    else
        assert_fail "二进制文件不存在"
        exit 1
    fi

    print_step "检查版本信息..."
    "$MINIOS" --help > /dev/null 2>&1
    assert_pass "--help 正常输出"

    # ======================================================================
    # 阶段 2: 服务器启动
    # ======================================================================
    print_header "阶段 2: 服务器启停测试"

    cleanup
    start_server

    # ==== 2.1: STATUS 测试 ====
    print_step "测试 status 命令..."
    STATUS_OUT=$("$MINIOS" --socket-path "$SOCKET" status 2>&1)
    echo "$STATUS_OUT"
    if echo "$STATUS_OUT" | grep -q "MiniOS Server Status"; then
        assert_pass "status 命令返回正常"
    else
        assert_fail "status 命令返回异常"
    fi

    if echo "$STATUS_OUT" | grep -q "Objects:"; then
        assert_pass "status 包含对象计数"
    fi

    if echo "$STATUS_OUT" | grep -q "Cache"; then
        assert_pass "status 包含缓存信息"
    fi

    if echo "$STATUS_OUT" | grep -q "Shared Memory"; then
        assert_pass "status 包含共享内存信息"
    fi

    # ======================================================================
    # 阶段 3: 对象操作功能测试
    # ======================================================================
    print_header "阶段 3: 对象 CRUD 操作测试"

    # 准备测试文件
    mkdir -p "$TEST_DIR"

    # ==== 3.1: PUT - 小文本文件 ====
    print_step "测试 PUT: 上传小文本文件..."
    # 用 printf 精确控制内容，避免 echo / locale 差异导致 diff 失败
    printf 'Hello, MiniOS! This is a test file.\nLine two\nLine three\n' > "$TEST_DIR/small.txt"

    PUT_OUT=$("$MINIOS" --socket-path "$SOCKET" put \
        --name "test-small" \
        --file "$TEST_DIR/small.txt" \
        --type "text/plain" \
        --tags '{"author":"tester","type":"small"}' 2>&1)
    echo "$PUT_OUT"

    if echo "$PUT_OUT" | grep -q "Object stored successfully"; then
        assert_pass "PUT 小文件成功"
    else
        assert_fail "PUT 小文件失败"
    fi

    # 提取 UUID
    SMALL_UUID=$(echo "$PUT_OUT" | grep "UUID:" | awk '{print $2}')
    echo "  对象 UUID: $SMALL_UUID"

    # ==== 3.2: PUT - 更大的文件 (测试多块存储) ====
    print_step "测试 PUT: 上传大文件 (测试多块/多页存储)..."
    # 生成约 100KB 的随机内容文件
    dd if=/dev/urandom of="$TEST_DIR/large.bin" bs=1024 count=100 2>/dev/null

    PUT_LARGE_OUT=$("$MINIOS" --socket-path "$SOCKET" put \
        --name "test-large" \
        --file "$TEST_DIR/large.bin" \
        --type "application/octet-stream" \
        --tags '{"author":"tester","type":"large"}' 2>&1)
    echo "$PUT_LARGE_OUT"

    if echo "$PUT_LARGE_OUT" | grep -q "Object stored successfully"; then
        assert_pass "PUT 大文件成功"
    else
        assert_fail "PUT 大文件失败"
    fi

    LARGE_UUID=$(echo "$PUT_LARGE_OUT" | grep "UUID:" | awk '{print $2}')

    # ==== 3.3: PUT - 空文件 ====
    print_step "测试 PUT: 上传空文件..."
    touch "$TEST_DIR/empty.txt"

    PUT_EMPTY_OUT=$("$MINIOS" --socket-path "$SOCKET" put \
        --name "test-empty" \
        --file "$TEST_DIR/empty.txt" \
        --type "text/plain" \
        --tags '{}' 2>&1)
    echo "$PUT_EMPTY_OUT"

    if echo "$PUT_EMPTY_OUT" | grep -q "Object stored successfully"; then
        assert_pass "PUT 空文件成功"
    else
        assert_fail "PUT 空文件失败"
    fi

    # ==== 3.4: PUT - 重复名称 (预期失败) ====
    print_step "测试 PUT: 重复名称 (应返回错误)..."
    PUT_DUP_OUT=$("$MINIOS" --socket-path "$SOCKET" put \
        --name "test-small" \
        --file "$TEST_DIR/small.txt" \
        --type "text/plain" \
        --tags '{}' 2>&1)
    echo "$PUT_DUP_OUT"

    if echo "$PUT_DUP_OUT" | grep -qi "already exists\|ALREADY_EXISTS"; then
        assert_pass "PUT 拒绝重复名称"
    else
        assert_fail "PUT 重复名称未被拒绝"
    fi

    # ==== 3.5: LIST - 短格式 ====
    print_step "测试 LIST: 短格式..."
    LIST_OUT=$("$MINIOS" --socket-path "$SOCKET" list 2>&1)
    echo "$LIST_OUT"

    if echo "$LIST_OUT" | grep -q "test-small"; then
        assert_pass "LIST 包含 test-small"
    fi
    if echo "$LIST_OUT" | grep -q "test-large"; then
        assert_pass "LIST 包含 test-large"
    fi
    if echo "$LIST_OUT" | grep -q "test-empty"; then
        assert_pass "LIST 包含 test-empty"
    fi

    # ==== 3.6: LIST - 长格式 ====
    print_step "测试 LIST: 长格式..."
    LIST_LONG_OUT=$("$MINIOS" --socket-path "$SOCKET" list --long 2>&1)
    echo "$LIST_LONG_OUT"

    # ==== 3.7: GET - 按名称下载 ====
    print_step "测试 GET: 按名称下载..."
    GET_OUT=$("$MINIOS" --socket-path "$SOCKET" get \
        --key "test-small" \
        --output "$TEST_DIR/downloaded_small.txt" 2>&1)
    echo "$GET_OUT"

    if echo "$GET_OUT" | grep -q "Downloaded"; then
        assert_pass "GET 按名称成功"
    else
        assert_fail "GET 按名称失败"
    fi

    # 验证下载的文件内容
    if diff "$TEST_DIR/small.txt" "$TEST_DIR/downloaded_small.txt" > /dev/null 2>&1; then
        assert_pass "GET 下载内容与原文件一致"
    else
        assert_fail "GET 下载内容不一致"
    fi

    # ==== 3.8: GET - 按 UUID 下载 ====
    print_step "测试 GET: 按 UUID 下载..."
    GET_UUID_OUT=$("$MINIOS" --socket-path "$SOCKET" get \
        --key "$LARGE_UUID" \
        --output "$TEST_DIR/downloaded_large.bin" 2>&1)
    echo "$GET_UUID_OUT"

    if echo "$GET_UUID_OUT" | grep -q "Downloaded"; then
        assert_pass "GET 按 UUID 成功"
    else
        assert_fail "GET 按 UUID 失败"
    fi

    # 验证大文件内容
    if diff "$TEST_DIR/large.bin" "$TEST_DIR/downloaded_large.bin" > /dev/null 2>&1; then
        assert_pass "GET 大文件内容一致"
    else
        assert_fail "GET 大文件内容不一致"
    fi

    # ==== 3.9: GET - 不存在的对象 ====
    print_step "测试 GET: 不存在的对象 (应返回错误)..."
    GET_MISS_OUT=$("$MINIOS" --socket-path "$SOCKET" get \
        --key "nonexistent-file" \
        --output "$TEST_DIR/should_not_exist.txt" 2>&1)
    echo "$GET_MISS_OUT"

    if echo "$GET_MISS_OUT" | grep -qi "not found\|NOT_FOUND"; then
        assert_pass "GET 正确返回'未找到'错误"
    else
        assert_fail "GET 未找到对象时的错误处理不当"
    fi

    # ==== 3.10: DELETE - 按名称删除 ====
    print_step "测试 DELETE: 按名称删除空文件..."
    DEL_OUT=$("$MINIOS" --socket-path "$SOCKET" delete --key "test-empty" 2>&1)
    echo "$DEL_OUT"

    if echo "$DEL_OUT" | grep -q "deleted\|Deleted"; then
        assert_pass "DELETE 按名称成功"
    else
        assert_fail "DELETE 按名称失败"
    fi

    # 验证已删除
    DEL_VERIFY_OUT=$("$MINIOS" --socket-path "$SOCKET" get \
        --key "test-empty" \
        --output "$TEST_DIR/deleted_verify.txt" 2>&1)
    if echo "$DEL_VERIFY_OUT" | grep -qi "not found\|NOT_FOUND"; then
        assert_pass "DELETE 后对象确实不存在"
    else
        assert_fail "DELETE 后对象仍可访问"
    fi

    # ==== 3.11: DELETE - 按 UUID 删除 ====
    print_step "测试 DELETE: 按 UUID 删除大文件..."
    DEL_UUID_OUT=$("$MINIOS" --socket-path "$SOCKET" delete --key "$LARGE_UUID" 2>&1)
    echo "$DEL_UUID_OUT"

    if echo "$DEL_UUID_OUT" | grep -q "deleted\|Deleted"; then
        assert_pass "DELETE 按 UUID 成功"
    else
        assert_fail "DELETE 按 UUID 失败"
    fi

    # 验证大文件已被删除
    DEL_LARGE_VERIFY=$("$MINIOS" --socket-path "$SOCKET" get \
        --key "$LARGE_UUID" \
        --output "$TEST_DIR/large_deleted.bin" 2>&1)
    if echo "$DEL_LARGE_VERIFY" | grep -qi "not found\|NOT_FOUND"; then
        assert_pass "大文件 DELETE 后确实不存在"
    else
        assert_fail "大文件 DELETE 后仍可访问"
    fi

    # ==== 3.12: DELETE - 不存在的对象 ====
    print_step "测试 DELETE: 不存在的对象 (应返回错误)..."
    DEL_MISS_OUT=$("$MINIOS" --socket-path "$SOCKET" delete --key "ghost-file" 2>&1)
    echo "$DEL_MISS_OUT"

    if echo "$DEL_MISS_OUT" | grep -qi "not found\|NOT_FOUND"; then
        assert_pass "DELETE 正确返回'未找到'错误"
    else
        assert_fail "DELETE 未找到对象时的错误处理不当"
    fi

    # ======================================================================
    # 阶段 4: 缓存功能验证
    # ======================================================================
    print_header "阶段 4: 缓存功能验证"

    print_step "多次 GET 同一对象以观察缓存命中率..."
    # 确保 test-small 还在
    for i in 1 2 3 4 5; do
        "$MINIOS" --socket-path "$SOCKET" get \
            --key "test-small" \
            --output /dev/null 2>&1
    done

    CACHE_STATUS=$("$MINIOS" --socket-path "$SOCKET" status 2>&1)
    echo "$CACHE_STATUS" | grep -A5 "Cache"

    if echo "$CACHE_STATUS" | grep -q "Hit rate"; then
        assert_pass "缓存命中率统计正常"
    fi
    if echo "$CACHE_STATUS" | grep -q "Hits:"; then
        assert_pass "缓存命中计数正常"
    fi
    if echo "$CACHE_STATUS" | grep -q "Misses:"; then
        assert_pass "缓存未命中计数正常"
    fi

    # ======================================================================
    # 阶段 5: LIST 更新验证
    # ======================================================================
    print_header "阶段 5: 删除后的 LIST 验证"

    LIST2_OUT=$("$MINIOS" --socket-path "$SOCKET" list 2>&1)
    echo "$LIST2_OUT"

    if echo "$LIST2_OUT" | grep -q "test-small"; then
        assert_pass "LIST 仍包含未删除的 test-small"
    fi
    if ! echo "$LIST2_OUT" | grep -q "test-empty"; then
        assert_pass "LIST 不再包含已删除的 test-empty"
    else
        assert_fail "LIST 仍包含已删除的 test-empty"
    fi
    if ! echo "$LIST2_OUT" | grep -q "test-large"; then
        assert_pass "LIST 不再包含已删除的 test-large"
    else
        assert_fail "LIST 仍包含已删除的 test-large"
    fi

    # ======================================================================
    # 阶段 6: 特殊字符和边界测试
    # ======================================================================
    print_header "阶段 6: 边界测试"

    # ==== 6.1: 特殊字符名称 ====
    print_step "测试 PUT: 特殊字符名称..."
    echo "content with special chars" > "$TEST_DIR/special.txt"
    SPECIAL_OUT=$("$MINIOS" --socket-path "$SOCKET" put \
        --name "my-file_v1.0 (copy)" \
        --file "$TEST_DIR/special.txt" \
        --type "text/plain" \
        --tags '{"chars":"-_.()"}' 2>&1)
    echo "$SPECIAL_OUT"

    if echo "$SPECIAL_OUT" | grep -q "Object stored successfully"; then
        assert_pass "PUT 特殊字符名称成功"
    else
        assert_fail "PUT 特殊字符名称失败"
    fi

    # ==== 6.2: 中文内容 ====
    print_step "测试 PUT: 中文内容..."
    printf '你好，MiniOS！这是一个中文测试文件。\n对象存储服务测试\n' > "$TEST_DIR/chinese.txt"

    CN_OUT=$("$MINIOS" --socket-path "$SOCKET" put \
        --name "中文测试" \
        --file "$TEST_DIR/chinese.txt" \
        --type "text/plain; charset=utf-8" \
        --tags '{"lang":"zh-CN"}' 2>&1)
    echo "$CN_OUT"

    if echo "$CN_OUT" | grep -q "Object stored successfully"; then
        assert_pass "PUT 中文内容文件成功"
    else
        assert_fail "PUT 中文内容文件失败"
    fi

    # 下载并验证中文内容
    "$MINIOS" --socket-path "$SOCKET" get \
        --key "中文测试" \
        --output "$TEST_DIR/chinese_dl.txt" 2>&1

    if diff "$TEST_DIR/chinese.txt" "$TEST_DIR/chinese_dl.txt" > /dev/null 2>&1; then
        assert_pass "GET 中文内容一致"
    else
        assert_fail "GET 中文内容不一致"
    fi

    # ==== 6.3: JSON tags 验证 ====
    print_step "测试 PUT: 复杂 JSON tags..."
    echo "json test" > "$TEST_DIR/json_test.txt"
    JSON_TAGS='{"nested":{"key":"value"},"array":[1,2,3],"bool":true}'
    JSON_OUT=$("$MINIOS" --socket-path "$SOCKET" put \
        --name "json-tags" \
        --file "$TEST_DIR/json_test.txt" \
        --type "application/json" \
        --tags "$JSON_TAGS" 2>&1)
    echo "$JSON_OUT"

    if echo "$JSON_OUT" | grep -q "Object stored successfully"; then
        assert_pass "PUT 复杂 JSON tags 成功"
    else
        assert_fail "PUT 复杂 JSON tags 失败"
    fi

    # ======================================================================
    # 阶段 7: 服务器启停控制测试
    # ======================================================================
    print_header "阶段 7: 服务器启停控制测试"

    # 重新启动并停止，测试完整生命周期
    stop_server

    print_step "重新启动服务器..."
    rm -f "$SOCKET" "$PID_FILE"
    soft_cleanup

    start_server

    print_step "验证服务器重启后数据持久性..."
    RESTART_LIST=$("$MINIOS" --socket-path "$SOCKET" list 2>&1)
    echo "$RESTART_LIST"

    if echo "$RESTART_LIST" | grep -q "test-small"; then
        assert_pass "重启后 test-small 仍存在 (数据持久性)"
    else
        assert_fail "重启后 test-small 丢失"
    fi

    if echo "$RESTART_LIST" | grep -q "中文测试"; then
        assert_pass "重启后中文对象仍存在 (数据持久性)"
    else
        assert_fail "重启后中文对象丢失"
    fi

    # ======================================================================
    # 阶段 8: 最终状态检查
    # ======================================================================
    print_header "阶段 8: 最终状态检查"

    FINAL_STATUS=$("$MINIOS" --socket-path "$SOCKET" status 2>&1)
    echo "$FINAL_STATUS"

    print_step "检查 store.odb 文件..."
    if [ -f "$STORE" ]; then
        FILE_SIZE=$(ls -lh "$STORE" | awk '{print $5}')
        echo "  store.odb 大小: $FILE_SIZE"
        assert_pass "store.odb 持久化文件存在"
    else
        assert_fail "store.odb 持久化文件不存在"
    fi

    # ======================================================================
    # 测试总结
    # ======================================================================
    print_header "测试总结"
    TOTAL=$((PASS + FAIL))
    echo ""
    echo "  总测试数:  $TOTAL"
    echo -e "  通过:      ${GREEN}$PASS${NC}"
    echo -e "  失败:      ${RED}$FAIL${NC}"
    echo ""

    if [ "$FAIL" -eq 0 ]; then
        echo -e "  ${GREEN}═══════════════════════════════════════${NC}"
        echo -e "  ${GREEN}  所有测试通过！✓${NC}"
        echo -e "  ${GREEN}═══════════════════════════════════════${NC}"
        echo ""
        exit 0
    else
        echo -e "  ${RED}═══════════════════════════════════════${NC}"
        echo -e "  ${RED}  有 $FAIL 个测试失败 ✗${NC}"
        echo -e "  ${RED}═══════════════════════════════════════${NC}"
        echo ""
        exit 1
    fi
}

main "$@"
