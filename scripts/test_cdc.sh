#!/bin/bash
# CDC 功能测试脚本
# 测试 INSERT、UPDATE、DELETE 三种场景

# 配置 (可通过环境变量覆盖)
# 注意: DNMP 环境中 MySQL 映射端口为 3307
MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_PORT="${MYSQL_PORT:-3307}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-root}"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-mysql}"  # DNMP MySQL 容器名
DB_NAME="testdb"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "CDC 功能测试 - MySQL Binlog 监听"
echo "=========================================="
echo "主机: $MYSQL_HOST:$MYSQL_PORT"
echo "用户: $MYSQL_USER"
echo "数据库: $DB_NAME"
echo "=========================================="

# 执行 SQL 的辅助函数
exec_sql() {
    local sql="$1"
    if [ -n "$MYSQL_CONTAINER" ]; then
        docker exec "$MYSQL_CONTAINER" mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$DB_NAME" -e "$sql" 2>/dev/null
    else
        # 尝试多种 MySQL 客户端
        if command -v mysql &>/dev/null; then
            mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$DB_NAME" -e "$sql" 2>/dev/null
        elif command -v docker &>/dev/null; then
            docker run --rm mysql:8 mysql -h "$MYSQL_HOST" -P "$MYSQL_PORT" -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" "$DB_NAME" -e "$sql" 2>/dev/null
        else
            echo "ERROR: No MySQL client available"
            return 1
        fi
    fi
}

# 等待连接
echo ""
echo "[1/7] 检查 MySQL 连接..."
if ! exec_sql "SELECT 1" &>/dev/null; then
    echo -e "${RED}❌ MySQL 连接失败${NC}"
    echo ""
    echo "请检查以下配置:"
    echo "  1. MySQL 服务是否运行中"
    echo "  2. 主机地址是否正确 (当前: $MYSQL_HOST)"
    echo "  3. 端口是否正确 (当前: $MYSQL_PORT)"
    echo "  4. 用户名密码是否正确"
    echo ""
    echo "可使用环境变量覆盖配置:"
    echo "  MYSQL_HOST=your_host"
    echo "  MYSQL_PORT=3306"
    echo "  MYSQL_USER=root"
    echo "  MYSQL_PASSWORD=your_password"
    echo ""
    echo "示例: MYSQL_HOST=192.168.1.100 MYSQL_PASSWORD=secret ./scripts/test_cdc.sh"
    exit 1
fi
echo -e "${GREEN}✅ MySQL 连接正常${NC}"

# 测试 Binlog 是否启用
echo ""
echo "[2/7] 检查 Binlog 配置..."
BINLOG_STATUS=$(exec_sql "SHOW VARIABLES LIKE 'log_bin';" 2>/dev/null | grep -v "Variable_name" | awk '{print $2}')
if [ "$BINLOG_STATUS" != "ON" ]; then
    echo -e "${RED}⚠️  警告: Binlog 未启用${NC}"
    echo "请在 MySQL 配置中添加:"
    echo "  log_bin = mysql-bin"
    echo "  binlog_format = ROW"
    echo "  server_id = 1"
else
    echo -e "${GREEN}✅ Binlog 已启用${NC}"
fi

# 初始化数据库
echo ""
echo "[3/7] 初始化测试数据库..."
exec_sql "CREATE DATABASE IF NOT EXISTS $DB_NAME;" 2>/dev/null
echo -e "${GREEN}✅ 数据库 $DB_NAME 已准备${NC}"

# 初始化表结构
echo ""
echo "[4/7] 创建测试表..."
exec_sql "
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;
" 2>/dev/null
echo -e "${GREEN}✅ 测试表已创建${NC}"

# 清空测试记录（确保干净状态）
echo ""
echo "[5/7] 清空测试数据..."
exec_sql "DELETE FROM users; ALTER TABLE users AUTO_INCREMENT=1;" 2>/dev/null
echo -e "${GREEN}✅ 测试表已清空${NC}"

echo ""
echo "=========================================="
echo "开始 CDC 测试"
echo "=========================================="

# 测试1: INSERT
echo ""
echo -e "${YELLOW}[Test 1] INSERT - 插入新记录${NC}"
echo "执行: INSERT INTO users (username, email, age) VALUES ('test_user', 'test@example.com', 25)"
exec_sql "INSERT INTO users (username, email, age) VALUES ('test_user', 'test@example.com', 25);"
echo -e "${GREEN}✅ INSERT 完成${NC}"
sleep 1

# 测试2: UPDATE
echo ""
echo -e "${YELLOW}[Test 2] UPDATE - 更新记录${NC}"
echo "执行: UPDATE users SET age = 30 WHERE username = 'test_user'"
exec_sql "UPDATE users SET age = 30 WHERE username = 'test_user';"
echo -e "${GREEN}✅ UPDATE 完成${NC}"
sleep 1

# 测试3: DELETE
echo ""
echo -e "${YELLOW}[Test 3] DELETE - 删除记录${NC}"
echo "执行: DELETE FROM users WHERE username = 'test_user'"
exec_sql "DELETE FROM users WHERE username = 'test_user';"
echo -e "${GREEN}✅ DELETE 完成${NC}"

# 批量操作测试
echo ""
echo -e "${YELLOW}[Test 4] 批量操作 - 快速插入多条记录${NC}"
for i in {1..5}; do
    exec_sql "INSERT INTO users (username, email, age) VALUES ('user_$i', 'user_$i@test.com', $((20+i)));"
done
echo -e "${GREEN}✅ 批量 INSERT 完成 (5条记录)${NC}"

# 显示当前数据
echo ""
echo "[6/7] 当前 users 表数据:"
exec_sql "SELECT id, username, email, age FROM users;"

echo ""
echo "[7/7] 获取当前 Binlog 位置:"
exec_sql "SHOW MASTER STATUS\G" 2>/dev/null

echo ""
echo "=========================================="
echo -e "${GREEN}✅ CDC 测试完成！${NC}"
echo "=========================================="
echo ""
echo "请检查:"
echo "  1. data-lake 目录下的输出文件"
echo "  2. metadata/checkpoint.json (如果已创建)"
echo ""
echo "预期验证:"
echo "  - INSERT 事件 ✓"
echo "  - UPDATE 事件 (包含 Before/After) ✓"
echo "  - DELETE 事件 ✓"
echo ""
