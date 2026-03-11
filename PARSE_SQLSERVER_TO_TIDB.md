# SQL Server → TiDB SQL 兼容性转换记录

本文档记录 SQL Server 慢查询回放到 TiDB 时遇到的语法不兼容问题及解决方案。

---

## 1. 标识符引用语法

### 问题描述
SQL Server 使用 `[column_name]` 作为标识符引用，TiDB/MySQL 使用反引号 `` `column_name` ``。

### 示例
```sql
-- SQL Server
SELECT [t].[ChargeItemID] FROM [Finance].[dbo].[ChargeItemMediCareItemCodeMapping] t

-- TiDB 需要
SELECT `t`.`ChargeItemID` FROM `Finance`.`dbo`.`ChargeItemMediCareItemCodeMapping` t
```

### 解决方案
在 `parsesqlserver.go` 的 `convertSQLServerIdentifiers` 函数中转换：

```go
// [column_name] -> `column_name`
if sql[i] == '[' {
    end := strings.Index(sql[i+1:], "]")
    if end >= 0 {
        identifier := sql[i+1 : i+1+end]
        result.WriteByte('`')
        result.WriteString(identifier)
        result.WriteByte('`')
        i = i + 1 + end + 1
        continue
    }
}
```

### 状态
✅ 已修复

---

## 2. WITH(NOLOCK) 表提示

### 问题描述
SQL Server 的 `WITH(NOLOCK)` 表提示用于脏读，TiDB 不支持此语法。

### 示例
```sql
-- SQL Server
SELECT * FROM ChargeItemMediCareItemCodeMapping t WITH(NOLOCK)
INNER JOIN ChargeItemMedicalCareInfo t2 WITH(NOLOCK) ON t.MediCareItemCode = t2.MediCareItemCode

-- TiDB 不支持，需要移除
SELECT * FROM ChargeItemMediCareItemCodeMapping t
INNER JOIN ChargeItemMedicalCareInfo t2 ON t.MediCareItemCode = t2.MediCareItemCode
```

### 解决方案
在 `parsesqlserver.go` 的 `removeWithHints` 函数中移除：

```go
// 移除 WITH(...) 表提示
func removeWithHints(sql string) string {
    result := strings.Builder{}
    i := 0
    upper := strings.ToUpper(sql)
    
    for i < len(sql) {
        if i+4 < len(sql) && upper[i:i+4] == "WITH" {
            j := i + 4
            for j < len(sql) && (sql[j] == ' ' || sql[j] == '\t') {
                j++
            }
            if j < len(sql) && sql[j] == '(' {
                depth := 1
                k := j + 1
                for k < len(sql) && depth > 0 {
                    if sql[k] == '(' { depth++ }
                    else if sql[k] == ')' { depth-- }
                    k++
                }
                if depth == 0 {
                    i = k  // 跳过 WITH(...)
                    continue
                }
            }
        }
        result.WriteByte(sql[i])
        i++
    }
    return result.String()
}
```

### 状态
✅ 已修复

---

## 3. 数据类型转换

### 问题描述
SQL Server 和 TiDB 的数据类型名称不同。

### 类型映射表

| SQL Server | TiDB/MySQL | 说明 |
|------------|------------|------|
| `NVARCHAR(n)` | `VARCHAR(n)` | Unicode 字符串，TiDB 默认 UTF8MB4 |
| `NCHAR(n)` | `CHAR(n)` | Unicode 字符 |
| `DATETIME` | `DATETIME` | 兼容 |
| `SMALLDATETIME` | `DATETIME` | 精度降低 |
| `MONEY` | `DECIMAL(19,4)` | 货币类型 |
| `SMALLMONEY` | `DECIMAL(10,4)` | 小货币类型 |
| `UNIQUEIDENTIFIER` | `CHAR(36)` 或 `VARCHAR(64)` | GUID |
| `XML` | `TEXT` 或 `JSON` | XML 数据 |
| `IMAGE` | `BLOB` | 二进制大对象 |
| `TEXT` | `TEXT` | 兼容 |
| `NTEXT` | `TEXT` | Unicode 文本 |
| `TIMESTAMP` | `TIMESTAMP` | ⚠️ SQL Server 是行版本号，TiDB 是时间戳 |
| `BIT` | `BOOLEAN` 或 `TINYINT(1)` | 布尔值 |

### 解决方案
在表结构转换时手动映射，或创建自动化转换脚本。

### 状态
⚠️ 需要在创建表时手动处理

---

## 4. 内置函数差异

### 问题描述
SQL Server 和 TiDB 的内置函数名称或行为不同。

### 函数映射表

| SQL Server | TiDB/MySQL | 说明 |
|------------|------------|------|
| `GETDATE()` | `NOW()` | 当前日期时间 |
| `GETUTCDATE()` | `UTC_TIMESTAMP()` | UTC 时间 |
| `DATEADD(day, 1, date)` | `DATE_ADD(date, INTERVAL 1 DAY)` | 日期加减 |
| `DATEDIFF(day, d1, d2)` | `DATEDIFF(d2, d1)` | ⚠️ 参数顺序相反！ |
| `ISNULL(a, b)` | `IFNULL(a, b)` 或 `COALESCE(a, b)` | 空值处理 |
| `LEN(string)` | `LENGTH(string)` | 字符串长度 |
| `SUBSTRING(s, start, len)` | `SUBSTRING(s, start, len)` | 兼容（但 SQL Server 从 1 开始） |
| `CHARINDEX(s1, s2)` | `INSTR(s2, s1)` | ⚠️ 参数顺序相反！ |
| `REPLACE(s, old, new)` | `REPLACE(s, old, new)` | 兼容 |
| `CONVERT(VARCHAR, col)` | `CAST(col AS CHAR)` | 类型转换 |
| `NEWID()` | `UUID()` | 生成 UUID |
| `SCOPE_IDENTITY()` | `LAST_INSERT_ID()` | 最后插入 ID |
| `@@ROWCOUNT` | `ROW_COUNT()` | 影响行数 |

### 示例
```sql
-- SQL Server
SELECT DATEADD(day, 7, GETDATE()), ISNULL(name, 'N/A')

-- TiDB
SELECT DATE_ADD(NOW(), INTERVAL 7 DAY), IFNULL(name, 'N/A')
```

### 解决方案
需要在解析时进行函数名替换，或在回放前预处理 SQL。

### 状态
⏳ 待实现

---

## 5. TOP vs LIMIT

### 问题描述
SQL Server 使用 `TOP N`，TiDB/MySQL 使用 `LIMIT N`。

### 示例
```sql
-- SQL Server
SELECT TOP 10 * FROM Users ORDER BY CreateDate DESC

-- TiDB
SELECT * FROM Users ORDER BY CreateDate DESC LIMIT 10
```

### 解决方案
需要正则替换：
```go
// TOP N -> LIMIT N (需要移动到 ORDER BY 后面)
re := regexp.MustCompile(`(?i)TOP\s+(\d+)`)
```

### 状态
⏳ 待实现

---

## 6. 字符串连接

### 问题描述
SQL Server 使用 `+` 连接字符串，TiDB 使用 `CONCAT()` 或保留 `+`（但行为不同）。

### 示例
```sql
-- SQL Server
SELECT FirstName + ' ' + LastName AS FullName FROM Users

-- TiDB
SELECT CONCAT(FirstName, ' ', LastName) AS FullName FROM Users
-- 或（MySQL 模式下 + 会被当作加法）
SELECT FirstName + ' ' + LastName AS FullName FROM Users  -- 可能报错
```

### 解决方案
将 `+` 替换为 `CONCAT()`，但需要区分字符串和数字。

### 状态
⏳ 待实现

---

## 7. 存储过程和 EXEC

### 问题描述
SQL Server 的存储过程语法与 TiDB 完全不同，无法直接转换。

### 示例
```sql
-- SQL Server
EXEC sp_executesql N'SELECT * FROM Users', N'@id INT', @id=1
EXEC proc_GetUserById @UserId=123

-- TiDB
-- 需要完全重写，或者创建兼容的存储过程
```

### 解决方案
- `sp_executesql` 可以提取内部的 SQL 直接执行
- 自定义存储过程需要在 TiDB 中重新创建

### 状态
⚠️ 部分支持（提取 SQL 直接执行），存储过程需手动处理

---

## 8. 临时表

### 问题描述
SQL Server 使用 `#TempTable` 和 `##TempTable`，TiDB 使用 `TEMPORARY TABLE`。

### 示例
```sql
-- SQL Server
SELECT * INTO #TempUsers FROM Users WHERE Status = 1
SELECT * FROM #TempUsers

-- TiDB
CREATE TEMPORARY TABLE TempUsers AS SELECT * FROM Users WHERE Status = 1
SELECT * FROM TempUsers
```

### 解决方案
需要在回放时跳过临时表相关 SQL，或预先创建临时表。

### 状态
⏳ 待实现

---

## 9. 分页语法

### 问题描述
SQL Server 2012+ 使用 `OFFSET ... FETCH`，TiDB 使用 `LIMIT ... OFFSET`。

### 示例
```sql
-- SQL Server 2012+
SELECT * FROM Users ORDER BY ID 
OFFSET 10 ROWS FETCH NEXT 20 ROWS ONLY

-- TiDB
SELECT * FROM Users ORDER BY ID LIMIT 20 OFFSET 10
```

### 解决方案
正则替换语法。

### 状态
⏳ 待实现

---

## 10. 其他语法差异

| 特性 | SQL Server | TiDB | 状态 |
|------|------------|------|------|
| 字符串引号 | `'string'` 或 `N'unicode'` | `'string'` | ⚠️ N 前缀需移除 |
| 布尔值 | `1/0` 或 `TRUE/FALSE` | `TRUE/FALSE` | ✅ 兼容 |
| 注释 | `--` 和 `/* */` | `--` 和 `/* */` | ✅ 兼容 |
| 空字符串 | `''` 不等于 NULL | `''` 不等于 NULL | ✅ 兼容 |
| 大小写敏感 | 默认不敏感 | 依赖排序规则 | ⚠️ 需注意 |
| Schema | `Database.Schema.Table` | `Database.Table` | ⚠️ 需移除 Schema |

---

## 修复优先级

| 优先级 | 问题 | 影响 | 状态 |
|--------|------|------|------|
| P0 | 标识符 `[name]` | 所有 SQL | ✅ 已修复 |
| P0 | `WITH(NOLOCK)` | 大量查询 | ✅ 已修复 |
| P1 | `TOP N` | 分页查询 | ⏳ 待实现 |
| P1 | 内置函数 | 部分查询 | ⏳ 待实现 |
| P2 | 存储过程 | 复杂业务 | ⚠️ 需手动处理 |
| P2 | 临时表 | 复杂查询 | ⏳ 待实现 |
| P3 | 分页语法 | 新版 SQL Server | ⏳ 待实现 |

---

## 更新日志

### 2026-03-11
- ✅ 修复标识符引用 `[name]` → `` `name` ``
- ✅ 移除 `WITH(NOLOCK)` 表提示
- 📝 创建本文档

---

## 参考资料

- [TiDB SQL 语法](https://docs.pingcap.com/zh/tidb/stable/sql-statement-reference)
- [SQL Server to MySQL Migration Guide](https://dev.mysql.com/doc/mysql-migration-excerpt/5.7/en/sql-server-to-mysql.html)
- [PingCAP Migration Tools](https://docs.pingcap.com/zh/tidb-data-migration/stable/overview)