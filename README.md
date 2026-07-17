# WatchFolder

監控指定目錄的檔案變動，並透過 SMTP 自動發送電子郵件通知。

## 授權

本專案以 BSD 3-Clause License 發布，詳見 [`LICENSE`](./LICENSE)。

## 功能特色

- 遞迴監控目錄內檔案的建立、修改、刪除事件
- 以 SHA256 雜湊比對檔案內容，避免僅有 mtime 變動造成的偽陽性通知
- 內建 debouncer，過濾短時間內的連續事件
- 背景 worker 依序寄送通知，不阻塞事件處理主迴圈
- 雜湊表自動持久化於 `<監控目錄>/hashes.txt`，重啟後可接續比對

## 環境需求

- Rust 1.74 以上（建議使用最新 stable）

## 安裝

```bash
cargo build --release
```

編譯產物位於 `target/release/watchFolder`。

## 設定

複製環境變數範本並編輯：

```bash
cp .env.example .env
```

填入以下 SMTP 連線資訊：

| 變數 | 必填 | 預設值 | 說明 |
|------|------|--------|------|
| `SMTP_HOST` | ✅ | — | SMTP 伺服器主機 |
| `SMTP_PORT` | ❌ | `587` | SMTP 通訊埠 |
| `SMTP_USERNAME` | ✅ | — | SMTP 帳號 |
| `SMTP_PASSWORD` | ✅ | — | SMTP 密碼 |
| `SMTP_FROM` | ✅ | — | 寄件者，可含顯示名稱，例如 `監控程式 <monitor@example.com>` |
| `DEBOUNCE_MS` | ❌ | `500` | 事件 debounce 毫秒數 |

## 使用方式

```bash
cargo run --release -- <監控目錄> <通知電子郵件地址>
```

範例：

```bash
cargo run --release -- /path/to/folder you@example.com
```

第一次啟動時會遞迴掃描目錄並建立基準 `hashes.txt`；之後僅在偵測到變動時寄信。按 `Ctrl-C` 可結束程式。

## hashes.txt 格式

位置：`<監控目錄>/hashes.txt`

每行一筆 tab 分隔的記錄：

```
<canonical 絕對路徑>	<SHA256 hex>
```

範例：

```
/Users/me/folder/report.pdf	ab12cd34ef567890...
```

> 舊版使用的冒號分隔格式仍可被讀取，但新寫入一律使用 tab 分隔。子目錄內的 `hashes.txt` 會被當作一般檔案監控，僅根目錄的那份會被排除（避免自我監控迴圈）。

## 測試

```bash
cargo test
```

## 專案結構

```
src/main.rs          # 全部程式邏輯
docs/superpowers/    # 內部重構設計規格與計畫
.env.example         # 環境變數範本
Cargo.toml           # 依賴宣告
```

## 已知限制

- 一次僅能監控單一目錄
- 程式停止期間發生的變動不會被追蹤（重新啟動時以 `hashes.txt` 為基準比對）
- 郵件主旨固定為「檔案變動通知」，無法自訂
- 寄信失敗時僅記錄錯誤，不自動重試