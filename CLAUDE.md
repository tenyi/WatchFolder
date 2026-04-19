# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
cargo build          # 編譯
cargo run -- <監控目錄> <通知電子郵件>  # 執行
cargo check          # 快速語法檢查
```

目前尚無測試。

## 架構概覽

Rust 單檔專案（`src/main.rs`），功能為監控指定目錄的檔案變動並透過 SMTP 發送電子郵件通知。

核心流程：
1. 載入或初始化 `hashes.txt`（SHA256 雜湊對照表）
2. 使用 `notify::recommended_watcher` 監聽檔案系統事件（Create/Modify/Remove）
3. 比對新舊雜湊值，若有變動則在背景執行緒發送郵件（`lettre`）
4. 每次變動後持久化雜湊表回 `hashes.txt`

SMTP 認證資訊目前為硬編碼佔位值（`send_notification_email` 函式），尚未改為環境變數或設定檔。
