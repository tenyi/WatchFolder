# watchFolder 測試與事件處理重構設計

## 目標

先為現有檔案監控功能建立可重跑的測試基準，再以最小範圍修正上一輪 review 發現的三個問題：

1. 檔案重新命名後留下舊的雜湊 key。
2. 遞迴監控時錯誤排除所有子目錄中的 `hashes.txt`。
3. 郵件 worker 失敗時，記憶體中的雜湊狀態與磁碟上的 `hashes.txt` 不一致。

## 範圍

包含：

- 為雜湊解析、初始掃描、持久化、新增／修改／刪除與事件分流補上測試。
- 補上重新命名、子目錄 `hashes.txt`、郵件佇列關閉等回歸測試。
- 讓事件處理以磁碟持久化成功作為狀態提交點。
- 保持目前單一 `src/main.rs` 架構，只新增測試所需的 dev dependency。

不包含：

- 真實 SMTP 伺服器整合測試。
- 郵件自動重試、持久化郵件佇列或新的通知格式。
- 不相關的模組拆分、CLI 變更或 UI／部署變更。

## 架構

維持目前單一 `src/main.rs`。事件處理拆成「規劃」與「提交」兩階段，但不引入額外 trait 或模組抽象。

事件規劃階段只讀取檔案、計算雜湊並建立：

- 下一份完整的 `HashMap<String, String>`。
- 這次變更要產生的 `EmailJob` 清單。

規劃階段不修改傳入的目前狀態，也不寫磁碟或送郵件。必要時以目前雜湊表的 clone 作為工作副本，事件中任何讀取或雜湊錯誤都會丟棄整個工作副本。

提交階段依序執行：

1. 將下一份雜湊表寫入 `hashes.txt`。
2. 寫入成功後更新記憶體中的 `file_hashes`。
3. 逐一將 `EmailJob` 送進既有的單一郵件 worker。

因此，郵件佇列錯誤不會回滾已持久化的檔案狀態，也不會讓事件處理失敗；錯誤會被記錄。若雜湊計算或 `write_hashes` 失敗，則不更新記憶體，也不送出通知。

## 路徑與事件行為

啟動時將監控目錄 canonicalize，讓初始掃描、notify 事件與 `hashes.txt` 使用一致的絕對路徑。

`is_hashes_file` 只判斷事件路徑是否為監控根目錄的 `hashes_path`，不再以 basename 判斷。因此：

- 根目錄的 `hashes.txt` 不納入雜湊，也不觸發自我監控迴圈。
- `subdir/hashes.txt` 是一般被監控檔案，會被掃描、雜湊、更新與通知。

事件分流如下：

- `Create` 與一般 `Modify`：處理仍存在的檔案。
- `Modify(Name(_))`：執行完整 rescan，以同時支援檔案與目錄重新命名。
- `Remove`：只使用完整路徑或 canonical 路徑尋找雜湊 key，不再以相同檔名猜測路徑。
- `need_rescan`：沿用完整 rescan 流程。

重新命名沿用目前「差異通知」模型：舊路徑產生刪除通知，新路徑產生新增通知。目錄重新命名可能產生多筆檔案差異通知，但會讓雜湊狀態完整正確；本次不新增特殊的重新命名郵件格式。

## 測試策略

測試放在 `src/main.rs` 的 `#[cfg(test)] mod tests`，新增 `tempfile` 作為 dev dependency。測試不連線外部 SMTP，而是檢查事件處理產生的 `EmailJob` 內容，並以斷開 receiver 的 channel 模擬 worker 失敗。

### 第一階段：既有功能基準測試

先加入並確認以下測試在目前功能下通過：

- 新格式 tab 分隔與舊格式冒號分隔的 `hashes.txt` 解析。
- 初始遞迴掃描、SHA256 計算、`write_hashes`／`load_hashes` round trip。
- 新增檔案會更新 hash 並產生新增通知。
- 修改檔案會保留舊 hash 並產生變更通知。
- 內容未變更時不產生通知，也不需要更新狀態。
- 刪除檔案會移除 hash 並產生刪除通知。
- `Create`、一般 `Modify`、`Remove` 事件會正確分流並持久化。

### 第二階段：回歸測試

在重構前加入以下預期會先失敗的測試：

- 檔案重新命名後，舊路徑不在 hash map，新路徑存在，且 `hashes.txt` 只保留新路徑。
- 子目錄中的 `hashes.txt` 被納入初始掃描，且其修改／刪除事件不會被根目錄排除規則吞掉。
- 郵件 sender 已斷線時，事件處理仍成功完成；`hashes.txt` 與記憶體狀態都反映新檔案內容，且不因 queue error 造成下次重啟重複偵測。
- 具有相同 basename 的兩個不同目錄檔案，刪除其中一個時只移除正確的完整路徑 key。

### 驗證命令

實作完成後必須執行：

```bash
cargo test
cargo fmt -- --check
```

兩者都成功，且三個回歸測試與所有既有功能測試通過，才算完成。

## 錯誤處理

- 檔案讀取或雜湊計算失敗：事件不提交，主迴圈記錄錯誤並繼續監控。
- `write_hashes` 失敗：保留上一份記憶體狀態，不排入通知。
- 郵件 channel 關閉：保留已寫入的新狀態，記錄通知失敗，不回滾。
- SMTP 寄信失敗：既有 worker 記錄錯誤並繼續處理後續工作，不加入自動重試。
- `hashes.txt` 無法解析的行：維持既有行為，忽略該行。

## 檔案變更

- Modify: `Cargo.toml`，新增 `tempfile` dev dependency。
- Modify: `src/main.rs`，加入測試、事件規劃／提交流程、根目錄 hash 排除、重新命名 rescan 與郵件失敗後的持久化行為。
- Create: `docs/superpowers/specs/2026-07-15-watch-folder-refactor-design.md`，保存本設計。

## 完成標準

- 所有既有核心功能都有可重跑測試。
- 三個 review 問題都有先失敗、後通過的回歸測試。
- 郵件失敗不會破壞雜湊狀態一致性。
- `cargo test` 與 `cargo fmt -- --check` 通過。
- 不修改與本重構無關的使用者檔案或設定。
