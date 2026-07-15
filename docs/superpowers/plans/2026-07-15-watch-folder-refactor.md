# watchFolder 測試與事件處理重構 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 先建立 watchFolder 現有功能的可重跑測試，再修正重新命名、子目錄雜湊檔排除與郵件失敗造成的狀態不一致。

**Architecture:** 維持單一 `src/main.rs`，在檔案末端加入 unit tests。事件處理改為先規劃下一份雜湊表與 EmailJob 清單，成功寫入 `hashes.txt` 後才更新記憶體，最後以最佳努力方式排入既有郵件 worker。

**Tech Stack:** Rust 2021、anyhow、notify 8、notify-debouncer-full 0.7、sha2、lettre、tempfile（dev dependency）。

## Global Constraints

- 維持目前單一 `src/main.rs` 架構，不拆出新的 production module。
- 不加入真實 SMTP 伺服器整合測試；測試直接驗證 EmailJob 並以斷線 channel 模擬 worker 失敗。
- 不加入郵件自動重試、持久化郵件佇列或新的通知格式。
- 郵件佇列錯誤不得回滾已成功寫入的雜湊狀態。
- 啟動時將監控目錄 canonicalize，掃描、事件與 hash key 使用一致的絕對路徑。
- 只排除監控根目錄的 `hashes.txt`；子目錄中的同名檔案必須被監控。
- 實作完成後 `cargo test` 與 `cargo fmt -- --check` 必須成功。
- 不修改 `.codegraph/`、`.serena/`、`AGENTS.md` 或其他與本任務無關的使用者檔案。

---

## 檔案結構

- Modify: `Cargo.toml` — 新增 `tempfile` dev dependency。
- Modify: `Cargo.lock` — 由 Cargo 依 `Cargo.toml` 更新 lockfile。
- Modify: `src/main.rs` — 加入基準／回歸測試，重構事件規劃與提交流程，修正路徑排除與 rename 行為。
- Create: `docs/superpowers/specs/2026-07-15-watch-folder-refactor-design.md` — 已核准的設計規格，不再於實作階段修改。

## Task 1: 建立測試工具與既有功能基準

**Files:**

- Modify: `Cargo.toml` 的 dependencies 區段後方。
- Modify: `Cargo.lock`，由 Cargo 自動更新。
- Modify: `src/main.rs` 檔案末端，新增 `#[cfg(test)] mod tests`。

**Interfaces:**

- Consumes: 目前的 `parse_hash_line`、`scan_directory`、`load_hashes`、`write_hashes` 與 `process_debounced_event`。
- Produces: 共用測試 helper，以及以下基準測試名稱，後續回歸測試與重構不得破壞這些測試的意圖：
  - `parse_hash_line_supports_tab_and_legacy_formats`
  - `scan_directory_and_hashes_round_trip`
  - `process_file_lifecycle_updates_hashes_and_queues_notifications`

- [ ] **Step 1: 新增測試依賴**

在 `Cargo.toml` 的 dependencies 區段後加入：

~~~toml
[dev-dependencies]
tempfile = "3"
~~~

執行：

~~~bash
cargo test parse_hash_line_supports_tab_and_legacy_formats -- --exact
~~~

Expected: 測試可能因 test module 尚未存在而顯示找不到測試；此步只確認 Cargo 能解析新增的 dev dependency。

- [ ] **Step 2: 加入測試 helper 與基準測試**

在 `src/main.rs` 最後加入以下測試模組。測試使用絕對暫存路徑，使目前的 `path_key` 與後續 canonicalize 設計都能使用相同 key：

~~~rust
#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{DataChange, ModifyKind, RemoveKind, RenameMode};
    use notify::{Event, EventKind};
    use notify_debouncer_full::DebouncedEvent;
    use std::sync::mpsc::{self, Receiver};
    use std::time::Instant;
    use tempfile::tempdir;

    fn key(path: &Path) -> String {
        path.canonicalize()
            .unwrap_or_else(|_| path.to_path_buf())
            .to_string_lossy()
            .into_owned()
    }

    fn debounced_event(kind: EventKind, paths: &[&Path]) -> DebouncedEvent {
        let event = paths.iter().fold(Event::new(kind), |event, path| {
            event.add_path((*path).to_path_buf())
        });
        DebouncedEvent::new(event, Instant::now())
    }

    fn queued_jobs(rx: &Receiver<EmailJob>) -> Vec<EmailJob> {
        rx.try_iter().collect()
    }

    #[test]
    fn parse_hash_line_supports_tab_and_legacy_formats() {
        assert_eq!(
            parse_hash_line("/tmp/a:b.txt\t012345"),
            Some(("/tmp/a:b.txt".to_string(), "012345".to_string()))
        );
        assert_eq!(
            parse_hash_line("/tmp/a:b.txt:012345"),
            Some(("/tmp/a".to_string(), "b.txt:012345".to_string()))
        );
        assert_eq!(parse_hash_line("invalid"), None);
        assert_eq!(parse_hash_line("\t012345"), None);
    }

    #[test]
    fn scan_directory_and_hashes_round_trip() {
        let temp = tempdir().unwrap();
        let nested = temp.path().join("nested");
        fs::create_dir(&nested).unwrap();
        let file = nested.join("data.txt");
        fs::write(&file, "initial").unwrap();
        let hashes_path = temp.path().join(HASHES_FILENAME);

        let hashes = scan_directory(temp.path(), &hashes_path).unwrap();

        assert_eq!(
            hashes.get(&key(&file)),
            Some(&calculate_hash(&file).unwrap())
        );
        assert!(!hashes.contains_key(&key(&hashes_path)));

        write_hashes(&hashes, &hashes_path).unwrap();
        assert_eq!(load_hashes(temp.path(), &hashes_path).unwrap(), hashes);
    }

    #[test]
    fn process_file_lifecycle_updates_hashes_and_queues_notifications() {
        let temp = tempdir().unwrap();
        let file = temp.path().join("watched.txt");
        let hashes_path = temp.path().join(HASHES_FILENAME);
        let (tx, rx) = mpsc::channel();
        let mut hashes = HashMap::new();

        fs::write(&file, "one").unwrap();
        process_debounced_event(
            &debounced_event(EventKind::Create(notify::event::CreateKind::File), &[&file]),
            temp.path(),
            &hashes_path,
            &mut hashes,
            "to@example.com",
            &tx,
        )
        .unwrap();
        assert_eq!(hashes.get(&key(&file)), Some(&calculate_hash(&file).unwrap()));
        assert_eq!(queued_jobs(&rx).len(), 1);
        assert_eq!(load_hashes(temp.path(), &hashes_path).unwrap(), hashes);

        fs::write(&file, "two").unwrap();
        process_debounced_event(
            &debounced_event(
                EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                &[&file],
            ),
            temp.path(),
            &hashes_path,
            &mut hashes,
            "to@example.com",
            &tx,
        )
        .unwrap();
        assert_eq!(queued_jobs(&rx).len(), 1);

        let unchanged = hashes.clone();
        process_debounced_event(
            &debounced_event(
                EventKind::Modify(ModifyKind::Data(DataChange::Content)),
                &[&file],
            ),
            temp.path(),
            &hashes_path,
            &mut hashes,
            "to@example.com",
            &tx,
        )
        .unwrap();
        assert_eq!(hashes, unchanged);
        assert!(queued_jobs(&rx).is_empty());

        fs::remove_file(&file).unwrap();
        process_debounced_event(
            &debounced_event(EventKind::Remove(RemoveKind::File), &[&file]),
            temp.path(),
            &hashes_path,
            &mut hashes,
            "to@example.com",
            &tx,
        )
        .unwrap();
        assert!(!hashes.contains_key(&key(&file)));
        assert_eq!(queued_jobs(&rx).len(), 1);
        assert_eq!(load_hashes(temp.path(), &hashes_path).unwrap(), hashes);
    }
}
~~~

- [ ] **Step 3: 執行基準測試**

執行：

~~~bash
cargo test tests::parse_hash_line_supports_tab_and_legacy_formats -- --exact
cargo test tests::scan_directory_and_hashes_round_trip -- --exact
cargo test tests::process_file_lifecycle_updates_hashes_and_queues_notifications -- --exact
~~~

Expected: 三個測試全部 PASS。這一步只建立目前功能的可重跑基準，不修正上一輪 review 的三個問題。

- [ ] **Step 4: 確認只包含預期檔案並提交**

執行：

~~~bash
git diff --check
git status --short
git add Cargo.toml Cargo.lock src/main.rs
git commit -m "test: cover existing watch folder behavior"
~~~

Expected: commit 只包含 Cargo manifest／lockfile 與 `src/main.rs` 的測試變更；`.codegraph/`、`.serena/`、`AGENTS.md` 保持未追蹤且未被加入。

## Task 2: 加入三個問題的預期失敗回歸測試

**Files:**

- Modify: `src/main.rs` 末端既有 test module。

**Interfaces:**

- Consumes: Task 1 的 `key`、`debounced_event`、`queued_jobs` helper 與目前事件處理函式。
- Produces: 四個回歸測試；前三個直接覆蓋 review 問題，第四個防止刪除事件以 basename 猜錯檔案。

- [ ] **Step 1: 加入 rename 回歸測試**

在 test module 加入：

~~~rust
#[test]
fn rename_event_removes_old_hash_and_persists_new_path() {
    let temp = tempdir().unwrap();
    let old_path = temp.path().join("old.txt");
    let new_path = temp.path().join("new.txt");
    let hashes_path = temp.path().join(HASHES_FILENAME);
    fs::write(&old_path, "same contents").unwrap();
    let mut hashes = scan_directory(temp.path(), &hashes_path).unwrap();
    let (tx, rx) = mpsc::channel();

    fs::rename(&old_path, &new_path).unwrap();
    process_debounced_event(
        &debounced_event(
            EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
            &[&old_path, &new_path],
        ),
        temp.path(),
        &hashes_path,
        &mut hashes,
        "to@example.com",
        &tx,
    )
    .unwrap();

    assert!(!hashes.contains_key(&key(&old_path)));
    assert!(hashes.contains_key(&key(&new_path)));
    assert_eq!(load_hashes(temp.path(), &hashes_path).unwrap(), hashes);
    assert_eq!(queued_jobs(&rx).len(), 2);
}
~~~

- [ ] **Step 2: 加入 nested hashes 與 basename 回歸測試**

加入：

~~~rust
#[test]
fn nested_hashes_file_is_monitored_but_root_hashes_file_is_ignored() {
    let temp = tempdir().unwrap();
    let nested_dir = temp.path().join("nested");
    fs::create_dir(&nested_dir).unwrap();
    let nested_hashes = nested_dir.join(HASHES_FILENAME);
    let root_hashes = temp.path().join(HASHES_FILENAME);
    fs::write(&nested_hashes, "initial").unwrap();

    let mut hashes = scan_directory(temp.path(), &root_hashes).unwrap();
    assert!(hashes.contains_key(&key(&nested_hashes)));
    assert!(!hashes.contains_key(&key(&root_hashes)));

    fs::write(&nested_hashes, "changed").unwrap();
    let (tx, rx) = mpsc::channel();
    process_debounced_event(
        &debounced_event(
            EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            &[&nested_hashes],
        ),
        temp.path(),
        &root_hashes,
        &mut hashes,
        "to@example.com",
        &tx,
    )
    .unwrap();

    assert_eq!(hashes.get(&key(&nested_hashes)), Some(&calculate_hash(&nested_hashes).unwrap()));
    assert_eq!(queued_jobs(&rx).len(), 1);
}

#[test]
fn remove_does_not_guess_hash_by_basename() {
    let temp = tempdir().unwrap();
    let first = temp.path().join("first/file.txt");
    let second = temp.path().join("second/file.txt");
    let mut hashes = HashMap::new();
    hashes.insert(first.to_string_lossy().into_owned(), "first".to_string());
    hashes.insert(second.to_string_lossy().into_owned(), "second".to_string());

    assert_eq!(find_hash_key(&hashes, Path::new("file.txt")), None);
}
~~~

- [ ] **Step 3: 加入郵件 channel 關閉回歸測試**

加入：

~~~rust
#[test]
fn closed_email_queue_does_not_block_hash_persistence() {
    let temp = tempdir().unwrap();
    let file = temp.path().join("watched.txt");
    let hashes_path = temp.path().join(HASHES_FILENAME);
    fs::write(&file, "new content").unwrap();
    let mut hashes = HashMap::new();
    let (tx, rx) = mpsc::channel();
    drop(rx);

    let result = process_debounced_event(
        &debounced_event(EventKind::Create(notify::event::CreateKind::File), &[&file]),
        temp.path(),
        &hashes_path,
        &mut hashes,
        "to@example.com",
        &tx,
    );

    assert!(result.is_ok());
    assert_eq!(load_hashes(temp.path(), &hashes_path).unwrap(), hashes);
    assert_eq!(hashes.get(&key(&file)), Some(&calculate_hash(&file).unwrap()));
}
~~~

- [ ] **Step 4: 分別執行回歸測試，確認它們先失敗**

執行：

~~~bash
cargo test tests::rename_event_removes_old_hash_and_persists_new_path -- --exact
cargo test tests::nested_hashes_file_is_monitored_but_root_hashes_file_is_ignored -- --exact
cargo test tests::remove_does_not_guess_hash_by_basename -- --exact
cargo test tests::closed_email_queue_does_not_block_hash_persistence -- --exact
~~~

Expected: 每個命令都 FAIL，原因分別是舊 rename key 未移除、basename 排除過寬、刪除 fallback 會猜路徑，以及 queue error 會向上傳播。此時不修改 production code。

- [ ] **Step 5: 提交預期失敗的測試**

執行：

~~~bash
git diff --check
git add src/main.rs
git commit -m "test: reproduce event handling regressions"
~~~

Expected: commit 只包含四個回歸測試；測試在此 commit 暫時失敗是刻意的 TDD 狀態。

## Task 3: 修正 canonical 路徑、hash 排除與 rename 路由

**Files:**

- Modify: `src/main.rs:38-83` 的 `run`。
- Modify: `src/main.rs:205-263` 的 `process_debounced_event`。
- Modify: `src/main.rs:310-315` 的 `is_hashes_file`。
- Modify: `src/main.rs:468-490` 的 `find_hash_key`。

**Interfaces:**

- Consumes: Task 2 的 rename、nested hashes 與 basename 回歸測試。
- Produces: 所有路徑使用一致的 canonical root；rename 事件進入 rescan；移除危險的 basename fallback。

- [ ] **Step 1: 將監控目錄 canonicalize**

把 `run` 中建立監控目錄的程式碼改為：

~~~rust
let watch_dir = fs::canonicalize(&args[1])
    .with_context(|| format!("監控目錄不存在或不是資料夾: {}", args[1]))?;
let notify_email = args[2].clone();
~~~

移除後方原本單獨的 `if !watch_dir.is_dir()` 檢查，因為 `canonicalize` 已同時驗證路徑可讀且存在；保留錯誤訊息中的使用者輸入路徑。

- [ ] **Step 2: 只排除根目錄的 hashes.txt**

將 `is_hashes_file` 改成：

~~~rust
fn is_hashes_file(path: &Path, hashes_path: &Path) -> bool {
    path == hashes_path
}
~~~

因為 Task 3 Step 1 已讓 watcher、scan 與 `hashes_path` 都使用同一個 canonical root，不能再用 basename 比較。

- [ ] **Step 3: 移除 basename fallback**

將 `find_hash_key` 改成只檢查 raw path 與 canonical path：

~~~rust
fn find_hash_key(hashes: &HashMap<String, String>, path: &Path) -> Option<String> {
    let raw = path.to_string_lossy().into_owned();
    if hashes.contains_key(&raw) {
        return Some(raw);
    }

    path.canonicalize()
        .ok()
        .map(|canonical| canonical.to_string_lossy().into_owned())
        .filter(|canonical| hashes.contains_key(canonical))
}
~~~

不要保留依 basename 尋找的 `or_else` 分支；找不到確定的完整 key 時必須回傳 `None`，不可刪除不確定的同名檔案。

- [ ] **Step 4: 將 rename 事件導向 rescan**

在 `process_debounced_event` 的 `match &event.kind` 中，將 rename match 放在一般 `Modify` 前面：

~~~rust
use notify::{event::ModifyKind, EventKind};

match &event.kind {
    EventKind::Modify(ModifyKind::Name(_)) => {
        rescan_and_notify(
            watch_dir,
            hashes_path,
            file_hashes,
            notify_email,
            email_tx,
        )?;
    }
    EventKind::Create(_) | EventKind::Modify(_) => {
        let mut changed = false;
        for path in &event.paths {
            if path.is_file() && !is_hashes_file(path, hashes_path) {
                if handle_file_change(file_hashes, path, notify_email, email_tx)? {
                    changed = true;
                }
            }
        }
        if changed {
            write_hashes(file_hashes, hashes_path)?;
        }
    }
    EventKind::Remove(_) => {
        let mut changed = false;
        for path in &event.paths {
            if is_hashes_file(path, hashes_path) {
                continue;
            }
            if handle_file_remove(file_hashes, path, notify_email, email_tx)? {
                changed = true;
            }
        }
        if changed {
            write_hashes(file_hashes, hashes_path)?;
        }
    }
    _ => {}
}
~~~

此步只新增 `ModifyKind` import 與 rename arm；Task 4 才會將兩個 helper 改為不接收 `email_tx` 的規劃式介面。

- [ ] **Step 5: 執行已修正範圍的測試**

執行：

~~~bash
cargo test tests::rename_event_removes_old_hash_and_persists_new_path -- --exact
cargo test tests::nested_hashes_file_is_monitored_but_root_hashes_file_is_ignored -- --exact
cargo test tests::remove_does_not_guess_hash_by_basename -- --exact
~~~

Expected: 三個測試 PASS；`closed_email_queue_does_not_block_hash_persistence` 仍 FAIL，因為郵件 queue 與狀態提交尚未拆開。

- [ ] **Step 6: 提交路徑與 rename 修正**

執行：

~~~bash
git diff --check
git add src/main.rs
git commit -m "fix: reconcile renamed files and preserve nested hashes"
~~~

## Task 4: 將雜湊持久化與郵件通知解耦

**Files:**

- Modify: `src/main.rs:205-307` 的事件處理與 rescan 函式。
- Modify: `src/main.rs:418-466` 的檔案變更／刪除 helper。
- Modify: `src/main.rs:492-500` 的郵件佇列呼叫點。

**Interfaces:**

- Consumes: Task 3 的 canonical path、root-only hash 排除與 rename rescan。
- Produces:
  - `struct EventOutcome { hashes: HashMap<String, String>, emails: Vec<EmailJob> }`
  - `fn plan_event(event: &DebouncedEvent, watch_dir: &Path, hashes_path: &Path, current_hashes: &HashMap<String, String>, notify_email: &str) -> Result<EventOutcome>`
  - `fn plan_rescan(watch_dir: &Path, hashes_path: &Path, current_hashes: &HashMap<String, String>, notify_email: &str) -> Result<EventOutcome>`
  - `fn handle_file_change(hashes: &mut HashMap<String, String>, path: &Path, notify_email: &str) -> Result<Option<EmailJob>>`
  - `fn handle_file_remove(hashes: &mut HashMap<String, String>, path: &Path, notify_email: &str) -> Result<Option<EmailJob>>`

- [ ] **Step 1: 建立 EventOutcome 與提交流程**

在事件函式附近加入：

~~~rust
struct EventOutcome {
    hashes: HashMap<String, String>,
    emails: Vec<EmailJob>,
}
~~~

將 `process_debounced_event` 改成以下提交順序：

~~~rust
fn process_debounced_event(
    event: &DebouncedEvent,
    watch_dir: &Path,
    hashes_path: &Path,
    file_hashes: &mut HashMap<String, String>,
    notify_email: &str,
    email_tx: &Sender<EmailJob>,
) -> Result<()> {
    let outcome = plan_event(
        event,
        watch_dir,
        hashes_path,
        file_hashes,
        notify_email,
    )?;

    if outcome.hashes != *file_hashes {
        write_hashes(&outcome.hashes, hashes_path)?;
        *file_hashes = outcome.hashes;
    }

    for job in outcome.emails {
        if let Err(e) = queue_email(email_tx, &job.to, job.body) {
            eprintln!("通知排入郵件佇列失敗: {e:#}");
        }
    }

    Ok(())
}
~~~

這裡只能在 `write_hashes` 成功後移動 `outcome.hashes`；queue error 不得使用 `?` 向上傳播。

- [ ] **Step 2: 實作 plan_event**

加入以下完整分流規則；所有一般事件都以 `current_hashes.clone()` 作為工作副本：

~~~rust
fn plan_event(
    event: &DebouncedEvent,
    watch_dir: &Path,
    hashes_path: &Path,
    current_hashes: &HashMap<String, String>,
    notify_email: &str,
) -> Result<EventOutcome> {
    if event.need_rescan() {
        return plan_rescan(watch_dir, hashes_path, current_hashes, notify_email);
    }

    if event.paths.iter().all(|path| is_hashes_file(path, hashes_path)) {
        return Ok(EventOutcome {
            hashes: current_hashes.clone(),
            emails: Vec::new(),
        });
    }

    if matches!(
        event.kind,
        notify::EventKind::Modify(notify::event::ModifyKind::Name(_))
    ) {
        return plan_rescan(watch_dir, hashes_path, current_hashes, notify_email);
    }

    let mut hashes = current_hashes.clone();
    let mut emails = Vec::new();

    match &event.kind {
        notify::EventKind::Create(_) | notify::EventKind::Modify(_) => {
            for path in &event.paths {
                if path.is_file() && !is_hashes_file(path, hashes_path) {
                    if let Some(job) = handle_file_change(&mut hashes, path, notify_email)? {
                        emails.push(job);
                    }
                }
            }
        }
        notify::EventKind::Remove(_) => {
            for path in &event.paths {
                if !is_hashes_file(path, hashes_path) {
                    if let Some(job) = handle_file_remove(&mut hashes, path, notify_email)? {
                        emails.push(job);
                    }
                }
            }
        }
        _ => {}
    }

    Ok(EventOutcome { hashes, emails })
}
~~~

事件中任一檔案讀取／雜湊錯誤都會讓 `plan_event` 返回 Err；因為工作副本尚未提交，原本的 `file_hashes` 不會被改動。

- [ ] **Step 3: 將檔案 helper 改為只產生 EmailJob**

從 `handle_file_change` 與 `handle_file_remove` 移除 `email_tx: &Sender<EmailJob>` 參數與 `queue_email` 呼叫。兩個 helper 仍先更新傳入的工作副本，然後分別回傳：

~~~rust
Ok(Some(EmailJob {
    to: notify_email.to_string(),
    body,
}))
~~~

內容未變更或找不到刪除 key 時回傳：

~~~rust
Ok(None)
~~~

保留既有通知 body 的文字格式與 timestamp 行為，不在本次重構修改郵件內容。

- [ ] **Step 4: 將 rescan_and_notify 改為 plan_rescan**

將函式改為：

~~~rust
fn plan_rescan(
    watch_dir: &Path,
    hashes_path: &Path,
    current_hashes: &HashMap<String, String>,
    notify_email: &str,
) -> Result<EventOutcome> {
    let next_hashes = scan_directory(watch_dir, hashes_path)?;
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S");
    let mut emails = Vec::new();

    for (path, new_hash) in &next_hashes {
        match current_hashes.get(path) {
            Some(old_hash) if old_hash == new_hash => {}
            Some(old_hash) => emails.push(EmailJob {
                to: notify_email.to_string(),
                body: format!(
                    "檔案變動時間: {timestamp}\n路徑: {path}\n原始雜湊值: {old_hash}\n新的雜湊值: {new_hash}"
                ),
            }),
            None => emails.push(EmailJob {
                to: notify_email.to_string(),
                body: format!(
                    "檔案變動時間: {timestamp}\n路徑: {path}\n新的雜湊值: {new_hash}"
                ),
            }),
        }
    }

    for (path, old_hash) in current_hashes {
        if !next_hashes.contains_key(path) {
            emails.push(EmailJob {
                to: notify_email.to_string(),
                body: format!(
                    "檔案刪除時間: {timestamp}\n路徑: {path}\n刪除前雜湊值: {old_hash}"
                ),
            });
        }
    }

    Ok(EventOutcome {
        hashes: next_hashes,
        emails,
    })
}
~~~

不要在 `plan_rescan` 中呼叫 `write_hashes` 或 `queue_email`；兩者都由 `process_debounced_event` 的提交階段負責。

- [ ] **Step 5: 執行 queue failure 與完整測試**

執行：

~~~bash
cargo test tests::closed_email_queue_does_not_block_hash_persistence -- --exact
cargo test
cargo fmt -- --check
~~~

Expected: queue failure 測試 PASS，所有測試 PASS，fmt check PASS。主迴圈在 worker 已結束時仍會寫入 `hashes.txt`，只會輸出通知佇列失敗訊息。

- [ ] **Step 6: 提交交易式事件處理重構**

執行：

~~~bash
git diff --check
git add src/main.rs
git commit -m "fix: persist hashes before best-effort notifications"
~~~

## Task 5: 最終驗證與交付檢查

**Files:**

- Inspect only: `Cargo.toml`、`Cargo.lock`、`src/main.rs`、已提交的 spec 與 plan。
- Do not modify: `.codegraph/`、`.serena/`、`AGENTS.md`。

**Interfaces:**

- Consumes: Task 4 完成的測試與事件處理實作。
- Produces: 可交付的測試結果、格式結果與乾淨的任務 diff。

- [ ] **Step 1: 執行完整測試**

~~~bash
cargo test
~~~

Expected: test result 顯示所有 unit tests passed，沒有 failed、ignored 以外的未通過測試。

- [ ] **Step 2: 執行格式與 diff 驗證**

~~~bash
cargo fmt -- --check
git diff --check HEAD~3..HEAD
git status --short --branch
~~~

Expected: fmt 與 diff check 都沒有輸出錯誤；status 只保留原本未追蹤的 `.codegraph/`、`.serena/`、`AGENTS.md`，不出現未提交的 Cargo 或 Rust 變更。

- [ ] **Step 3: 以測試名稱核對需求覆蓋**

~~~bash
cargo test tests::parse_hash_line_supports_tab_and_legacy_formats -- --exact
cargo test tests::scan_directory_and_hashes_round_trip -- --exact
cargo test tests::process_file_lifecycle_updates_hashes_and_queues_notifications -- --exact
cargo test tests::rename_event_removes_old_hash_and_persists_new_path -- --exact
cargo test tests::nested_hashes_file_is_monitored_but_root_hashes_file_is_ignored -- --exact
cargo test tests::closed_email_queue_does_not_block_hash_persistence -- --exact
cargo test tests::remove_does_not_guess_hash_by_basename -- --exact
~~~

Expected: 七個明確需求測試全部 PASS。

- [ ] **Step 4: 交付摘要**

回報以下內容，不宣稱未實際執行的驗證：

- 測試與格式命令的實際結果。
- 三個 review 問題各自對應的測試名稱與修正位置。
- 沒有修改使用者原有未追蹤檔案。
