# 部署離線版到 Streamlit Community Cloud

`data.db` 不進 git，而是透過 GitHub Release 當 artifact 分發。

## 一次性設定

1. **先在本機同步好 data.db**
   ```bash
   source venv/bin/activate
   python offline/sync_data.py --source all
   # 需要縣市議員再補：
   python offline/sync_data.py --source tw_councilor --cities 臺北市 新北市 ... --from 2023-01 --to 2026-04
   ```

2. **安裝 gh CLI 並登入**
   ```bash
   brew install gh
   gh auth login
   ```

3. **上傳 data.db 到 GitHub Release**
   ```bash
   ./scripts/upload_data_db.sh
   ```
   這會在 `datadigshawn/congress_tracker` 下建立 / 更新 tag `data-latest`，把 data.db 當 asset 附上。

4. **push 程式碼到 GitHub**
   ```bash
   git add -A
   git commit -m "feat: offline version + release-based data.db"
   git push
   ```

5. **到 https://share.streamlit.io 部署**
   - New app → 選 `datadigshawn/congress_tracker`
   - Main file path: `offline/app.py`
   - Python version: 3.11+
   - Advanced → Secrets 貼上：
     ```toml
     [data_release]
     repo  = "datadigshawn/congress_tracker"
     tag   = "data-latest"
     asset = "data.db"
     ```
     若 repo 是 private 再加一行 `token = "ghp_xxx"`。

6. **第一次啟動**：app 會偵測到 `/tmp/data.db` 不存在 → 自動從 Release 下載 →
   顯示成功訊息 → 正常運作。

## 日常更新流程

本機跑完 `offline/sync_data.py` 後：
```bash
./scripts/upload_data_db.sh
```
Streamlit Cloud 的 app 下次打開、或按側邊欄的「⬇️ 從 GitHub Release 重新下載 data.db」
就會抓到最新版本。Streamlit Cloud 的 `/tmp` 會隨 container 重啟清空，
所以第一位訪客會觸發下載，之後 session 共用。

## 常見問題

- **data.db 超過 2GB**：GitHub Release 單檔上限 2GB，真的塞不下再考慮 R2 / S3。
- **下載很慢**：Release asset 走 CDN，一般 29MB 在幾秒內。
- **Streamlit Cloud 免費版記憶體**：~1GB，目前用量遠低於此。
- **資料庫隨時間變大**：在 `offline/sync_data.py` 上加 `--prune 10` 保持 10 年窗口。
