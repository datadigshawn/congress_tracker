#!/usr/bin/env bash
# ------------------------------------------------------------------
# upload_data_db.sh — 把本機 data.db 上傳到 GitHub Release 供雲端版下載
#
# 用法：
#   ./scripts/upload_data_db.sh                 # 使用預設 tag=data-latest
#   ./scripts/upload_data_db.sh my-tag          # 指定 tag
#
# 前置需求：
#   1. 已安裝 gh CLI 並 `gh auth login`
#   2. 目前資料夾為 congressTrack，且 data.db 已由 sync_data.py 產出
# ------------------------------------------------------------------
set -euo pipefail

TAG="${1:-data-latest}"
DB_PATH="$(cd "$(dirname "$0")/.." && pwd)/data.db"
REPO_SLUG="$(git -C "$(dirname "$DB_PATH")" remote get-url origin \
             | sed -E 's#(git@github.com:|https://github.com/)##; s#\.git$##')"

if [[ ! -f "$DB_PATH" ]]; then
  echo "❌ 找不到 $DB_PATH，請先執行 python sync_data.py --source all"
  exit 1
fi

SIZE=$(du -h "$DB_PATH" | cut -f1)
NOW=$(date "+%Y-%m-%d %H:%M:%S")
echo "📦 data.db size: $SIZE  repo: $REPO_SLUG  tag: $TAG"

# 建立 release（若已存在則略過錯誤）
if ! gh release view "$TAG" --repo "$REPO_SLUG" >/dev/null 2>&1; then
  echo "🆕 建立 release $TAG ..."
  gh release create "$TAG" \
     --repo "$REPO_SLUG" \
     --title "data.db snapshot" \
     --notes "Auto-uploaded data.db for congressTrack offline version."
fi

# 上傳（--clobber 會覆蓋同名 asset）
echo "⬆️  上傳 data.db ..."
gh release upload "$TAG" "$DB_PATH" --repo "$REPO_SLUG" --clobber

# 更新 release notes 以顯示最新同步時間
gh release edit "$TAG" --repo "$REPO_SLUG" \
   --notes "data.db snapshot — size: $SIZE — uploaded: $NOW"

echo "✅ 完成。雲端 app 下次啟動會自動抓最新版本。"
