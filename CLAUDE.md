# n8n Automation Project

## 概要
n8n全般のワークフロー一元管理リポジトリ
業務自動化・AI連携・SNS運用など全ワークフローをGitで管理

## ルール
- ワークフロー生成後は必ず workflows/ にJSONエクスポート
- git commit: feat/fix/chore: 内容
- 修正時は n8n_update_workflow → 再export → commit

## ワークフロー一覧
（追加都度記載）
| ファイル名 | 概要 | 最終更新 |
|-----------|------|---------|

## n8n情報
- URL: http://localhost:5678
- Docker: cd ~/docker/n8n && docker compose up -d
- workflows volume: ~/n8n-repo/workflows → /workflows (コンテナ内)
