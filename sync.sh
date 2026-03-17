#!/bin/bash
cd ~/n8n-repo
git add workflows/
git commit -m "auto sync: $(date '+%Y-%m-%d %H:%M')" --allow-empty
git push origin main
