#!/usr/bin/env bash
# Called after isolated SQL and browser acceptance, with an exact main commit archive.
set -euo pipefail
root=/data/dealviewer-issuer-production
commit=${1:?full Git commit required}
[[ "$commit" =~ ^[0-9a-f]{40}$ ]]
src="$root/releases/$commit/issuer-management"
test -d "$src"
acquisition="dealviewer-issuer-acquisition:git-$commit"
web="dealviewer-issuer-web:git-$commit"
docker build --build-arg "REVISION=$commit" --build-arg "ACQUISITION_IMAGE=$acquisition" -f "$src/deployment/Dockerfile.acquisition" -t "$acquisition" "$src"
docker build --build-arg "REVISION=$commit" --build-arg "ACQUISITION_IMAGE=$acquisition" -f "$src/deployment/Dockerfile.web" -t "$web" "$src"
python3 - "$src/release.json" "$commit" "$acquisition" <<'PY'
import json,sys
json.dump({'commit':sys.argv[2],'acquisition_image':sys.argv[3]},open(sys.argv[1],'w'))
PY
# Existing jobs must finish before replacing their code. A running writer is not killed.
for unit in acquisition processing risk; do
 if systemctl is-active --quiet "dealviewer-issuer-$unit.service"; then echo "Active issuer service: $unit";exit 3;fi
done
for dir in acquisition risk; do
 mkdir -p "$root/$dir"
done
mkdir -p "$root/site-state" "$root/risk/originals"
chown 1200:1201 "$root/risk" "$root/risk/originals" "$root/site-state"
backup="$root/deploy-backups/risk-$commit"
mkdir -p "$backup"
test ! -e "$backup/deployed"
for unit in acquisition processing risk;do
 for suffix in service timer;do
  path="/etc/systemd/system/dealviewer-issuer-$unit.$suffix"
  if [ -f "$path" ];then cp "$path" "$backup/";fi
 done
done
docker run --rm --network host --user 0 -e ISSUER_PILOT_ROOT="$root/acquisition" \
 -e DEALVIEWER_SECRETS_FILE=/run/dealviewer/secrets.json \
 -v "$root/acquisition:$root/acquisition" -v /data/dealviewer-crawler/private/secrets.json:/run/dealviewer/secrets.json:ro \
 "$acquisition" /opt/issuer/deployment/migrate-risk.py
run_web(){
 docker run -d --name "$1" --restart unless-stopped --user 1200:1201 --read-only --cap-drop ALL \
 --security-opt no-new-privileges --cpus 1 --memory 768m --tmpfs /tmp:rw,noexec,nosuid,size=64m \
 -e ISSUER_REVIEW_CONFIG=/run/issuer/review.json -p "127.0.0.1:$2:8080" \
 -v "$root/private/web.json:/run/issuer/web.json:ro" -v "$root/private/review.json:/run/issuer/review.json:ro" \
 -v "$root/archive:/archive:ro" -v "$root/risk:/risk:ro" "$web"
}
candidate="dealviewer-issuer-web-risk-${commit:0:12}"
run_web "$candidate" 18883
for attempt in $(seq 1 20);do
 if curl -fsS http://127.0.0.1:18883/health > "$backup/candidate-health.json";then break;fi
 sleep 1
done
python3 - "$backup/candidate-health.json" "$commit" <<'PY'
import json,sys
assert json.load(open(sys.argv[1]))['commit']==sys.argv[2]
PY
old="dealviewer-issuer-web-before-${commit:0:12}"
docker stop dealviewer-issuer-web
docker rename dealviewer-issuer-web "$old"
rollback(){ docker rm -f dealviewer-issuer-web >/dev/null 2>&1 || true;docker rename "$old" dealviewer-issuer-web;docker start dealviewer-issuer-web; }
trap rollback ERR
run_web dealviewer-issuer-web 18880
for attempt in $(seq 1 20);do
 if curl -fsS http://127.0.0.1:18880/health > "$backup/production-health.json";then break;fi
 sleep 1
done
python3 - "$backup/production-health.json" "$commit" <<'PY'
import json,sys
assert json.load(open(sys.argv[1]))['commit']==sys.argv[2]
PY
trap - ERR
if [ -L "$root/acquisition-src" ];then readlink "$root/acquisition-src" > "$backup/previous-source.txt";rm "$root/acquisition-src";
else mv "$root/acquisition-src" "$backup/acquisition-src";fi
ln -s "$src" "$root/acquisition-src"
for name in dealviewer-issuer-acquisition.service dealviewer-issuer-processing.service dealviewer-issuer-risk.service dealviewer-issuer-risk.timer;do
 install -m 644 "$src/deployment/$name" "/etc/systemd/system/$name"
done
systemctl daemon-reload
systemctl enable --now dealviewer-issuer-risk.timer
touch "$backup/deployed"
systemctl list-timers 'dealviewer-issuer-*' --all --no-pager
docker inspect dealviewer-issuer-web --format '{{.Config.Image}} {{.Image}}'
