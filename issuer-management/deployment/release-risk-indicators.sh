#!/usr/bin/env bash
# Web-only release: no migrations, ingestion jobs or schedule changes.
set -euo pipefail
root=/data/dealviewer-issuer-production
commit=${1:?full main commit required}
mode=${2:?prepare or activate}
[[ "$commit" =~ ^[0-9a-f]{40}$ ]]
src="$root/releases/$commit/issuer-management"
backup="$root/deploy-backups/indicators-$commit"
candidate="dealviewer-issuer-indicators-${commit:0:12}"
web="dealviewer-issuer-web:git-$commit"
run_web(){
 docker run -d --name "$1" --restart unless-stopped --user 1200:1201 --read-only --cap-drop ALL \
  --security-opt no-new-privileges --cpus 1 --memory 768m --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  -e ISSUER_REVIEW_CONFIG=/run/issuer/review.json -p "127.0.0.1:$2:8080" \
  -v "$root/private/web.json:/run/issuer/web.json:ro" -v "$root/private/review.json:/run/issuer/review.json:ro" \
  -v "$root/archive:/archive:ro" -v "$root/risk:/risk:ro" "$web"
}
health(){
 local port=$1 file=$2
 for attempt in $(seq 1 20); do
  if curl -fsS "http://127.0.0.1:$port/health" > "$file"; then break; fi
  sleep 1
 done
 python3 - "$file" "$commit" <<'PY'
import json,sys
assert json.load(open(sys.argv[1]))['commit']==sys.argv[2]
PY
}
if [ "$mode" = prepare ]; then
 test -d "$src"; test ! -e "$backup"
 mkdir -p "$backup"
 docker inspect dealviewer-issuer-web --format '{{.Config.Image}}' > "$backup/previous-image.txt"
 docker inspect dealviewer-issuer-web > "$backup/previous-container.json"
 chmod 600 "$backup/previous-container.json"
 docker cp dealviewer-issuer-web:/opt/issuer/release.json "$backup/previous-release.json"
 acquisition=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["acquisition_image"])' "$backup/previous-release.json")
 docker build --build-arg "REVISION=$commit" --build-arg "ACQUISITION_IMAGE=$acquisition" \
  -f "$src/deployment/Dockerfile.web" -t "$web" "$src"
 run_web "$candidate" 18883
 health 18883 "$backup/candidate-health.json"
elif [ "$mode" = activate ]; then
 python3 - "$backup/candidate-acceptance.json" "$commit" <<'PY'
import json,sys
result=json.load(open(sys.argv[1]))
assert result['passed'] and result['live'] and result['commit']==sys.argv[2]
assert not result['page_errors'] and result['real_writes']==0
PY
 test ! -e "$backup/deployed"
 test "$(docker inspect dealviewer-issuer-web --format '{{.Config.Image}}')" = "$(cat "$backup/previous-image.txt")"
 health 18883 "$backup/candidate-health.json"
 old="dealviewer-issuer-web-before-${commit:0:12}"
 docker stop dealviewer-issuer-web
 docker rename dealviewer-issuer-web "$old"
 rollback(){ docker rm -f dealviewer-issuer-web >/dev/null 2>&1 || true; docker rename "$old" dealviewer-issuer-web; docker start dealviewer-issuer-web; }
 trap rollback ERR
 run_web dealviewer-issuer-web 18880
 health 18880 "$backup/production-health.json"
 trap - ERR
 cp "$backup/production-health.json" "$root/web-release.json"
 touch "$backup/deployed"
 docker stop "$candidate"
else
 echo 'Expected prepare or activate' >&2; exit 2
fi
