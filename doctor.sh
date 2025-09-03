#!/usr/bin/env bash
set -euo pipefail

echo "== 1) Validando docker-compose.yml =="
docker compose config >/dev/null && echo "OK docker-compose.yml"

echo "== 2) Procurando caracteres inválidos (NUL/BOM/CRLF) nos fontes =="
bad=0
# NUL
if LC_ALL=C grep -nP '\x00' app/*.py start.py Dockerfile 2>/dev/null; then
  echo "ERRO: há bytes NUL (\x00) nos arquivos acima"; bad=1
fi
# BOM UTF-8
if LC_ALL=C grep -nP '^\xEF\xBB\xBF' app/*.py start.py Dockerfile 2>/dev/null; then
  echo "ERRO: há BOM UTF-8 em arquivos acima"; bad=1
fi
# CRLF
if LC_ALL=C grep -nP '\r$' app/*.py start.py Dockerfile 2>/dev/null; then
  echo "ERRO: há quebras CRLF; converta para LF"; bad=1
fi
if [ "$bad" -ne 0 ]; then
  echo "Dica: regrave arquivos com here-doc (como já te passei) ou rode:"
  echo "  perl -0777 -pe 's/\\x00//g; s/\\x{FEFF}//g' -i app/*.py start.py Dockerfile"
  echo "  sed -i '' \$'s/\\r\$//' app/*.py start.py Dockerfile"
  exit 1
fi
echo "OK fontes sem caracteres inválidos"

echo "== 3) Compilando .py localmente (sanity check) =="
python3 -m py_compile app/*.py

echo "== 4) Subindo containers (rebuild só do app) =="
docker compose up -d db
docker compose up -d --no-deps --build app
docker compose ps

echo "== 5) Aguardando health do app =="
for i in $(seq 1 30); do
  code=$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8000/healthz || true)
  if [ "$code" = "200" ]; then
    echo "Health OK (tentativa $i)"; break
  fi
  sleep 1
done
if [ "${code:-}" != "200" ]; then
  echo "Health NÃO ficou OK. Últimos logs:"
  docker compose logs --no-color app | tail -n 200
  exit 2
fi

echo "== 6) Smoke tests =="
echo "- /healthz:"
curl -s http://localhost:8000/healthz | jq . || curl -s http://localhost:8000/healthz
echo
echo "- /sidra/variaveis/1612:"
curl -s http://localhost:8000/sidra/variaveis/1612 | head -n 30
echo
echo "- /sidra/produtos/1612:"
curl -s http://localhost:8000/sidra/produtos/1612 | head -n 30
echo
echo "== FIM: se tudo acima deu OK, siga com seed/coleta/relatório =="
