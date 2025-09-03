import importlib, os, sys, time, traceback
from pathlib import Path

print("🔎 Starter iniciado", flush=True)
print(f"Python: {sys.version}", flush=True)
print(f"PWD: {Path.cwd()}", flush=True)
try:
    print(f"FILES /app: { [str(p) for p in Path('/app').glob('*')] }", flush=True)
    if Path("/app/app").exists():
        all_files = [str(p) for p in Path("/app/app").glob('**/*')]
        print(f"FILES /app/app (primeiros 30): { all_files[:30] }", flush=True)
except Exception as e:
    print("Listagem falhou:", e, flush=True)

candidates = []
env_target = os.getenv("APP_MODULE")
if env_target: candidates.append(env_target)
candidates.extend(["app.main:app", "main:app"])

chosen = None
for target in candidates:
    try:
        mod, attr = target.split(":")
        print(f"→ Testando target '{target}' ...", flush=True)
        m = importlib.import_module(mod)
        app = getattr(m, attr)
        if app is None: raise RuntimeError(f"Objeto '{attr}' não encontrado em {mod}")
        chosen = target
        print(f"✅ Target selecionado: {chosen}", flush=True)
        break
    except Exception as e:
        print(f"⚠️ Falhou '{target}': {e}", flush=True)
        traceback.print_exc()

if not chosen:
    print("❌ Nenhum módulo ASGI válido encontrado.", flush=True)
    time.sleep(30); sys.exit(1)

try:
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    print(f"▶️ Subindo Uvicorn em 0.0.0.0:{port} com '{chosen}'", flush=True)
    uvicorn.run(chosen, host="0.0.0.0", port=port, log_level="info")
except Exception as e:
    print("❌ Erro ao iniciar Uvicorn:", e, flush=True)
    traceback.print_exc()
    time.sleep(30); sys.exit(1)
