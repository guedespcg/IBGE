Passos rápidos (Windows/macOS):

1) Descompacte este projeto em:
   - Windows: C:\Docker\ibge_sidra_automation
   - macOS:   ~/Docker/ibge_sidra_automation

2) Coloque seus arquivos dentro de ./data
   - municipio_por_filial.xlsx  (obrigatório)
   - planilha_completa_todas_culturas.xlsx (opcional)

3) Docker:
   > docker compose up -d --build
   > docker compose logs -f app

4) Inicializar:
   > curl -X POST http://localhost:8000/init
   > curl -X POST http://localhost:8000/bootstrap

5) Gerar Excel:
   > Invoke-WebRequest -Uri 'http://localhost:8000/relatorio/x.xlsx' -OutFile './data/relatorio_filiais.xlsx'   (Windows PowerShell)
   > curl -L http://localhost:8000/relatorio/x.xlsx -o ./data/relatorio_filiais.xlsx                           (macOS/Linux)

Observações:
- Coletas: vegetais (PAM 1612), rebanhos (PPM 3939) e tentativa de aquicultura (PPM).
- Apenas municípios da sua planilha com match IBGE (RS/SC/PR) são coletados.
- Excel final: uma aba por filial; linhas = municípios; colunas = produtos.
