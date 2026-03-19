# WeChat OCR Auto

Automacao para monitorar imagens de comprovantes do WeChat Desktop, extrair dados com OCR e gravar no Excel.

## O que este projeto faz
- Monitora `WeChat Files/.../FileStorage` em tempo real.
- Processa novas imagens/comprovantes.
- Extrai campos para Excel:
  - `CLIENTE`
  - `DATA`
  - `HORA`
  - `BANCO`
  - `VALOR`
- Usa fallback para miniatura quando imagem completa nao chega.

## Arquivos principais
- `wechat_receipt_daemon.py` -> motor principal.
- `INICIAR_WECHAT_OCR.ps1` -> inicia o monitor.
- `PARAR_WECHAT_OCR.ps1` -> para o monitor.
- `STATUS_WECHAT_OCR.ps1` -> status detalhado.
- `STATUS_WECHAT_OCR_AO_VIVO.cmd` -> status em loop.
- `refresh_group_map.py` -> atualiza mapeamento hash->nome de grupo.
- `clientes_grupos.template.json` -> template de mapeamento.

## OCR usado
- Padrao: `rapidocr-onnxruntime` (gratuito, Apache-2.0).
- Opcional: `pytesseract` (tambem gratuito).

## Instalacao (novo PC)
1. Instale Python 3.12+.
2. Abra PowerShell na pasta do projeto.
3. Crie venv e instale dependencias:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

4. Prepare mapeamento inicial:

```powershell
Copy-Item .\clientes_grupos.template.json .\clientes_grupos.json
python -X utf8 .\refresh_group_map.py
```

## Executar
```powershell
powershell -ExecutionPolicy Bypass -File .\INICIAR_WECHAT_OCR.ps1
```

Status:
```powershell
powershell -ExecutionPolicy Bypass -File .\STATUS_WECHAT_OCR.ps1
```

Ao vivo:
```bat
.\STATUS_WECHAT_OCR_AO_VIVO.cmd
```

Parar:
```powershell
powershell -ExecutionPolicy Bypass -File .\PARAR_WECHAT_OCR.ps1
```

## Subir no GitHub
Se ainda nao tiver repo git nesta pasta:

```powershell
git init
git add .
git commit -m "wechat ocr auto initial"
```

Depois, conecte com seu repositorio no GitHub:

```powershell
git branch -M main
git remote add origin https://github.com/SEU_USUARIO/SEU_REPO.git
git push -u origin main
```

## Importante
- Este repositrio ignora automaticamente:
  - banco local (`wechat_receipt_state.db`)
  - logs
  - excel gerado
  - `clientes_grupos.json` real (dados privados)
  - pasta `decrypted_msg`
- O template `clientes_grupos.template.json` pode ser versionado sem dados sensiveis.
