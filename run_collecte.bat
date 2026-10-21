@echo off
cd /d "%~dp0"
if not exist logs mkdir logs
"C:\Users\MADIESSE CHRISTELLE\miniconda3\python.exe" collect_france_travail.py >> logs\collecte.log 2>&1
"C:\Users\MADIESSE CHRISTELLE\miniconda3\python.exe" upload_to_minio.py >> logs\collecte.log 2>&1