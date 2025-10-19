#web: gunicorn server:app
#worker_envios: python worker.py
#worker_conversas: python conversas.py
web: gunicorn server:app --workers ${WEB_WORKERS:-8} --threads ${WEB_THREADS:-1} --bind 0.0.0.0:${PORT:-8080} --timeout ${WEB_TIMEOUT:-60} --graceful-timeout ${WEB_GRACEFUL_TIMEOUT:-30}
conversas_web: gunicorn conversas:app --workers ${CONV_WORKERS:-8} --threads ${CONV_THREADS:-1} --bind 0.0.0.0:${CONV_PORT:-8081} --timeout ${CONV_TIMEOUT:-60} --graceful-timeout ${CONV_GRACEFUL_TIMEOUT:-30}
worker_envios: python worker.py
