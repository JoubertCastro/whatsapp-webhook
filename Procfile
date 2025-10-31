#web: gunicorn server:app
#worker_envios: python worker.py
#worker_conversas: python conversas.py
web: gunicorn server:app --worker-class gthread --workers ${WEB_CONCURRENCY:-64} --threads ${WEB_THREADS:-16} --timeout ${WEB_TIMEOUT:-90} --graceful-timeout ${WEB_GRACEFUL_TIMEOUT:-30} --keep-alive 5 --max-requests ${WEB_MAX_REQUESTS:-5000} --max-requests-jitter ${WEB_MAX_REQUESTS_JITTER:-500} --worker-tmp-dir /dev/shm --log-level info --access-logfile - --error-logfile -
#web: gunicorn server:app --worker-class gthread --workers ${WEB_CONCURRENCY:-2} --threads ${WEB_THREADS:-2} --timeout ${WEB_TIMEOUT:-90} --graceful-timeout ${WEB_GRACEFUL_TIMEOUT:-30} --keep-alive 30 --max-requests ${WEB_MAX_REQUESTS:-1000} --max-requests-jitter ${WEB_MAX_REQUESTS_JITTER:-100} --worker-tmp-dir /dev/shm --log-level info --access-logfile - --error-logfile -
worker_envios: python -u worker.py
