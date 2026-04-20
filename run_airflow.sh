cat > run_airflow.sh <<'EOF'
#!/bin/bash
cd "$(dirname "$0")"
docker compose up -d
echo "Airflow started at http://localhost:8080"
EOF
chmod +x run_airflow.sh