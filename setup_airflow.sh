#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Setting up Airflow environment...${NC}"

# Create virtual environment
echo -e "${GREEN}Creating virtual environment...${NC}"
python -m venv venv
source venv/bin/activate

# Install required packages
echo -e "${GREEN}Installing required packages...${NC}"
pip install apache-airflow==2.7.1 \
    apache-airflow-providers-snowflake \
    pandas \
    requests \
    python-dotenv

# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Initialize Airflow database
echo -e "${GREEN}Initializing Airflow database...${NC}"
airflow db init

# Create required directories
echo -e "${GREEN}Creating required directories...${NC}"
mkdir -p ~/airflow/dags
mkdir -p ~/airflow/logs
mkdir -p ~/airflow/plugins

# Copy DAG file
echo -e "${GREEN}Copying DAG file...${NC}"
cp dags/forex_pipeline_dag.py ~/airflow/dags/

# Create Airflow user
echo -e "${GREEN}Creating Airflow admin user...${NC}"
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Create start script
echo -e "${GREEN}Creating start script...${NC}"
cat > start_airflow.sh << 'EOF'
#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Export Airflow home
export AIRFLOW_HOME=~/airflow

# Start Airflow scheduler in background
echo "Starting Airflow scheduler..."
airflow scheduler > ~/airflow/logs/scheduler.log 2>&1 &
SCHEDULER_PID=$!

# Start Airflow webserver in background
echo "Starting Airflow webserver..."
airflow webserver -p 8080 > ~/airflow/logs/webserver.log 2>&1 &
WEBSERVER_PID=$!

echo "Airflow is running!"
echo "Access the web interface at http://localhost:8080"
echo "Username: admin"
echo "Password: admin"
echo ""
echo "To stop Airflow, run: ./stop_airflow.sh"

# Save PIDs for stop script
echo $SCHEDULER_PID > ~/airflow/scheduler.pid
echo $WEBSERVER_PID > ~/airflow/webserver.pid
EOF

# Create stop script
echo -e "${GREEN}Creating stop script...${NC}"
cat > stop_airflow.sh << 'EOF'
#!/bin/bash

# Kill processes
if [ -f ~/airflow/scheduler.pid ]; then
    kill $(cat ~/airflow/scheduler.pid)
    rm ~/airflow/scheduler.pid
fi

if [ -f ~/airflow/webserver.pid ]; then
    kill $(cat ~/airflow/webserver.pid)
    rm ~/airflow/webserver.pid
fi

echo "Airflow stopped"
EOF

# Make scripts executable
chmod +x start_airflow.sh stop_airflow.sh

echo -e "${BLUE}Setup complete!${NC}"
echo -e "${BLUE}To start Airflow, run: ./start_airflow.sh${NC}"
echo -e "${BLUE}To stop Airflow, run: ./stop_airflow.sh${NC}"
echo -e "${BLUE}Access the web interface at http://localhost:8080${NC}"
echo -e "${BLUE}Username: admin${NC}"
echo -e "${BLUE}Password: admin${NC}" 