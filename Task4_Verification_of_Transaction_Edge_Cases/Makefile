.PHONY: install init_rs start_rs stop_rs run_monitor kill_primary clean test stop_monitor

# Virtual environment directory and bin path
VENV := ./venv
VENV_BIN := $(VENV)/bin

# MongoDB replica set configuration
REPLICA_SET_NAME := txntest
NODES := 3
START_PORT := 27100

# Install dependencies (only need to run once)
install:
        @echo "Installing Python dependencies in virtual environment..."
        test -d $(VENV) || python3 -m venv $(VENV)
        $(VENV_BIN)/pip install --upgrade pip
        $(VENV_BIN)/pip install pymongo mtools psutil python-dateutil packaging

# Initialize replica set (only need to run once)
init_rs:
        @echo "Initializing and starting MongoDB replica set..."
        $(VENV_BIN)/mlaunch init --replicaset --nodes $(NODES) --port $(START_PORT) --name $(REPLICA_SET_NAME)

# Start the initialized replica set
start_rs:
        @echo "Starting MongoDB replica set..."
        $(VENV_BIN)/mlaunch start --name $(REPLICA_SET_NAME)

# Stop the replica set
stop_rs:
        @echo "Stopping MongoDB replica set..."
        $(VENV_BIN)/mlaunch stop --name $(REPLICA_SET_NAME)

# Clean replica set data directories (manually delete corresponding folders)
clean:
        @echo "Cleaning MongoDB data directories..."
        rm -rf ./data/$(REPLICA_SET_NAME)

# Run transaction monitor script in background and output logs to monitor.log
run_monitor:
        @echo "Starting txn_monitor.py in background, logs to monitor.log..."
        nohup $(VENV_BIN)/python txn_monitor.py > monitor.log 2>&1 & echo $$! > monitor.pid
        @echo "Monitor script started in background, log file: monitor.log"

# Stop the background running transaction monitor script
stop_monitor:
        @if [ -f monitor.pid ]; then \
                pid=$$(cat monitor.pid); \
                echo "Stopping txn_monitor.py (PID $$pid)..."; \
                kill $$pid; \
                rm monitor.pid; \
        else \
                echo "monitor.pid file does not exist, unable to stop monitor process."; \
        fi

# Run script to kill the primary node
kill_primary:
        @echo "Running kill_primary.py to kill primary node..."
        $(VENV_BIN)/python kill_primary.py

# One-step test process: initialize replica set, start monitor, kill primary after 15 seconds
test:
        @echo "==> Initializing replica set..."
        $(MAKE) init_rs
        @sleep 10
        @echo "==> Starting transaction monitor script in background..."
        $(MAKE) run_monitor
        @sleep 2
        @echo "==> Kill primary node after 15 seconds..."
        sleep 15 && $(MAKE) kill_primary
        @echo "==> Test run complete. Use 'make stop_monitor' to stop the monitor script, 'make stop_rs' to stop the replica set."
