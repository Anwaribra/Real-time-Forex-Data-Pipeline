import os
import sys
import pytest
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.insert(0, project_root)

from data_ingestion.fetch_data import fetch_forex_data
from data_ingestion.fetch_historical_data import fetch_historical_data
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_config_exists():
    """Test if config files exist"""
    assert Path("config/config.json").exists()
    assert Path(".env").exists()
    assert os.getenv("FOREX_API_KEY") is not None

def test_fetch_forex_data():
    """Test real-time forex data fetching"""
    try:
        data = fetch_forex_data()
        assert data is not None
        # Check if we got data for at least one currency pair
        assert len(data) > 0
    except Exception as e:
        pytest.fail(f"fetch_forex_data failed: {str(e)}")

def test_fetch_historical_data():
    """Test historical forex data fetching"""
    try:
        data = fetch_historical_data("EUR", "USD")
        assert data is not None
        assert "Time Series FX (Daily)" in data
        # Check if we have some historical data points
        time_series = data["Time Series FX (Daily)"]
        assert len(time_series) > 0
    except Exception as e:
        pytest.fail(f"fetch_historical_data failed: {str(e)}")

def test_directories_exist():
    """Test if required directories exist"""
    assert Path("data").exists()
    assert Path("processed_data").exists()
    assert Path("logs").exists() 