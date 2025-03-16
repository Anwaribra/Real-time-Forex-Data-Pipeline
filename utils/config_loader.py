import os
import json
import re

def load_config(config_path='config/config.json'):
    """
    Load configuration file and substitute environment variables
    """
    with open(config_path, 'r') as f:
        config_str = f.read()
    
    # Replace environment variables
    def replace_env_var(match):
        env_var = match.group(1)
        return os.environ.get(env_var, '')
    
    # Replace ${VAR} or $VAR with environment variable values
    config_str = re.sub(r'\${([^}]+)}|\$([A-Za-z0-9_]+)', replace_env_var, config_str)
    
    return json.loads(config_str) 