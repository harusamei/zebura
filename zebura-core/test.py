import os

if __name__ == '__main__':
    # Get all environment variables
    env_vars = os.environ
    # Print each environment variable
    for key, value in env_vars.items():
        print(f"{key}: {value}")
    print(f"Environment variables:{len(env_vars)}")
