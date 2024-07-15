import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()
print(f"HOST: {os.getenv('HOST')}")
print(f"DATABASE: {os.getenv('DATABASE')}")
print(f"USER: {os.getenv('USER')}")
print(f"PASSWORD: {os.getenv('PASSWORD')}")