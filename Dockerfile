# Usando uma imagem base oficial do Python
FROM python:3.12-slim

# Definindo o diretório de trabalho dentro do container
WORKDIR /app

# Definindo o fuso horário
ENV TZ=America/Sao_Paulo

# Copiando o arquivo de dependências para o diretório de trabalho
COPY requirements.txt .

# Instalando as dependências do projeto
RUN pip install --no-cache-dir -r requirements.txt

# Copiando todo o conteúdo do projeto para o diretório de trabalho
COPY . .

# Expondo a porta que o seu aplicativo usa
EXPOSE 8000

# Comando para rodar o aplicativo
CMD ["python", "main.py"]
