# Levantar los servicios definidos en docker-compose.yml
docker_up:
	docker-compose up --build

# Comando principal que levanta Docker y Airflow
all: docker_up