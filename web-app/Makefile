db-up:
	docker compose up -d cassandra

db-setup:
	python3 scripts/setup_cassandra.py

db-reset:
	docker compose down -v
	docker compose up -d cassandra
	python3 scripts/setup_cassandra.py

db-shell:
	docker exec -it web-app-cassandra cqlsh

start:
	npm start
