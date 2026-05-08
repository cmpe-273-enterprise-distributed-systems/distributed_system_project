CREATE KEYSPACE IF NOT EXISTS web_app
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};
