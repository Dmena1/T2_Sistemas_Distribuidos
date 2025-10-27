CREATE TABLE IF NOT EXISTS answers (
  id TEXT PRIMARY KEY,
  question TEXT,
  answer TEXT,
  score REAL,
  created_at TIMESTAMP DEFAULT now()
);
