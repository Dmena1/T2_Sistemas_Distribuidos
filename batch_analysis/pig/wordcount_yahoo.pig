-- Análisis de frecuencia de palabras: Yahoo! Answers
raw_data = LOAD '/input/yahoo_answers.txt' AS (line:chararray);

-- Tokenizar: separar cada línea en palabras
tokenized = FOREACH raw_data GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- Normalizar: convertir a minúsculas
normalized = FOREACH tokenized GENERATE LOWER(word) AS word;

-- Limpiar: remover puntuación y caracteres especiales
-- Solo mantener palabras que contengan letras
cleaned = FILTER normalized BY word MATCHES '.*[a-záéíóúñ].*';

-- Cargar stopwords desde archivo
stopwords = LOAD '/pig_scripts/stopwords.txt' AS (stopword:chararray);

-- Filtrar stopwords mediante LEFT OUTER JOIN
joined = JOIN cleaned BY word LEFT OUTER, stopwords BY stopword;
filtered = FILTER joined BY stopwords::stopword IS NULL;
words_only = FOREACH filtered GENERATE cleaned::word AS word;

-- Agrupar por palabra y contar frecuencias
grouped = GROUP words_only BY word;
word_counts = FOREACH grouped GENERATE 
    group AS word, 
    COUNT(words_only) AS count;

-- Filtrar palabras que aparecen al menos 2 veces (reducir ruido)
significant_words = FILTER word_counts BY count >= 2;

-- Ordenar por frecuencia descendente
sorted = ORDER significant_words BY count DESC;

-- Limitar a las top 100 palabras
top_words = LIMIT sorted 100;

-- Guardar resultados en HDFS
STORE top_words INTO '/output/yahoo' USING PigStorage('\t');

-- También guardar todas las palabras significativas para análisis comparativo
STORE significant_words INTO '/output/yahoo_all' USING PigStorage('\t');
