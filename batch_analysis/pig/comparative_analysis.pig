-- Análisis comparativo: Yahoo! vs LLM
yahoo_words = LOAD '/output/yahoo_all/part-*' USING PigStorage('\t') 
    AS (word:chararray, yahoo_count:long);

llm_words = LOAD '/output/llm_all/part-*' USING PigStorage('\t') 
    AS (word:chararray, llm_count:long);

-- Combinar ambos conjuntos
combined = JOIN yahoo_words BY word FULL OUTER, llm_words BY word;

-- Procesar resultados del join
comparison = FOREACH combined GENERATE
    (yahoo_words::word IS NOT NULL ? yahoo_words::word : llm_words::word) AS word,
    (yahoo_words::yahoo_count IS NOT NULL ? yahoo_words::yahoo_count : 0L) AS yahoo_count,
    (llm_words::llm_count IS NOT NULL ? llm_words::llm_count : 0L) AS llm_count,
    (yahoo_words::yahoo_count IS NOT NULL AND llm_words::llm_count IS NOT NULL ? 'both' :
     (yahoo_words::yahoo_count IS NOT NULL ? 'yahoo_only' : 'llm_only')) AS source;

-- Calcular diferencia de frecuencia (para palabras comunes)
comparison_with_diff = FOREACH comparison GENERATE
    word,
    yahoo_count,
    llm_count,
    source,
    (long)(llm_count - yahoo_count) AS diff;

-- Calcular valor absoluto de la diferencia para ordenamiento
comparison_with_abs = FOREACH comparison_with_diff GENERATE
    word,
    yahoo_count,
    llm_count,
    source,
    diff,
    (diff >= 0 ? diff : -diff) AS abs_diff;

-- Ordenar por diferencia absoluta (más diferentes primero)
sorted_comparison = ORDER comparison_with_abs BY abs_diff DESC;

-- Guardar resultados
STORE sorted_comparison INTO '/output/comparative' USING PigStorage('\t');

-- Extraer palabras únicas de Yahoo!
yahoo_unique = FILTER comparison BY source == 'yahoo_only';
yahoo_unique_sorted = ORDER yahoo_unique BY yahoo_count DESC;
yahoo_unique_top = LIMIT yahoo_unique_sorted 50;
STORE yahoo_unique_top INTO '/output/yahoo_unique' USING PigStorage('\t');

-- Extraer palabras únicas del LLM
llm_unique = FILTER comparison BY source == 'llm_only';
llm_unique_sorted = ORDER llm_unique BY llm_count DESC;
llm_unique_top = LIMIT llm_unique_sorted 50;
STORE llm_unique_top INTO '/output/llm_unique' USING PigStorage('\t');

-- Extraer palabras comunes con mayor diferencia
common_words = FILTER comparison_with_abs BY source == 'both';
common_sorted = ORDER common_words BY abs_diff DESC;
common_top = LIMIT common_sorted 50;
STORE common_top INTO '/output/common_differences' USING PigStorage('\t');
