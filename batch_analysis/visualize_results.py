"""Generación de visualizaciones a partir de resultados de Pig."""

import os
import sys
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Backend sin GUI para Docker
from wordcloud import WordCloud
import pandas as pd

RESULTS_DIR = "/results"
OUTPUT_DIR = "/results/visualizations"

def ensure_output_dir():
    """Crea el directorio de salida si no existe."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_word_counts(filepath):
    """Carga resultados de conteo de palabras desde archivo TSV."""
    try:
        df = pd.read_csv(filepath, sep='\t', header=None, names=['word', 'count'])
        return df
    except Exception as e:
        print(f"Error cargando {filepath}: {e}")
        return None

def create_bar_chart(df, title, filename, top_n=50):
    """Crea gráfico de barras con las palabras más frecuentes."""
    if df is None or len(df) == 0:
        print(f"No hay datos para {title}")
        return
    
    df_top = df.head(top_n)
    plt.figure(figsize=(14, 10))
    plt.barh(range(len(df_top)), df_top['count'], color='steelblue')
    plt.yticks(range(len(df_top)), df_top['word'])
    plt.xlabel('Frecuencia', fontsize=12)
    plt.ylabel('Palabra', fontsize=12)
    plt.title(title, fontsize=14, fontweight='bold')
    plt.gca().invert_yaxis()
    plt.tight_layout()
    
    output_path = os.path.join(OUTPUT_DIR, filename)
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Gráfico guardado: {output_path}")

def create_wordcloud(df, title, filename):
    """Crea nube de palabras."""
    if df is None or len(df) == 0:
        print(f"No hay datos para {title}")
        return
    
    word_freq = dict(zip(df['word'], df['count']))
    wordcloud = WordCloud(
        width=1600,
        height=800,
        background_color='white',
        colormap='viridis',
        relative_scaling=0.5,
        min_font_size=10
    ).generate_from_frequencies(word_freq)
    
    plt.figure(figsize=(16, 8))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.title(title, fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout(pad=0)
    
    output_path = os.path.join(OUTPUT_DIR, filename)
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Nube de palabras guardada: {output_path}")

def create_comparison_chart(yahoo_df, llm_df, top_n=30):
    """Crea gráfico comparativo lado a lado."""
    if yahoo_df is None or llm_df is None:
        print("No hay suficientes datos para comparación")
        return
    
    yahoo_top = yahoo_df.head(top_n)
    llm_top = llm_df.head(top_n)
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 10))
    
    ax1.barh(range(len(yahoo_top)), yahoo_top['count'], color='coral')
    ax1.set_yticks(range(len(yahoo_top)))
    ax1.set_yticklabels(yahoo_top['word'])
    ax1.set_xlabel('Frecuencia', fontsize=11)
    ax1.set_title('Yahoo! Answers - Top 30 Palabras', fontsize=13, fontweight='bold')
    ax1.invert_yaxis()
    
    ax2.barh(range(len(llm_top)), llm_top['count'], color='skyblue')
    ax2.set_yticks(range(len(llm_top)))
    ax2.set_yticklabels(llm_top['word'])
    ax2.set_xlabel('Frecuencia', fontsize=11)
    ax2.set_title('LLM - Top 30 Palabras', fontsize=13, fontweight='bold')
    ax2.invert_yaxis()
    
    plt.tight_layout()
    
    output_path = os.path.join(OUTPUT_DIR, 'comparison_side_by_side.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Gráfico comparativo guardado: {output_path}")

def create_summary_table(yahoo_df, llm_df):
    """Crea tabla resumen con estadísticas."""
    summary = {
        'Métrica': [
            'Total de palabras únicas',
            'Palabra más frecuente',
            'Frecuencia máxima',
            'Frecuencia promedio',
            'Vocabulario rico (>10 apariciones)'
        ],
        'Yahoo!': [
            len(yahoo_df) if yahoo_df is not None else 0,
            yahoo_df.iloc[0]['word'] if yahoo_df is not None and len(yahoo_df) > 0 else 'N/A',
            yahoo_df.iloc[0]['count'] if yahoo_df is not None and len(yahoo_df) > 0 else 0,
            round(yahoo_df['count'].mean(), 2) if yahoo_df is not None else 0,
            len(yahoo_df[yahoo_df['count'] > 10]) if yahoo_df is not None else 0
        ],
        'LLM': [
            len(llm_df) if llm_df is not None else 0,
            llm_df.iloc[0]['word'] if llm_df is not None and len(llm_df) > 0 else 'N/A',
            llm_df.iloc[0]['count'] if llm_df is not None and len(llm_df) > 0 else 0,
            round(llm_df['count'].mean(), 2) if llm_df is not None else 0,
            len(llm_df[llm_df['count'] > 10]) if llm_df is not None else 0
        ]
    }
    
    summary_df = pd.DataFrame(summary)
    output_path = os.path.join(OUTPUT_DIR, 'summary_statistics.csv')
    summary_df.to_csv(output_path, index=False)
    print(f"Tabla resumen guardada: {output_path}")
    
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.axis('tight')
    ax.axis('off')
    table = ax.table(cellText=summary_df.values, colLabels=summary_df.columns,
                     cellLoc='center', loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)
    
    for i in range(len(summary_df.columns)):
        table[(0, i)].set_facecolor('#4472C4')
        table[(0, i)].set_text_props(weight='bold', color='white')
    
    plt.title('Estadísticas Comparativas', fontsize=14, fontweight='bold', pad=20)
    
    output_path = os.path.join(OUTPUT_DIR, 'summary_statistics.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Tabla visual guardada: {output_path}")

def main():
    print("=" * 60)
    print("GENERACIÓN DE VISUALIZACIONES")
    print("=" * 60)
    
    ensure_output_dir()
    print("\nCargando resultados...")
    yahoo_df = load_word_counts(os.path.join(RESULTS_DIR, 'yahoo', 'part-r-00000'))
    llm_df = load_word_counts(os.path.join(RESULTS_DIR, 'llm', 'part-r-00000'))
    
    if yahoo_df is None and llm_df is None:
        print("\nNo se encontraron resultados.")
        print("Asegúrate de ejecutar los scripts de Pig primero.")
        sys.exit(1)
    
    print(f"   Yahoo!: {len(yahoo_df) if yahoo_df is not None else 0} palabras")
    print(f"   LLM: {len(llm_df) if llm_df is not None else 0} palabras")
    
    print("\nGenerando visualizaciones...")
    create_bar_chart(yahoo_df, 'Yahoo! Answers - Top 50 Palabras Más Frecuentes', 
                     'yahoo_top50_bar.png', top_n=50)
    create_bar_chart(llm_df, 'LLM - Top 50 Palabras Más Frecuentes', 
                     'llm_top50_bar.png', top_n=50)
    create_wordcloud(yahoo_df, 'Yahoo! Answers - Nube de Palabras', 
                     'yahoo_wordcloud.png')
    create_wordcloud(llm_df, 'LLM - Nube de Palabras', 
                     'llm_wordcloud.png')
    create_comparison_chart(yahoo_df, llm_df, top_n=30)
    create_summary_table(yahoo_df, llm_df)
    
    print("\n" + "=" * 60)
    print("VISUALIZACIONES COMPLETADAS")
    print("=" * 60)
    print(f"Archivos generados en: {OUTPUT_DIR}/")
    print("\nArchivos creados:")
    for file in os.listdir(OUTPUT_DIR):
        print(f"  - {file}")

if __name__ == "__main__":
    main()
