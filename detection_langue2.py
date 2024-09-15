import pandas as pd
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
import os

# Fixer le random seed pour rendre la détection reproductible
DetectorFactory.seed = 0

# Charger le fichier CSV
csv_file = 'archive (1)/songs_with_attributes_and_lyrics.csv/songs_with_attributes_and_lyrics.csv'
df = pd.read_csv(csv_file, sep=',')

# Créer un dossier pour les fichiers CSV
output_dir = 'processed_data'
os.makedirs(output_dir, exist_ok=True)

# Chemins des fichiers de log et de sortie
log_file = os.path.join(output_dir, 'progress.log')
output_file = os.path.join(output_dir, 'songs_with_languages.csv')  # Fichier pour sauvegarder les données modifiées

# Fonction pour détecter la langue
def detect_language(lyrics):
    if isinstance(lyrics, str):  # Vérifier si lyrics est une chaîne de caractères
        try:
            return detect(lyrics)
        except LangDetectException:
            return None  # Cas où la détection échoue
    else:
        return None  # Cas où lyrics n'est pas une chaîne

# Fonction pour afficher la progression dans un fichier de log
def process_with_progress(df, log_file, output_file_prefix='partial_output', chunk_size=1000):
    languages = []
    with open(log_file, 'w') as log:  # Ouvrir le fichier de log en mode écriture
        for i, row in df.iterrows():
            lang = detect_language(row['lyrics'])
            languages.append(lang)
            
            # Sauvegarder les résultats partiels toutes les `chunk_size` lignes
            if (i + 1) % chunk_size == 0:
                log.write(f"Traitement des lignes : {i + 1} sur {len(df)}\n")
                log.flush()
                
                # Créer un DataFrame partiel et sauvegarder
                partial_df = df.iloc[:i + 1].copy()
                partial_df['language'] = languages
                partial_output_file = os.path.join(output_dir, f'{output_file_prefix}_{i // chunk_size}.csv')
                partial_df.to_csv(partial_output_file, index=False)
                
        # Sauvegarder les résultats finaux
        final_df = df.copy()
        final_df['language'] = languages
        final_df.to_csv(output_file, index=False)
    
    return languages

# Appliquer la détection de langue avec suivi de progression
df['language'] = process_with_progress(df, log_file)

# Compter le nombre de chansons en français (code langue 'fr')
french_songs_count = df[df['language'] == 'fr'].shape[0]

print(f"Nombre de chansons en français : {french_songs_count}")
print(f"Les données ont été sauvegardées dans {output_file}")
