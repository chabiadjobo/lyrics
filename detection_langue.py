import pandas as pd
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

# Fixer le random seed pour rendre la détection reproductible
DetectorFactory.seed = 0

# Charger le fichier CSV
csv_file = 'archive (1)/songs_with_attributes_and_lyrics.csv/songs_with_attributes_and_lyrics.csv'
df = pd.read_csv(csv_file, sep=',')

# Chemin du fichier de log
log_file = 'progress.log'

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
def process_with_progress(df, log_file, chunk_size=1000):
    languages = []
    with open(log_file, 'w') as log:  # Ouvrir le fichier de log en mode écriture
        for i, row in df.iterrows():
            if i % chunk_size == 0:
                log.write(f"Traitement des lignes : {i} sur {len(df)}\n")
                log.flush()  # Assurer que le message est écrit immédiatement
            lang = detect_language(row['lyrics'])
            languages.append(lang)
    return languages

# Appliquer la détection de langue avec suivi de progression
df['language'] = process_with_progress(df, log_file)

# Compter le nombre de chansons en français (code langue 'fr')
french_songs_count = df[df['language'] == 'fr'].shape[0]

print(f"Nombre de chansons en français : {french_songs_count}")
