import pandas as pd
# ==========================================
# 1. EXTRACT (ÇIKARMA)
# ==========================================
print("Veri okunuyor...")
# JSON dosyasını pandas DataFrame olarak okuyoruz
file_path = 'Spotify Account Data/StreamingHistory_music_0.json' 
df = pd.read_json(file_path)
print(df)