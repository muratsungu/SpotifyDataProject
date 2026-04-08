from etl_pipe import SpotifyETL

Server_name = 'DESKTOP-PRJ71IU\SQLEXPRESS'
Database_name = 'SpotifyData'

tasks = [
    {
        "json_path": "Spotify Account Data/StreamingHistory_music_0.json",
        "table_name": "MusicStreamingHistory"
    },
    {
        "json_path": "Spotify Account Data/StreamingHistory_podcast_0.json",
        "table_name": "PodcastStreamingHistory"
    }
]

def main():
    for task in tasks:
        json_path = task["json_path"]
        target_table = task["table_name"]
        print(f"Processing file: {json_path} into table: {target_table}")

        etl = SpotifyETL(
            json_path=task["json_path"],
            table_name=task["table_name"],
            server_name=Server_name,
            database_name=Database_name
        )
        try:
            etl.run()
        except Exception as e:
            print(f"Error processing {json_path}: {e}")

if __name__ == "__main__":
    main()