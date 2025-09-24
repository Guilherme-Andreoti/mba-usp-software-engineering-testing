import os
import csv
import threading
import paho.mqtt.client as mqtt
from tqdm import tqdm
import json
import time
import argparse

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
FOLDER_PATH = "./data"
MQTT_TOPIC_PREFIX = "dados/csv/"

# --- Variáveis Globais ---
progress_bar = None
progress_lock = threading.Lock()
client = None  # único cliente global

# --- Funções ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("\n✅ Conectado ao broker MQTT com sucesso!")
    else:
        print(f"\n❌ Falha na conexão. Código de erro: {rc}")

def init_mqtt_client():
    global client
    client = mqtt.Client(protocol=mqtt.MQTTv311)
    client.on_connect = on_connect
    client.connect(MQTT_BROKER, MQTT_PORT)
    client.loop_start()

def csv_row_generator(folder_path):
    """Gera linhas dos CSVs um por um (streaming)."""
    files_to_process = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith(".csv")]
    for filepath in files_to_process:
        filename = os.path.basename(filepath)
        topic = f"{MQTT_TOPIC_PREFIX}{os.path.splitext(filename)[0]}"
        try:
            with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
                csv_reader = csv.DictReader(csvfile)
                for row in csv_reader:
                    yield row, topic
        except Exception as e:
            print(f"Erro ao ler o arquivo {filename}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Publica dados de arquivos CSV via MQTT a uma taxa e por um tempo especificados.")
    parser.add_argument("--rate", type=float, default=1000, help="Taxa de publicação em mensagens por segundo (msg/s).")
    parser.add_argument("--duration", type=float, default=60, help="Duração total do envio em segundos.")
    args = parser.parse_args()

    if not os.path.exists(FOLDER_PATH):
        print(f"O diretório '{FOLDER_PATH}' não foi encontrado.")
        return

    print("📊 Preparando envio dos dados dos arquivos CSV...")

    init_mqtt_client()

    start_time = time.time()
    messages_sent = 0

    # Progress bar sem total fixo (vai atualizando dinamicamente)
    global progress_bar
    progress_bar = tqdm(desc="Progresso do envio", unit="msg")

    try:
        for row, topic in csv_row_generator(FOLDER_PATH):
            if time.time() - start_time >= args.duration:
                break

            data_dict = {
                "date_time": row.get("date_time"),
                "proximity": float(row.get("proximity", 0)),
                "humidity": float(row.get("humidity", 0)),
                "pressure": float(row.get("pressure", 0)),
                "light": float(row.get("light", 0)),
                "oxidised": float(row.get("oxidised", 0)),
                "reduced": float(row.get("reduced", 0)),
                "nh3": float(row.get("nh3", 0)),
                "temperature": float(row.get("temperature", 0)),
                "sound": {
                    "amplitude": float(row.get("sound_amplitude", 0))
                },
                "topic": topic,
                "startProcessingTimestamp": int(time.time_ns() // 1_000_000)
            }

            json_payload = json.dumps(data_dict)
            client.publish(topic, json_payload, qos=1)

            messages_sent += 1
            progress_bar.update(1)

            time.sleep(1 / args.rate)

    except KeyboardInterrupt:
        print("\n🛑 Interrupção do usuário (Ctrl+C) detectada. Encerrando...")

    finally:
        if progress_bar:
            progress_bar.close()
        if client:
            client.disconnect()
            client.loop_stop()
        print(f"\n✅ Processo de envio concluído. Total enviado: {messages_sent} mensagens.")

if __name__ == "__main__":
    main()
