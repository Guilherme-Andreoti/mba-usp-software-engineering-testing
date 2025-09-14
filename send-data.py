import os
import csv
import threading
import paho.mqtt.client as mqtt
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import json
import time

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
FOLDER_PATH = "./data"
MQTT_TOPIC_PREFIX = "dados/csv/"

## Para testes
MAX_FILE_LINES = 5000

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
    client.loop_start()  # necessário para processar envios/acks

def count_total_lines(folder_path):
    total_lines = 0
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            filepath = os.path.join(folder_path, filename)
            try:
                with open(filepath, 'r', newline='', encoding='utf-8') as f:
                    file_lines = sum(1 for _ in f) - 1
                    if MAX_FILE_LINES > 0 and file_lines > MAX_FILE_LINES:
                        total_lines += MAX_FILE_LINES
                    else:
                        total_lines += file_lines
            except Exception as e:
                print(f"Erro ao contar linhas no arquivo {filename}: {e}")
    return total_lines

def process_and_send_file(filepath):
    filename = os.path.basename(filepath)
    topic = f"{MQTT_TOPIC_PREFIX}{os.path.splitext(filename)[0]}"
    
    try:
        with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.DictReader(csvfile)
            
            line_count = 0
            
            for row in csv_reader:
                
                if MAX_FILE_LINES > 0  and line_count >= MAX_FILE_LINES:
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
                    "topic": topic
                }
                
                time.sleep(0.001)

                json_payload = json.dumps(data_dict)
                
                # publica de forma thread-safe
                client.publish(topic, json_payload, qos=1)
                
                with progress_lock:
                    progress_bar.update(1)

                line_count+=1
    
    except Exception as e:
        print(f"Erro ao processar o arquivo {filename}: {e}")

def main():
    if not os.path.exists(FOLDER_PATH):
        print(f"O diretório '{FOLDER_PATH}' não foi encontrado.")
        return

    print("📊 Contando o total de linhas nos arquivos CSV...")
    total_messages = count_total_lines(FOLDER_PATH)

    if total_messages <= 0:
        print("Nenhuma mensagem para processar.")
        return

    print(f"Total de mensagens a processar: {total_messages}")

    files_to_process = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith(".csv")]
    
    global progress_bar
    progress_bar = tqdm(total=total_messages, desc="Progresso do envio", unit="msg")

    max_workers = os.cpu_count()/2 or 4
    print(f"⚡ Usando {max_workers} threads para processar os arquivos...")

    try:
        init_mqtt_client()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(process_and_send_file, files_to_process)
            
    except KeyboardInterrupt:
        print("\n🛑 Interrupção do usuário (Ctrl+C) detectada. Encerrando...")
    
    finally:
        if progress_bar:
            progress_bar.close()

        if client:
            client.disconnect()
            client.loop_stop()
        
        print("\n✅ Processo de envio concluído.")

if __name__ == "__main__":
    main()
