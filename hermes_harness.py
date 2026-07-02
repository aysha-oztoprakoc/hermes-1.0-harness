#!/usr/bin/env python3
import json
import subprocess
import paho.mqtt.client as mqtt

BROKER = "localhost"
REQUEST_TOPIC = "amdy/fbe/hermes/request"
RESPONSE_TOPIC = "amdy/fbe/hermes/response"
HOME_DIR = "/home/amdy"
HERMES_CMD = f"{HOME_DIR}/.hermes/bin/uv"

def on_connect(client, userdata, flags, rc):
    print(f"[Hermes Harness] Conectado ao broker MQTT local. Código: {rc}")
    client.subscribe(REQUEST_TOPIC)
    print(f"[Hermes Harness] Aguardando queries no tópico {REQUEST_TOPIC} (0% polling)...")

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"\n[Hermes Harness] Recebido request em {msg.topic}")
    
    try:
        data = json.loads(payload)
        query = data.get("query", "")
        if not query:
            print("[Hermes Harness] Query vazia ignorada.")
            return
            
        print(f"[Hermes Harness] Query recebida: {query}")
        print("[Hermes Harness] Invocando Hermes agent com contexto do HOME_DIR...")
        
        # Invoca Hermes no modo oneshot (-z) usando o contexto cwd=/home/amdy
        # Isso garante que ele use todo o conhecimento e pastas ocultas do usuário.
        # Em conformidade com o PON, o subprocesso é bloqueante apenas durante a execução da query solicitada,
        # atuando como um Método acionado pela notificação (I/O base event trigger).
        result = subprocess.run(
            [HERMES_CMD, "run", "hermes", "-z", query],
            cwd=HOME_DIR,
            capture_output=True,
            text=True
        )
        
        output = result.stdout.strip()
        error = result.stderr.strip()
        
        response_payload = {
            "query": query,
            "response": output,
            "error": error,
            "status": result.returncode
        }
        
        client.publish(RESPONSE_TOPIC, json.dumps(response_payload))
        print(f"[Hermes Harness] Resposta publicada em {RESPONSE_TOPIC}")
        
    except Exception as e:
        print(f"[Hermes Harness] Erro ao processar requisição: {e}")

if __name__ == "__main__":
    client = mqtt.Client(client_id="hermes_harness")
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect(BROKER, 1883, 60)
        # loop_forever() ensures blocking I/O and zero CPU polling (PON compliant)
        client.loop_forever()
    except Exception as e:
        print(f"[Hermes Harness] Falha ao iniciar: {e}")
