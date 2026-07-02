#!/usr/bin/env python3
import json
import subprocess
import threading
import paho.mqtt.client as mqtt

# Fix #1: paho-mqtt v2 future-proofing
try:
    from paho.mqtt.client import CallbackAPIVersion
    PAHO_V2 = True
except ImportError:
    PAHO_V2 = False

BROKER = "localhost"
REQUEST_TOPIC = "amdy/fbe/hermes/request"
RESPONSE_TOPIC = "amdy/fbe/hermes/response"
HOME_DIR = "/home/amdy"
HERMES_PROJECT_DIR = f"{HOME_DIR}/.hermes/hermes-agent"
HERMES_CMD = f"{HOME_DIR}/.hermes/bin/uv"
MAX_QUERY_LENGTH = 100000

def on_connect(client, userdata, flags, rc):
    print(f"[Hermes Harness] Conectado ao broker MQTT local. Código: {rc}")
    client.subscribe(REQUEST_TOPIC)
    print(f"[Hermes Harness] Aguardando queries no tópico {REQUEST_TOPIC} (0% polling)...")

def _execute_hermes(client, query, request_id):
    """
    Nó de Execução (Method) disparado pela notificação MQTT.
    Roda em thread separada para não bloquear o loop MQTT.
    Publica resultado de volta via MQTT (fire-and-forget PON-compliant).
    """
    try:
        # Fix #4: subprocess timeout (600s)
        result = subprocess.run(
            [HERMES_CMD, "run", "hermes", "-z", query],
            cwd=HERMES_PROJECT_DIR,
            capture_output=True,
            text=True,
            timeout=600
        )

        output = result.stdout.strip()
        error = result.stderr.strip()

        response_payload = {
            "query": query,
            "response": output,
            "error": error,
            "status": result.returncode
        }

        # Fix #7: echo request_id if present
        if request_id is not None:
            response_payload["request_id"] = request_id

        client.publish(RESPONSE_TOPIC, json.dumps(response_payload))
        print(f"[Hermes Harness] Resposta publicada em {RESPONSE_TOPIC}")

    except subprocess.TimeoutExpired:
        error_payload = {
            "query": query,
            "response": "",
            "error": "Timeout: execução excedeu 600 segundos",
            "status": -1
        }
        if request_id is not None:
            error_payload["request_id"] = request_id
        client.publish(RESPONSE_TOPIC, json.dumps(error_payload))
        print("[Hermes Harness] Erro: timeout do subprocess")

    except Exception as e:
        # Fix #5: publish error response on exception
        error_payload = {
            "query": query,
            "response": "",
            "error": str(e),
            "status": -1
        }
        if request_id is not None:
            error_payload["request_id"] = request_id
        client.publish(RESPONSE_TOPIC, json.dumps(error_payload))
        print(f"[Hermes Harness] Erro na execução do Hermes: {e}")

def on_message(client, userdata, msg):
    print(f"\n[Hermes Harness] Recebido request em {msg.topic}")

    try:
        # Fix #6: decode inside try block
        payload = msg.payload.decode()
        data = json.loads(payload)
        query = data.get("query", "")
        request_id = data.get("request_id")

        if not query:
            print("[Hermes Harness] Query vazia ignorada.")
            return

        # Fix #8: query length validation
        if len(query) > MAX_QUERY_LENGTH:
            error_payload = {
                "query": query[:200] + "...",
                "response": "",
                "error": f"Query excede o limite de {MAX_QUERY_LENGTH} caracteres ({len(query)} recebidos)",
                "status": -1
            }
            if request_id is not None:
                error_payload["request_id"] = request_id
            client.publish(RESPONSE_TOPIC, json.dumps(error_payload))
            print("[Hermes Harness] Query rejeitada: excede limite de tamanho.")
            return

        print(f"[Hermes Harness] Query recebida: {query}")
        print("[Hermes Harness] Invocando Hermes agent com contexto do HERMES_PROJECT_DIR...")

        # Fix #2: Run subprocess off the MQTT thread via threading.Thread.
        # PON-compliant: the thread is a fire-and-forget Method execution node
        # that publishes results back via MQTT notification when done.
        t = threading.Thread(
            target=_execute_hermes,
            args=(client, query, request_id),
            daemon=True
        )
        t.start()

    except Exception as e:
        # Fix #5: publish error response on parse/decode failure
        error_payload = {
            "query": "",
            "response": "",
            "error": str(e),
            "status": -1
        }
        client.publish(RESPONSE_TOPIC, json.dumps(error_payload))
        print(f"[Hermes Harness] Erro ao processar requisição: {e}")

if __name__ == "__main__":
    # Fix #1: paho-mqtt v2 future-proofing
    if PAHO_V2:
        client = mqtt.Client(CallbackAPIVersion.VERSION1, client_id="hermes_harness")
    else:
        client = mqtt.Client(client_id="hermes_harness")

    client.on_connect = on_connect
    client.on_message = on_message

    try:
        # Fix #9: keepalive increased to 300
        client.connect(BROKER, 1883, 300)
        # loop_forever() ensures blocking I/O and zero CPU polling (PON compliant)
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[Hermes Harness] Encerrando por interrupção do usuário...")
    except Exception as e:
        print(f"[Hermes Harness] Falha ao iniciar: {e}")
    finally:
        # Fix #10: graceful disconnect on shutdown
        client.disconnect()
        print("[Hermes Harness] Desconectado do broker MQTT.")
