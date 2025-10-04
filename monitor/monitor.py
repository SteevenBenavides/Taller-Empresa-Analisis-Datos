
import os
import pika
import time

def monitorear_cola():
    rabbit_host = os.getenv("RABBIT_HOST", "rabbitmq")
    rabbit_user = os.getenv("RABBIT_USER", "admin")
    rabbit_pass = os.getenv("RABBIT_PASS", "admin")

    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    parameters = pika.ConnectionParameters(host=rabbit_host, credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    
    try:
        while True:
            # Obtener información de la cola
            queue_state = channel.queue_declare(
                queue='tareas_distribuidas',
                durable=True,
                passive=True  # Solo consultar, no crear
            )
            
            mensajes_en_cola = queue_state.method.message_count
            print(f"[MONITOR] Mensajes en cola: {mensajes_en_cola}", flush=True)
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nMonitoreo detenido")
    finally:
        connection.close()

if __name__ == "__main__":
    monitorear_cola()
