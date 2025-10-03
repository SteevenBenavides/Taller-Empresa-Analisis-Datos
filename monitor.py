#!/usr/bin/env python3
import pika
import time

def monitorear_cola():
    """Monitorea el estado de la cola"""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost')
    )
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
            print(f"\rMensajes en cola: {mensajes_en_cola}", end='', flush=True)
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nMonitoreo detenido")
    finally:
        connection.close()

if __name__ == "__main__":
    monitorear_cola()
