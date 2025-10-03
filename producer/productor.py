#!/usr/bin/env python3
import os
import pika
import json
import time
import random

class ProductorTareas:
    def __init__(self):
        rabbit_host = os.getenv("RABBIT_HOST", "localhost")
        rabbit_user = os.getenv("RABBIT_USER", "admin")
        rabbit_pass = os.getenv("RABBIT_PASS", "admin")

        credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
        parameters = pika.ConnectionParameters(host=rabbit_host, credentials=credentials)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        
        # Declarar la cola como durable para persistencia
        self.channel.queue_declare(queue='tareas_distribuidas', durable=True)

    def enviar_tarea(self, complejidad):
        """Envía una tarea a la cola con la complejidad especificada"""
        tarea = {
            'complejidad': complejidad,
            'timestamp': time.time(),
            'id': random.randint(1000, 9999)
        }
        
        self.channel.basic_publish(
            exchange='',
            routing_key='tareas_distribuidas',
            body=json.dumps(tarea),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Hacer el mensaje persistente
            )
        )
        print(f"[PRODUCER] Tarea enviada: ID {tarea['id']}, Complejidad: {complejidad}", flush=True)
    
    def generar_tareas(self):
        """Genera 10 tareas con complejidades aleatorias"""
        print("[PRODUCER] Generando 10 tareas...", flush=True)
        for i in range(10):
            complejidad = random.randint(1, 5)
            self.enviar_tarea(complejidad)
            time.sleep(0.5)  # Pequeña pausa entre tareas
        
        self.connection.close()
        print("[PRODUCER] Todas las tareas han sido enviadas", flush=True)

if __name__ == "__main__":
    productor = ProductorTareas()
    productor.generar_tareas()
