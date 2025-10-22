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

        self.channel.exchange_declare(exchange='tareas_reflectivas', exchange_type='direct')

    def enviar_tarea(self, complejidad, tipo):
        """Envía una tarea a la cola con la complejidad especificada"""
        tarea = {
            'complejidad': complejidad,
            'tipo': tipo,
            'timestamp': time.time(),
            'id': random.randint(1000, 9999)
        }
        
        self.channel.basic_publish(
            exchange='tareas_reflectivas',
            routing_key=tipo,
            body=json.dumps(tarea),
        )
        print(f"[PRODUCER] Tarea enviada: ID {tarea['id']}, Complejidad: {complejidad}, Tipo: {tipo}", flush=True)

    def generar_tareas(self):
        """Genera 10 tareas con complejidades aleatorias"""
        tipo_tareas = ['tarea_chevere', 'tarea_paila_reflectiva']
        print("[PRODUCER] Generando 10 tareas...", flush=True)
        for i in range(10):
            complejidad = random.randint(1, 5)
            tipo = random.choice(tipo_tareas)
            self.enviar_tarea(complejidad, tipo)
            time.sleep(0.5) 
        
        self.connection.close()
        print("[PRODUCER] Todas las tareas han sido enviadas", flush=True)

if __name__ == "__main__":
    productor = ProductorTareas()
    productor.generar_tareas()
