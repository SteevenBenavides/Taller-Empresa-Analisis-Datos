#!/usr/bin/env python3
import pika
import json
import time
import random

class ProductorTareas:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        self.channel = self.connection.channel()
        
        # Declarar la cola como durable para persistencia
        self.channel.queue_declare(
            queue='tareas_distribuidas',
            durable=True
        )
    
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
        print(f" [x] Tarea enviada: ID {tarea['id']}, Complejidad: {complejidad}")
    
    def generar_tareas(self):
        """Genera 10 tareas con complejidades aleatorias"""
        print("Generando 10 tareas...")
        for i in range(10):
            complejidad = random.randint(1, 5)
            self.enviar_tarea(complejidad)
            time.sleep(0.5)  # Pequeña pausa entre tareas
        
        self.connection.close()
        print("Todas las tareas han sido enviadas")

if __name__ == "__main__":
    productor = ProductorTareas()
    productor.generar_tareas()
