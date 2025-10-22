import os
import pika
import json
import time
import random
import sys

class WorkerTareas:
    def __init__(self, worker_id):
        self.worker_id = worker_id

        rabbit_host = os.getenv("RABBIT_HOST")
        rabbit_user = os.getenv("RABBIT_USER")
        rabbit_pass = os.getenv("RABBIT_PASS")
        tipo_tarea = os.getenv("TIPO_TAREA")

        credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
        parameters = pika.ConnectionParameters(host=rabbit_host, credentials=credentials)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='tareas_reflectivas', exchange_type='direct')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue


        self.channel.queue_bind(exchange='tareas_reflectivas', queue=self.queue_name, routing_key=tipo_tarea)
        print(f"[WORKER-{self.worker_id}] Suscrito a tareas de tipo '{tipo_tarea}'", flush=True)
            

        print(f"[WORKER-{self.worker_id}] Iniciado y listo para procesar tareas suscrito al exchange 'tareas_reflectivas'", flush=True)
    
    def procesar_tarea(self, ch, method, properties, body):
        """Callback para procesar una tarea recibida"""
        try:
            tarea = json.loads(body)
            complejidad = tarea['complejidad']
            tarea_id = tarea['id']
            tipo = tarea['tipo']

            print(f"[WORKER-{self.worker_id}] Recibió tarea {tarea_id} (complejidad: {complejidad}, tipo: {tipo})", flush=True)

            time.sleep(complejidad)
            
            print(f"[WORKER-{self.worker_id}] Tarea completada: {tarea_id}", flush=True)
            
        except Exception as e:
           print(f"[WORKER-{self.worker_id}] Error: {e}", flush=True)
    
    def iniciar_consumo(self):
        """Inicia el consumo de tareas de la cola"""
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.procesar_tarea,
            auto_ack=True  
        )
        
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print(f"[WORKER-{self.worker_id}] Detenido", flush=True)
            self.connection.close()

if __name__ == "__main__":
    worker_id = sys.argv[1] if len(sys.argv) > 1 else "1"
    worker = WorkerTareas(worker_id)
    worker.iniciar_consumo()
