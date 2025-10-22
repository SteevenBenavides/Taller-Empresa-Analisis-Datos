import os
import pika
import json
import time
import random
import sys

class WorkerTareas:
    def __init__(self, worker_id):
        self.worker_id = worker_id

        rabbit_host = os.getenv("RABBIT_HOST", "localhost")
        rabbit_user = os.getenv("RABBIT_USER", "admin")
        rabbit_pass = os.getenv("RABBIT_PASS", "admin")

        credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
        parameters = pika.ConnectionParameters(host=rabbit_host, credentials=credentials)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        
        self.channel.exchange_declare(exchange='distribuidor_tareas', exchange_type='fanout')
        
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange='distribuidor_tareas', queue=self.queue_name)
        
        print(f"[WORKER-{self.worker_id}] Iniciado y listo para procesar tareas suscrito al exchange 'distribuidor_tareas'", flush=True)
    
    def procesar_tarea(self, ch, method, properties, body):
        """Callback para procesar una tarea recibida"""
        try:
            tarea = json.loads(body)
            complejidad = tarea['complejidad']
            tarea_id = tarea['id']
            
            print(f"[WORKER-{self.worker_id}] Recibió tarea {tarea_id} (complejidad: {complejidad})", flush=True)
            
            
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
