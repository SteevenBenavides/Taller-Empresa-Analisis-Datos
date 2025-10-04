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
        
        # Declarar la cola como durable
        self.channel.queue_declare(
            queue='tareas_distribuidas',
            durable=True
        )
        
        # Configurar prefetch_count=1 para distribución equitativa
        self.channel.basic_qos(prefetch_count=1)
        
        print(f"[WORKER-{self.worker_id}] Iniciado y listo para procesar tareas...", flush=True)
    
    def procesar_tarea(self, ch, method, properties, body):
        """Callback para procesar una tarea recibida"""
        try:
            tarea = json.loads(body)
            complejidad = tarea['complejidad']
            tarea_id = tarea['id']
            
            print(f"[WORKER-{self.worker_id}] Recibió tarea {tarea_id} (complejidad: {complejidad})", flush=True)
            
            
            # # Simular falla aleatoria (para pruebas)
            # if random.random() < 0.2:  # 20% de probabilidad de falla
            #     print(f"[WORKER-{self.worker_id}] ❌ Falló en tarea {tarea_id}", flush=True)
            #     # NO enviamos ack - la tarea se reenviará
            #     return
            
            # Simular el procesamiento según la complejidad
            time.sleep(complejidad)
            
            # Confirmar finalización exitosa
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[WORKER-{self.worker_id}] ✅ Completó tarea {tarea_id}", flush=True)
            
        except Exception as e:
           print(f"[WORKER-{self.worker_id}] Error: {e}", flush=True)
            # En caso de error, no confirmamos para que se reenvíe
    
    def iniciar_consumo(self):
        """Inicia el consumo de tareas de la cola"""
        self.channel.basic_consume(
            queue='tareas_distribuidas',
            on_message_callback=self.procesar_tarea
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
