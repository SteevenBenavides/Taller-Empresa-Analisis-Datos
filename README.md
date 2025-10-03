# Taller-Empresa-Analisis-Datos

## Instrucciones de Prueba:

1. Instalar dependencias: `pip install -r requirements.txt`
2. Asegurar que RabbitMQ esté corriendo
3. Abrir 4 terminales y ejecutar:
   - Terminal 1: `python worker.py 1`
   - Terminal 2: `python worker.py 2` 
   - Terminal 3: `python monitor.py`
   - Terminal 4: `python productor.py`
4. Capturar evidencia de:
   - Distribución inicial equilibrada
   - Tolerancia a fallos (detener worker con Ctrl+C)
   - Procesamiento completo sin pérdidas
