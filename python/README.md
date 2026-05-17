# Sistema de Procesamiento de Pedidos con RabbitMQ

## Descripción
Ejemplo de mensajería asincrónica que simula un sistema de e-commerce donde los pedidos se envían a una cola y se procesan de forma independiente.

## Conceptos clave
- **JSON en mensajes**: Formato estándar para datos complejos
- **`auto_ack=False`**: Confirmación manual para evitar pérdida de mensajes
- **`durable=True`**: La cola persiste si RabbitMQ se reinicia
- **`delivery_mode=2`**: Mensajes persistentes en disco
- **`basic_qos(prefetch_count=1)`**: Fair dispatch para balanceo de carga

## Requisitos
- RabbitMQ corriendo en `localhost:5672`
- Python 3 con `pika` instalado

## Ejecución

### 1. Iniciar el consumidor (en una terminal)
```bash
python order_consumer.py
```
El consumidor se queda esperando pedidos.

### 2. Enviar pedidos (en otra terminal)
```bash
python order_producer.py
```
El productor envía 5 pedidos aleatorios y termina.

### 3. Observar el resultado
El consumidor procesará cada pedido mostrando los detalles y confirmando el éxito.

## Pruebas adicionales
- Ejecuta múltiples consumidores para ver el balanceo de carga
- Detén el consumidor mientras envías pedidos y reinícialo para ver la persistencia
- Modifica `num_orders` en el productor para enviar más/menos pedidos
