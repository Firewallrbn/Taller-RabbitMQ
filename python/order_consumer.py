import pika
import json
import time

def process_order(order):
    """Simula el procesamiento de un pedido."""
    print(f"\n[PROCESSING] Order {order['order_id']} for {order['customer']}")
    print(f"   Items: {len(order['items'])}")
    for item in order['items']:
        print(f"     - {item['product']} x{item['quantity']} (${item['price']})")
    print(f"   Total: ${order['total']}")
    
    # Simula tiempo de procesamiento
    time.sleep(1)
    print(f"   [OK] Order {order['order_id']} processed successfully!")

def callback(ch, method, properties, body):
    """Callback que se ejecuta cuando llega un mensaje."""
    try:
        order = json.loads(body)
        process_order(order)
        # Confirmación manual: solo se ack si se procesó correctamente
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"   [ERROR] Error processing order: {e}")
        # Rechaza el mensaje y lo reenvía a la cola
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='orders', durable=True)
    
    # Prefetch: procesa un mensaje a la vez (fair dispatch)
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue='orders', on_message_callback=callback)

    print(' [*] Waiting for orders. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    main()
